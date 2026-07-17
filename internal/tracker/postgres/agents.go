package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const agentColumns = `
	id, user_id, kind, name, version, status, endpoint, endpoint_version,
	tls_spki_sha256, endpoint_status, attrs, last_heartbeat_at`

type rowScanner interface {
	Scan(dest ...any) error
}

func (s *Store) GetAgent(ctx context.Context, userID, agentID string) (tracker.Agent, error) {
	agent, err := scanAgent(s.pool.QueryRow(ctx, `
		SELECT `+agentColumns+` FROM tracker_agents WHERE id=$1 AND user_id=$2
	`, agentID, userID))
	if errors.Is(err, pgx.ErrNoRows) {
		return tracker.Agent{}, &tracker.Error{Code: protocol.ErrorNotFound, Message: "agent not found"}
	}
	if err != nil {
		return tracker.Agent{}, fmt.Errorf("tracker postgres: get agent: %w", err)
	}
	return agent, nil
}

func (s *Store) UpsertAgent(ctx context.Context, input tracker.AgentUpsert) (tracker.Agent, error) {
	attrs, err := encodeAttrs(input.Attrs)
	if err != nil {
		return tracker.Agent{}, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: err.Error()}
	}
	var result tracker.Agent
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		existing, scanError := scanAgent(tx.QueryRow(ctx, `SELECT `+agentColumns+` FROM tracker_agents WHERE id=$1 FOR UPDATE`, input.ID))
		if scanError != nil && !errors.Is(scanError, pgx.ErrNoRows) {
			return scanError
		}
		if scanError == nil {
			if existing.UserID != input.UserID {
				return &tracker.Error{Code: protocol.ErrorPermissionDenied, Message: "agent belongs to another user"}
			}
			if existing.Kind != input.Kind {
				return &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "agent kind is immutable"}
			}
			if existing.Status == "revoked" {
				return &tracker.Error{Code: protocol.ErrorAgentDisabled, Message: "agent is revoked"}
			}
			if input.Kind == protocol.AgentKindShard && existing.EndpointVersion != nil && input.EndpointVersion != nil {
				if *input.EndpointVersion < *existing.EndpointVersion {
					return &tracker.Error{
						Code: protocol.ErrorStaleEndpointVersion, Message: "endpoint version is stale",
						Details: map[string]any{"current_endpoint_version": *existing.EndpointVersion},
					}
				}
				if *input.EndpointVersion == *existing.EndpointVersion &&
					(!sameString(existing.Endpoint, input.Endpoint) || !sameString(existing.TLSSPKISHA256, input.TLSSPKISHA256)) {
					return &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "endpoint identity changed without increasing endpoint version"}
				}
			}
		}

		if errors.Is(scanError, pgx.ErrNoRows) {
			result, scanError = scanAgent(tx.QueryRow(ctx, `
				INSERT INTO tracker_agents(
					id, user_id, kind, name, version, status, endpoint, endpoint_version,
					tls_spki_sha256, endpoint_status, attrs, last_heartbeat_at, created_at, updated_at
				) VALUES ($1,$2,$3,$4,$5,'online',$6,$7,$8,$9,$10,$11,$11,$11)
				RETURNING `+agentColumns,
				input.ID, input.UserID, input.Kind, input.Name, input.Version, input.Endpoint,
				input.EndpointVersion, input.TLSSPKISHA256, input.EndpointStatus, attrs, input.Now))
		} else {
			result, scanError = scanAgent(tx.QueryRow(ctx, `
				UPDATE tracker_agents SET
					name=$2, version=$3, status='online', endpoint=$4, endpoint_version=$5,
					tls_spki_sha256=$6, endpoint_status=$7, attrs=$8,
					last_heartbeat_at=$9, updated_at=$9
				WHERE id=$1
				RETURNING `+agentColumns,
				input.ID, input.Name, input.Version, input.Endpoint, input.EndpointVersion,
				input.TLSSPKISHA256, input.EndpointStatus, attrs, input.Now))
		}
		return scanError
	})
	if err != nil {
		var domainError *tracker.Error
		if errors.As(err, &domainError) {
			return tracker.Agent{}, domainError
		}
		return tracker.Agent{}, fmt.Errorf("tracker postgres: upsert agent: %w", err)
	}
	return result, nil
}

func (s *Store) HeartbeatAgent(
	ctx context.Context,
	userID, agentID, version string,
	attrs map[string]any,
	endpointStatus string,
	allowShard, allowWorker bool,
	now, ownerLeaseExpiresAt int64,
) (tracker.AgentHeartbeat, error) {
	encodedAttrs, err := encodeAttrs(attrs)
	if err != nil {
		return tracker.AgentHeartbeat{}, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: err.Error()}
	}
	var result tracker.AgentHeartbeat
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var scanError error
		result.Agent, scanError = scanAgent(tx.QueryRow(ctx, `
			UPDATE tracker_agents SET version=$3, attrs=$4, endpoint_status=$5,
				status='online', last_heartbeat_at=$6, updated_at=$6
			WHERE id=$1 AND user_id=$2 AND status <> 'revoked'
				AND ((kind='shard' AND $7) OR (kind='worker' AND $8))
			RETURNING `+agentColumns,
			agentID, userID, version, encodedAttrs, endpointStatus, now, allowShard, allowWorker))
		if errors.Is(scanError, pgx.ErrNoRows) {
			var kind, status string
			inspectError := tx.QueryRow(ctx, `SELECT kind, status FROM tracker_agents WHERE id=$1 AND user_id=$2`, agentID, userID).
				Scan(&kind, &status)
			if errors.Is(inspectError, pgx.ErrNoRows) {
				return &tracker.Error{Code: protocol.ErrorNotFound, Message: "agent not found"}
			}
			if inspectError != nil {
				return inspectError
			}
			if status == "revoked" {
				return &tracker.Error{Code: protocol.ErrorAgentDisabled, Message: "agent is revoked"}
			}
			return &tracker.Error{Code: protocol.ErrorPermissionDenied, Message: "agent role is no longer granted"}
		}
		if scanError != nil {
			return scanError
		}
		result.OwnerAssignments = []protocol.OwnerAssignment{}
		if result.Agent.Kind != protocol.AgentKindShard || !endpointUsable(result.Agent.EndpointStatus) {
			return nil
		}
		rows, queryError := tx.Query(ctx, `
			UPDATE tracker_shards
			SET owner_lease_expires_at=$2, updated_at=$3
			WHERE owner_agent_id=$1 AND status = ANY($4)
			RETURNING project_id, id, generation, status, owner_lease_expires_at,
				source_uri, source_format, source_etag
		`, agentID, ownerLeaseExpiresAt, now, []string{
			tracker.ShardStatusLoading, tracker.ShardStatusActive, tracker.ShardStatusDraining,
			tracker.ShardStatusRecovering, tracker.ShardStatusOffline,
		})
		if queryError != nil {
			return queryError
		}
		defer rows.Close()
		for rows.Next() {
			var value protocol.OwnerAssignment
			if err := rows.Scan(&value.ProjectID, &value.ShardID, &value.Generation, &value.Status,
				&value.OwnerLeaseExpiresAt, &value.SourceURI, &value.SourceFormat, &value.SourceETag); err != nil {
				return err
			}
			result.OwnerAssignments = append(result.OwnerAssignments, value)
		}
		return rows.Err()
	})
	if err != nil {
		var domainError *tracker.Error
		if errors.As(err, &domainError) {
			return tracker.AgentHeartbeat{}, domainError
		}
		return tracker.AgentHeartbeat{}, fmt.Errorf("tracker postgres: heartbeat agent: %w", err)
	}
	return result, nil
}

func scanAgent(row rowScanner) (tracker.Agent, error) {
	var result tracker.Agent
	var attrs []byte
	err := row.Scan(
		&result.ID, &result.UserID, &result.Kind, &result.Name, &result.Version, &result.Status,
		&result.Endpoint, &result.EndpointVersion, &result.TLSSPKISHA256, &result.EndpointStatus,
		&attrs, &result.LastHeartbeatAt,
	)
	if err != nil {
		return tracker.Agent{}, err
	}
	result.Attrs, err = decodeAttrs(attrs)
	return result, err
}

func endpointUsable(status string) bool {
	return status == tracker.EndpointHealthy || status == tracker.EndpointInsecure
}

func sameString(left, right *string) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}
