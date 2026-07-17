package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const sessionColumns = `id, project_id, agent_id, user_id, attrs, created_at, lease_expires_at, last_heartbeat_at`

func (s *Store) CreateSession(
	ctx context.Context,
	userID, agentID, projectID string,
	attrs map[string]any,
	now, leaseExpiresAt int64,
) (tracker.Session, error) {
	encodedAttrs, err := encodeAttrs(attrs)
	if err != nil {
		return tracker.Session{}, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: err.Error()}
	}
	sessionID, err := newID("vs_")
	if err != nil {
		return tracker.Session{}, fmt.Errorf("tracker postgres: generate session ID: %w", err)
	}
	var result tracker.Session
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var agentUserID, kind, status string
		scanError := tx.QueryRow(ctx, `SELECT user_id, kind, status FROM tracker_agents WHERE id=$1`, agentID).
			Scan(&agentUserID, &kind, &status)
		if errors.Is(scanError, pgx.ErrNoRows) || agentUserID != userID || kind != protocol.AgentKindWorker {
			return &tracker.Error{Code: protocol.ErrorPermissionDenied, Message: "active worker agent required"}
		}
		if scanError != nil {
			return scanError
		}
		if status != "online" {
			return &tracker.Error{Code: protocol.ErrorAgentDisabled, Message: "worker agent is not online"}
		}
		var projectStatus string
		scanError = tx.QueryRow(ctx, `SELECT status FROM tracker_projects WHERE id=$1`, projectID).Scan(&projectStatus)
		if errors.Is(scanError, pgx.ErrNoRows) {
			return &tracker.Error{Code: protocol.ErrorNotFound, Message: "project not found"}
		}
		if scanError != nil {
			return scanError
		}
		if projectStatus != tracker.ProjectStatusActive {
			return &tracker.Error{Code: protocol.ErrorShardNotActive, Message: "project is not active"}
		}
		result, scanError = scanSession(tx.QueryRow(ctx, `
			INSERT INTO tracker_worker_sessions(
				id, project_id, agent_id, user_id, attrs, created_at, lease_expires_at, last_heartbeat_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$6)
			RETURNING `+sessionColumns,
			sessionID, projectID, agentID, userID, encodedAttrs, now, leaseExpiresAt))
		return scanError
	})
	if err != nil {
		return tracker.Session{}, storeError("create session", err)
	}
	return result, nil
}

func (s *Store) HeartbeatSession(
	ctx context.Context,
	userID, agentID, sessionID string,
	now, leaseExpiresAt int64,
) (tracker.Session, error) {
	result, err := scanSession(s.pool.QueryRow(ctx, `
		UPDATE tracker_worker_sessions ws
		SET lease_expires_at=$5, last_heartbeat_at=$4
		FROM tracker_agents a
		WHERE ws.id=$1 AND ws.user_id=$2 AND ws.agent_id=$3
			AND ws.lease_expires_at > $4 AND a.id=ws.agent_id AND a.status='online'
		RETURNING `+prefixedSessionColumns("ws."),
		sessionID, userID, agentID, now, leaseExpiresAt))
	if err == nil {
		return result, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return tracker.Session{}, fmt.Errorf("tracker postgres: heartbeat session: %w", err)
	}
	var currentLease int64
	err = s.pool.QueryRow(ctx, `
		SELECT lease_expires_at FROM tracker_worker_sessions WHERE id=$1 AND user_id=$2 AND agent_id=$3
	`, sessionID, userID, agentID).Scan(&currentLease)
	if errors.Is(err, pgx.ErrNoRows) {
		return tracker.Session{}, &tracker.Error{Code: protocol.ErrorNotFound, Message: "session not found"}
	}
	if err != nil {
		return tracker.Session{}, fmt.Errorf("tracker postgres: inspect session: %w", err)
	}
	if now >= currentLease {
		return tracker.Session{}, &tracker.Error{Code: protocol.ErrorSessionExpired, Message: "session lease expired"}
	}
	return tracker.Session{}, &tracker.Error{Code: protocol.ErrorAgentDisabled, Message: "worker agent is not online"}
}

func (s *Store) FindAssignment(
	ctx context.Context,
	userID, agentID, sessionID string,
	now int64,
) (*tracker.AssignmentCandidate, error) {
	session, err := scanSession(s.pool.QueryRow(ctx, `
		SELECT `+sessionColumns+` FROM tracker_worker_sessions
		WHERE id=$1 AND user_id=$2 AND agent_id=$3
	`, sessionID, userID, agentID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, &tracker.Error{Code: protocol.ErrorNotFound, Message: "session not found"}
	}
	if err != nil {
		return nil, fmt.Errorf("tracker postgres: find session: %w", err)
	}
	if now >= session.LeaseExpiresAt {
		return nil, &tracker.Error{Code: protocol.ErrorSessionExpired, Message: "session lease expired"}
	}
	var projectStatus string
	err = s.pool.QueryRow(ctx, `SELECT status FROM tracker_projects WHERE id=$1`, session.ProjectID).Scan(&projectStatus)
	if errors.Is(err, pgx.ErrNoRows) || projectStatus != tracker.ProjectStatusActive {
		return nil, &tracker.Error{Code: protocol.ErrorShardNotActive, Message: "project is not active"}
	}
	if err != nil {
		return nil, fmt.Errorf("tracker postgres: find project: %w", err)
	}

	var candidate tracker.AssignmentCandidate
	candidate.Session = session
	var agentAttrs []byte
	err = s.pool.QueryRow(ctx, `
		SELECT
			sh.project_id, sh.id, sh.status, sh.owner_agent_id, sh.generation,
			sh.owner_lease_expires_at, sh.source_uri, sh.source_format, sh.source_etag,
			a.id, a.user_id, a.kind, a.name, a.version, a.status, a.endpoint,
			a.endpoint_version, a.tls_spki_sha256, a.endpoint_status, a.attrs, a.last_heartbeat_at
		FROM tracker_shards sh
		JOIN tracker_agents a ON a.id=sh.owner_agent_id
		JOIN tracker_users u ON u.id=a.user_id
		WHERE sh.project_id=$1 AND sh.status=$2 AND sh.owner_lease_expires_at>$3
			AND a.kind=$4 AND a.status='online' AND a.endpoint IS NOT NULL AND a.endpoint_version IS NOT NULL
			AND a.endpoint_status=ANY($5) AND u.status=$6 AND $7=ANY(u.roles)
		ORDER BY sh.id
		LIMIT 1
	`, session.ProjectID, tracker.ShardStatusActive, now, protocol.AgentKindShard,
		[]string{tracker.EndpointHealthy, tracker.EndpointInsecure}, tracker.UserStatusActive, tracker.RoleShardOwner).
		Scan(
			&candidate.Shard.ProjectID, &candidate.Shard.ID, &candidate.Shard.Status,
			&candidate.Shard.OwnerAgentID, &candidate.Shard.Generation, &candidate.Shard.OwnerLeaseExpiresAt,
			&candidate.Shard.SourceURI, &candidate.Shard.SourceFormat, &candidate.Shard.SourceETag,
			&candidate.Agent.ID, &candidate.Agent.UserID, &candidate.Agent.Kind, &candidate.Agent.Name,
			&candidate.Agent.Version, &candidate.Agent.Status, &candidate.Agent.Endpoint,
			&candidate.Agent.EndpointVersion, &candidate.Agent.TLSSPKISHA256, &candidate.Agent.EndpointStatus,
			&agentAttrs, &candidate.Agent.LastHeartbeatAt,
		)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("tracker postgres: find assignment: %w", err)
	}
	candidate.Agent.Attrs, err = decodeAttrs(agentAttrs)
	if err != nil {
		return nil, fmt.Errorf("tracker postgres: decode agent attrs: %w", err)
	}
	return &candidate, nil
}

func scanSession(row rowScanner) (tracker.Session, error) {
	var result tracker.Session
	var attrs []byte
	err := row.Scan(&result.ID, &result.ProjectID, &result.AgentID, &result.UserID, &attrs,
		&result.CreatedAt, &result.LeaseExpiresAt, &result.LastHeartbeatAt)
	if err != nil {
		return tracker.Session{}, err
	}
	result.Attrs, err = decodeAttrs(attrs)
	return result, err
}

func storeError(operation string, err error) error {
	var domainError *tracker.Error
	if errors.As(err, &domainError) {
		return domainError
	}
	return fmt.Errorf("tracker postgres: %s: %w", operation, err)
}

func prefixedSessionColumns(prefix string) string {
	result := ""
	for index, column := range []string{"id", "project_id", "agent_id", "user_id", "attrs", "created_at", "lease_expires_at", "last_heartbeat_at"} {
		if index > 0 {
			result += ", "
		}
		result += prefix + column
	}
	return result
}
