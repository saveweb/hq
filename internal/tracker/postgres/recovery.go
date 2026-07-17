package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func (s *Store) FinishShardRecovery(
	ctx context.Context,
	userID, agentID, projectID, shardID string,
	generation int64,
	success bool,
	errorCode string,
	now int64,
) (tracker.Shard, error) {
	status := tracker.ShardStatusRecoveryFailed
	if success {
		status = tracker.ShardStatusActive
	}
	var result tracker.Shard
	err := s.pool.QueryRow(ctx, `
		UPDATE tracker_shards sh SET
			status=$5,
			owner_lease_expires_at=CASE WHEN $6 THEN owner_lease_expires_at ELSE 0 END,
			recovery_error_code=CASE WHEN $6 THEN NULL ELSE $9 END,
			updated_at=$7
		FROM tracker_agents a, tracker_users u
		WHERE sh.project_id=$1 AND sh.id=$2 AND sh.owner_agent_id=$3
			AND sh.generation=$4 AND sh.status='recovering'
			AND sh.checkpoint_uri IS NOT NULL AND sh.checkpoint_format='sqlite-zstd-v1'
			AND sh.checkpoint_generation IS NOT NULL AND sh.checkpoint_generation <= sh.generation
			AND sh.owner_lease_expires_at>$7
			AND a.id=sh.owner_agent_id AND a.user_id=$8 AND a.status='online'
			AND u.id=a.user_id AND u.status='active' AND 'shard_owner'=ANY(u.roles)
		RETURNING sh.project_id, sh.id, sh.status, sh.owner_agent_id, sh.generation,
			sh.owner_lease_expires_at, sh.source_uri, sh.source_format, sh.source_etag
	`, projectID, shardID, agentID, generation, status, success, now, userID, errorCode).Scan(
		&result.ProjectID, &result.ID, &result.Status, &result.OwnerAgentID, &result.Generation,
		&result.OwnerLeaseExpiresAt, &result.SourceURI, &result.SourceFormat, &result.SourceETag,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return tracker.Shard{}, &tracker.Error{
			Code: protocol.ErrorStaleGeneration, Message: "checkpoint recovery no longer belongs to this owner generation",
		}
	}
	if err != nil {
		return tracker.Shard{}, fmt.Errorf("tracker postgres: finish shard recovery: %w", err)
	}
	return result, nil
}
