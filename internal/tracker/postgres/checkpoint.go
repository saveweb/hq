package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

var checkpointOwnerStatuses = []string{tracker.ShardStatusActive, tracker.ShardStatusDraining}

func (s *Store) GetCheckpointTarget(
	ctx context.Context,
	userID, agentID, projectID, shardID string,
	generation, now int64,
) (tracker.Shard, error) {
	var result tracker.Shard
	err := s.pool.QueryRow(ctx, `
		SELECT sh.project_id, sh.id, sh.status, sh.owner_agent_id, sh.generation,
			sh.owner_lease_expires_at, sh.source_uri, sh.source_format, sh.source_etag
		FROM tracker_shards sh
		JOIN tracker_agents a ON a.id=sh.owner_agent_id
		JOIN tracker_users u ON u.id=a.user_id
		WHERE sh.project_id=$1 AND sh.id=$2 AND sh.owner_agent_id=$3
			AND sh.generation=$4 AND sh.status=ANY($5) AND sh.owner_lease_expires_at>$6
			AND a.user_id=$7 AND a.status='online'
			AND u.status='active' AND 'shard_owner'=ANY(u.roles)
	`, projectID, shardID, agentID, generation, checkpointOwnerStatuses, now, userID).Scan(
		&result.ProjectID, &result.ID, &result.Status, &result.OwnerAgentID, &result.Generation,
		&result.OwnerLeaseExpiresAt, &result.SourceURI, &result.SourceFormat, &result.SourceETag,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return tracker.Shard{}, staleCheckpointOwner()
	}
	if err != nil {
		return tracker.Shard{}, fmt.Errorf("tracker postgres: get checkpoint target: %w", err)
	}
	return result, nil
}

func (s *Store) GetCurrentCheckpointUpload(
	ctx context.Context,
	userID, agentID, projectID, shardID string,
	generation, now int64,
) (*tracker.CheckpointUpload, error) {
	var result tracker.CheckpointUpload
	var uploadID, s3UploadID, uri, checksum *string
	var sequence, size, createdAt *int64
	err := s.pool.QueryRow(ctx, `
		SELECT sh.checkpoint_upload_id, sh.checkpoint_s3_upload_id, sh.checkpoint_upload_uri,
			sh.checkpoint_upload_seq, sh.checkpoint_upload_size,
			sh.checkpoint_upload_checksum, sh.checkpoint_upload_started_at
		FROM tracker_shards sh
		JOIN tracker_agents a ON a.id=sh.owner_agent_id
		JOIN tracker_users u ON u.id=a.user_id
		WHERE sh.project_id=$1 AND sh.id=$2 AND sh.owner_agent_id=$3
			AND sh.generation=$4 AND sh.status=ANY($5) AND sh.owner_lease_expires_at>$6
			AND a.user_id=$7 AND a.status='online'
			AND u.status='active' AND 'shard_owner'=ANY(u.roles)
	`, projectID, shardID, agentID, generation, checkpointOwnerStatuses, now, userID).Scan(
		&uploadID, &s3UploadID, &uri, &sequence, &size, &checksum, &createdAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, staleCheckpointOwner()
	}
	if err != nil {
		return nil, fmt.Errorf("tracker postgres: get current checkpoint upload: %w", err)
	}
	if uploadID == nil {
		return nil, nil
	}
	if s3UploadID == nil || uri == nil || sequence == nil || size == nil || checksum == nil || createdAt == nil {
		return nil, fmt.Errorf("tracker postgres: incomplete checkpoint upload state")
	}
	result = tracker.CheckpointUpload{
		ProjectID: projectID, ShardID: shardID, Generation: generation,
		ID: *uploadID, S3UploadID: *s3UploadID, URI: *uri, Sequence: *sequence,
		SizeBytes: *size, SHA256: *checksum, CreatedAt: *createdAt,
	}
	return &result, nil
}

func (s *Store) ReserveCheckpoint(
	ctx context.Context,
	userID, agentID string,
	upload tracker.CheckpointUpload,
	now int64,
) (tracker.CheckpointUpload, error) {
	err := s.pool.QueryRow(ctx, `
		UPDATE tracker_shards sh SET
			checkpoint_upload_id=$5, checkpoint_s3_upload_id=$6,
			checkpoint_upload_uri=$7, checkpoint_upload_seq=checkpoint_seq+1,
			checkpoint_upload_generation=$4, checkpoint_upload_checksum=$8,
			checkpoint_upload_size=$9, checkpoint_upload_started_at=$10, updated_at=$10
		FROM tracker_agents a, tracker_users u
		WHERE sh.project_id=$1 AND sh.id=$2 AND sh.owner_agent_id=$3
			AND sh.generation=$4 AND sh.status=ANY($11) AND sh.owner_lease_expires_at>$10
			AND sh.checkpoint_upload_id IS NULL
			AND a.id=sh.owner_agent_id AND a.user_id=$12 AND a.status='online'
			AND u.id=a.user_id AND u.status='active' AND 'shard_owner'=ANY(u.roles)
		RETURNING sh.checkpoint_upload_seq
	`, upload.ProjectID, upload.ShardID, agentID, upload.Generation, upload.ID, upload.S3UploadID,
		upload.URI, upload.SHA256, upload.SizeBytes, now, checkpointOwnerStatuses, userID).Scan(&upload.Sequence)
	if errors.Is(err, pgx.ErrNoRows) {
		if _, targetError := s.GetCheckpointTarget(ctx, userID, agentID, upload.ProjectID, upload.ShardID, upload.Generation, now); targetError != nil {
			return tracker.CheckpointUpload{}, targetError
		}
		return tracker.CheckpointUpload{}, &tracker.Error{
			Code: protocol.ErrorCheckpointInProgress, Message: "another checkpoint upload is already active",
		}
	}
	if err != nil {
		return tracker.CheckpointUpload{}, fmt.Errorf("tracker postgres: reserve checkpoint: %w", err)
	}
	upload.CreatedAt = now
	return upload, nil
}

func (s *Store) GetCheckpointUpload(
	ctx context.Context,
	userID, agentID, projectID, shardID, uploadID string,
	generation, now int64,
) (tracker.CheckpointUpload, error) {
	current, err := s.GetCurrentCheckpointUpload(ctx, userID, agentID, projectID, shardID, generation, now)
	if err != nil {
		return tracker.CheckpointUpload{}, err
	}
	if current == nil || current.ID != uploadID {
		return tracker.CheckpointUpload{}, staleCheckpointOwner()
	}
	return *current, nil
}

func (s *Store) PublishCheckpoint(
	ctx context.Context,
	userID, agentID, projectID, shardID, uploadID string,
	generation, now int64,
) (tracker.Checkpoint, error) {
	var result tracker.Checkpoint
	err := s.pool.QueryRow(ctx, `
		UPDATE tracker_shards sh SET
			checkpoint_uri=checkpoint_upload_uri, checkpoint_format='sqlite-zstd-v1',
			checkpoint_seq=checkpoint_upload_seq, checkpoint_generation=checkpoint_upload_generation,
			checkpoint_checksum=checkpoint_upload_checksum, checkpoint_size=checkpoint_upload_size,
			checkpoint_at=$7,
			checkpoint_upload_id=NULL, checkpoint_s3_upload_id=NULL,
			checkpoint_upload_uri=NULL, checkpoint_upload_seq=NULL,
			checkpoint_upload_generation=NULL, checkpoint_upload_checksum=NULL,
			checkpoint_upload_size=NULL, checkpoint_upload_started_at=NULL, updated_at=$7
		FROM tracker_agents a, tracker_users u
		WHERE sh.project_id=$1 AND sh.id=$2 AND sh.owner_agent_id=$3
			AND sh.generation=$4 AND sh.checkpoint_upload_id=$5
			AND sh.status=ANY($6) AND sh.owner_lease_expires_at>$7
			AND a.id=sh.owner_agent_id AND a.user_id=$8 AND a.status='online'
			AND u.id=a.user_id AND u.status='active' AND 'shard_owner'=ANY(u.roles)
		RETURNING sh.project_id, sh.id, sh.checkpoint_generation, sh.checkpoint_seq,
			sh.checkpoint_uri, sh.checkpoint_format, sh.checkpoint_size,
			sh.checkpoint_checksum, sh.checkpoint_at
	`, projectID, shardID, agentID, generation, uploadID, checkpointOwnerStatuses, now, userID).Scan(
		&result.ProjectID, &result.ShardID, &result.Generation, &result.Sequence,
		&result.URI, &result.Format, &result.SizeBytes, &result.SHA256, &result.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return tracker.Checkpoint{}, staleCheckpointOwner()
	}
	if err != nil {
		return tracker.Checkpoint{}, fmt.Errorf("tracker postgres: publish checkpoint: %w", err)
	}
	return result, nil
}

func (s *Store) AbortCheckpoint(
	ctx context.Context,
	userID, agentID, projectID, shardID, uploadID string,
	generation, now int64,
) error {
	command, err := s.pool.Exec(ctx, `
		UPDATE tracker_shards sh SET
			checkpoint_upload_id=NULL, checkpoint_s3_upload_id=NULL,
			checkpoint_upload_uri=NULL, checkpoint_upload_seq=NULL,
			checkpoint_upload_generation=NULL, checkpoint_upload_checksum=NULL,
			checkpoint_upload_size=NULL, checkpoint_upload_started_at=NULL, updated_at=$7
		FROM tracker_agents a, tracker_users u
		WHERE sh.project_id=$1 AND sh.id=$2 AND sh.owner_agent_id=$3
			AND sh.generation=$4 AND sh.checkpoint_upload_id=$5
			AND sh.status=ANY($6) AND sh.owner_lease_expires_at>$7
			AND a.id=sh.owner_agent_id AND a.user_id=$8 AND a.status='online'
			AND u.id=a.user_id AND u.status='active' AND 'shard_owner'=ANY(u.roles)
	`, projectID, shardID, agentID, generation, uploadID, checkpointOwnerStatuses, now, userID)
	if err != nil {
		return fmt.Errorf("tracker postgres: abort checkpoint: %w", err)
	}
	if command.RowsAffected() != 1 {
		return staleCheckpointOwner()
	}
	return nil
}

func staleCheckpointOwner() *tracker.Error {
	return &tracker.Error{
		Code:    protocol.ErrorStaleGeneration,
		Message: "checkpoint no longer belongs to this owner generation",
	}
}
