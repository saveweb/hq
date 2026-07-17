package memory

import (
	"context"

	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func (s *Store) GetCheckpointTarget(
	_ context.Context,
	userID, agentID, projectID, shardID string,
	generation, now int64,
) (tracker.Shard, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.checkpointTarget(userID, agentID, projectID, shardID, generation, now)
}

func (s *Store) GetCurrentCheckpointUpload(
	_ context.Context,
	userID, agentID, projectID, shardID string,
	generation, now int64,
) (*tracker.CheckpointUpload, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, err := s.checkpointTarget(userID, agentID, projectID, shardID, generation, now); err != nil {
		return nil, err
	}
	upload, ok := s.checkpointUploads[shardKey(projectID, shardID)]
	if !ok {
		return nil, nil
	}
	copy := upload
	return &copy, nil
}

func (s *Store) ReserveCheckpoint(
	_ context.Context,
	userID, agentID string,
	upload tracker.CheckpointUpload,
	now int64,
) (tracker.CheckpointUpload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.checkpointTarget(userID, agentID, upload.ProjectID, upload.ShardID, upload.Generation, now); err != nil {
		return tracker.CheckpointUpload{}, err
	}
	key := shardKey(upload.ProjectID, upload.ShardID)
	if _, exists := s.checkpointUploads[key]; exists {
		return tracker.CheckpointUpload{}, &tracker.Error{
			Code: protocol.ErrorCheckpointInProgress, Message: "another checkpoint upload is already active",
		}
	}
	upload.Sequence = s.checkpoints[key].Sequence + 1
	upload.CreatedAt = now
	s.checkpointUploads[key] = upload
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
	_ context.Context,
	userID, agentID, projectID, shardID, uploadID string,
	generation, now int64,
) (tracker.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.checkpointTarget(userID, agentID, projectID, shardID, generation, now); err != nil {
		return tracker.Checkpoint{}, err
	}
	key := shardKey(projectID, shardID)
	upload, ok := s.checkpointUploads[key]
	if !ok || upload.ID != uploadID {
		return tracker.Checkpoint{}, staleCheckpointOwner()
	}
	checkpoint := tracker.Checkpoint{
		ProjectID: projectID, ShardID: shardID, Generation: generation,
		Sequence: upload.Sequence, URI: upload.URI, Format: "sqlite-zstd-v1",
		SizeBytes: upload.SizeBytes, SHA256: upload.SHA256, CreatedAt: now,
	}
	s.checkpoints[key] = checkpoint
	delete(s.checkpointUploads, key)
	return checkpoint, nil
}

func (s *Store) AbortCheckpoint(
	_ context.Context,
	userID, agentID, projectID, shardID, uploadID string,
	generation, now int64,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.checkpointTarget(userID, agentID, projectID, shardID, generation, now); err != nil {
		return err
	}
	key := shardKey(projectID, shardID)
	upload, ok := s.checkpointUploads[key]
	if !ok || upload.ID != uploadID {
		return staleCheckpointOwner()
	}
	delete(s.checkpointUploads, key)
	return nil
}

func (s *Store) checkpointTarget(
	userID, agentID, projectID, shardID string,
	generation, now int64,
) (tracker.Shard, error) {
	shard, shardOK := s.shards[shardKey(projectID, shardID)]
	agent, agentOK := s.agents[agentID]
	user := s.users[userID]
	validStatus := shard.Status == tracker.ShardStatusActive || shard.Status == tracker.ShardStatusDraining
	if !shardOK || !agentOK || agent.UserID != userID || agent.Status != "online" ||
		shard.OwnerAgentID != agentID || shard.Generation != generation || !validStatus ||
		shard.OwnerLeaseExpiresAt <= now || user.Status != tracker.UserStatusActive ||
		!user.HasRole(tracker.RoleShardOwner) {
		return tracker.Shard{}, staleCheckpointOwner()
	}
	return cloneShard(shard), nil
}

func staleCheckpointOwner() *tracker.Error {
	return &tracker.Error{
		Code:    protocol.ErrorStaleGeneration,
		Message: "checkpoint no longer belongs to this owner generation",
	}
}
