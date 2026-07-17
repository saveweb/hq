package tracker

import "context"

type Store interface {
	AuthenticateMachineToken(ctx context.Context, machineToken string) (User, error)
	GetAgent(ctx context.Context, userID, agentID string) (Agent, error)
	UpsertAgent(ctx context.Context, input AgentUpsert) (Agent, error)
	HeartbeatAgent(ctx context.Context, userID, agentID, version string, attrs map[string]any,
		endpointStatus string, allowShard, allowWorker bool, now, ownerLeaseExpiresAt int64) (AgentHeartbeat, error)
	CreateSession(ctx context.Context, userID, agentID, projectID string, attrs map[string]any,
		now, leaseExpiresAt int64) (Session, error)
	HeartbeatSession(ctx context.Context, userID, agentID, sessionID string,
		now, leaseExpiresAt int64) (Session, error)
	FindAssignment(ctx context.Context, userID, agentID, sessionID string, now int64) (*AssignmentCandidate, error)
	GetReceiverTarget(ctx context.Context, userID, agentID, sessionID, receiverID string,
		now int64) (Receiver, error)
	FinishShardLoad(ctx context.Context, userID, agentID, projectID, shardID string,
		generation int64, success bool, errorCode string, now int64) (Shard, error)
	FinishShardRecovery(ctx context.Context, userID, agentID, projectID, shardID string,
		generation int64, success bool, errorCode string, now int64) (Shard, error)
	GetCheckpointTarget(ctx context.Context, userID, agentID, projectID, shardID string,
		generation, now int64) (Shard, error)
	GetCurrentCheckpointUpload(ctx context.Context, userID, agentID, projectID, shardID string,
		generation, now int64) (*CheckpointUpload, error)
	ReserveCheckpoint(ctx context.Context, userID, agentID string, upload CheckpointUpload,
		now int64) (CheckpointUpload, error)
	GetCheckpointUpload(ctx context.Context, userID, agentID, projectID, shardID, uploadID string,
		generation, now int64) (CheckpointUpload, error)
	PublishCheckpoint(ctx context.Context, userID, agentID, projectID, shardID, uploadID string,
		generation, now int64) (Checkpoint, error)
	AbortCheckpoint(ctx context.Context, userID, agentID, projectID, shardID, uploadID string,
		generation, now int64) error
}

type EndpointChecker interface {
	Check(ctx context.Context, agentID, endpoint string, tlsSPKISHA256 *string) (endpointStatus string, err error)
}
