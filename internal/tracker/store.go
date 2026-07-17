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
}

type EndpointChecker interface {
	Check(ctx context.Context, agentID, endpoint string, tlsSPKISHA256 *string) (endpointStatus string, err error)
}
