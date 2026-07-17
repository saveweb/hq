// Package memory provides a concurrency-safe tracker store for tests and
// single-process development. Production uses the PostgreSQL implementation.
package memory

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"sort"
	"sync"

	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

var _ tracker.Store = (*Store)(nil)

type Store struct {
	mu                sync.RWMutex
	users             map[string]tracker.User
	tokenUsers        map[string]string
	agents            map[string]tracker.Agent
	projects          map[string]tracker.Project
	shards            map[string]tracker.Shard
	sessions          map[string]tracker.Session
	checkpointUploads map[string]tracker.CheckpointUpload
	checkpoints       map[string]tracker.Checkpoint
}

func New() *Store {
	return &Store{
		users: make(map[string]tracker.User), tokenUsers: make(map[string]string),
		agents: make(map[string]tracker.Agent), projects: make(map[string]tracker.Project),
		shards: make(map[string]tracker.Shard), sessions: make(map[string]tracker.Session),
		checkpointUploads: make(map[string]tracker.CheckpointUpload),
		checkpoints:       make(map[string]tracker.Checkpoint),
	}
}

func (s *Store) AddUser(user tracker.User, machineToken string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	user.Roles = cloneRoles(user.Roles)
	s.users[user.ID] = user
	s.tokenUsers[machineToken] = user.ID
}

func (s *Store) AddProject(project tracker.Project) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.projects[project.ID] = project
}

func (s *Store) AddShard(shard tracker.Shard) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shards[shardKey(shard.ProjectID, shard.ID)] = cloneShard(shard)
}

func (s *Store) AddCheckpoint(checkpoint tracker.Checkpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoints[shardKey(checkpoint.ProjectID, checkpoint.ShardID)] = checkpoint
}

func (s *Store) AuthenticateMachineToken(_ context.Context, machineToken string) (tracker.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	userID, ok := s.tokenUsers[machineToken]
	if !ok {
		return tracker.User{}, &tracker.Error{Code: protocol.ErrorInvalidMachineToken, Message: "invalid machine token"}
	}
	return cloneUser(s.users[userID]), nil
}

func (s *Store) GetAgent(_ context.Context, userID, agentID string) (tracker.Agent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	agent, ok := s.agents[agentID]
	if !ok || agent.UserID != userID {
		return tracker.Agent{}, &tracker.Error{Code: protocol.ErrorNotFound, Message: "agent not found"}
	}
	return cloneAgent(agent), nil
}

func (s *Store) UpsertAgent(_ context.Context, input tracker.AgentUpsert) (tracker.Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, exists := s.agents[input.ID]
	if exists {
		if existing.UserID != input.UserID {
			return tracker.Agent{}, &tracker.Error{Code: protocol.ErrorPermissionDenied, Message: "agent belongs to another user"}
		}
		if existing.Kind != input.Kind {
			return tracker.Agent{}, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "agent kind is immutable"}
		}
		if input.Kind == protocol.AgentKindShard {
			if existing.EndpointVersion != nil && input.EndpointVersion != nil {
				if *input.EndpointVersion < *existing.EndpointVersion {
					return tracker.Agent{}, &tracker.Error{
						Code: protocol.ErrorStaleEndpointVersion, Message: "endpoint version is stale",
						Details: map[string]any{"current_endpoint_version": *existing.EndpointVersion},
					}
				}
				if *input.EndpointVersion == *existing.EndpointVersion &&
					(!equalStringPointer(existing.Endpoint, input.Endpoint) ||
						!equalStringPointer(existing.TLSSPKISHA256, input.TLSSPKISHA256)) {
					return tracker.Agent{}, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "endpoint identity changed without increasing endpoint version"}
				}
			}
		}
		if existing.Status == "revoked" {
			return tracker.Agent{}, &tracker.Error{Code: protocol.ErrorAgentDisabled, Message: "agent is revoked"}
		}
	}
	now := input.Now
	agent := tracker.Agent{
		ID: input.ID, UserID: input.UserID, Kind: input.Kind, Name: input.Name,
		Version: input.Version, Status: "online", Endpoint: cloneString(input.Endpoint),
		EndpointVersion: cloneInt64(input.EndpointVersion), TLSSPKISHA256: cloneString(input.TLSSPKISHA256),
		EndpointStatus: input.EndpointStatus, Attrs: cloneMap(input.Attrs), LastHeartbeatAt: &now,
	}
	s.agents[agent.ID] = agent
	return cloneAgent(agent), nil
}

func (s *Store) HeartbeatAgent(
	_ context.Context,
	userID, agentID, version string,
	attrs map[string]any,
	endpointStatus string,
	allowShard, allowWorker bool,
	now, ownerLeaseExpiresAt int64,
) (tracker.AgentHeartbeat, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	agent, ok := s.agents[agentID]
	if !ok || agent.UserID != userID {
		return tracker.AgentHeartbeat{}, &tracker.Error{Code: protocol.ErrorNotFound, Message: "agent not found"}
	}
	if agent.Status == "revoked" {
		return tracker.AgentHeartbeat{}, &tracker.Error{Code: protocol.ErrorAgentDisabled, Message: "agent is revoked"}
	}
	if agent.Kind == protocol.AgentKindShard && !allowShard || agent.Kind == protocol.AgentKindWorker && !allowWorker {
		return tracker.AgentHeartbeat{}, &tracker.Error{Code: protocol.ErrorPermissionDenied, Message: "agent role is no longer granted"}
	}
	agent.Version = version
	agent.Attrs = cloneMap(attrs)
	agent.EndpointStatus = endpointStatus
	agent.Status = "online"
	agent.LastHeartbeatAt = cloneInt64(&now)
	s.agents[agentID] = agent

	assignments := []protocol.OwnerAssignment{}
	if agent.Kind == protocol.AgentKindShard && endpointUsable(agent.EndpointStatus) {
		keys := make([]string, 0, len(s.shards))
		for key := range s.shards {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			shard := s.shards[key]
			if shard.OwnerAgentID != agentID || !ownerStatus(shard.Status) {
				continue
			}
			shard.OwnerLeaseExpiresAt = ownerLeaseExpiresAt
			s.shards[key] = shard
			assignment := protocol.OwnerAssignment{
				Route:  protocol.Route{ProjectID: shard.ProjectID, ShardID: shard.ID, Generation: shard.Generation},
				Status: shard.Status, OwnerLeaseExpiresAt: shard.OwnerLeaseExpiresAt,
				SourceURI: cloneString(shard.SourceURI), SourceFormat: cloneString(shard.SourceFormat),
				SourceETag: cloneString(shard.SourceETag),
			}
			if checkpoint, ok := s.checkpoints[key]; ok && shard.Status == tracker.ShardStatusRecovering {
				assignment.Checkpoint = &protocol.CheckpointRestore{
					URI:        checkpoint.URI,
					Generation: checkpoint.Generation, Sequence: checkpoint.Sequence,
					Format: checkpoint.Format, SHA256: checkpoint.SHA256,
					SizeBytes: checkpoint.SizeBytes, CreatedAt: checkpoint.CreatedAt,
				}
			}
			assignments = append(assignments, assignment)
		}
	}
	return tracker.AgentHeartbeat{Agent: cloneAgent(agent), OwnerAssignments: assignments}, nil
}

func (s *Store) CreateSession(
	_ context.Context,
	userID, agentID, projectID string,
	attrs map[string]any,
	now, leaseExpiresAt int64,
) (tracker.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	agent, ok := s.agents[agentID]
	if !ok || agent.UserID != userID || agent.Kind != protocol.AgentKindWorker {
		return tracker.Session{}, &tracker.Error{Code: protocol.ErrorPermissionDenied, Message: "active worker agent required"}
	}
	if agent.Status != "online" {
		return tracker.Session{}, &tracker.Error{Code: protocol.ErrorAgentDisabled, Message: "worker agent is not online"}
	}
	project, ok := s.projects[projectID]
	if !ok {
		return tracker.Session{}, &tracker.Error{Code: protocol.ErrorNotFound, Message: "project not found"}
	}
	if project.Status != tracker.ProjectStatusActive {
		return tracker.Session{}, &tracker.Error{Code: protocol.ErrorShardNotActive, Message: "project is not active"}
	}
	sessionID, err := newID("vs_")
	if err != nil {
		return tracker.Session{}, err
	}
	session := tracker.Session{
		ID: sessionID, ProjectID: projectID, AgentID: agentID, UserID: userID,
		Attrs: cloneMap(attrs), CreatedAt: now, LeaseExpiresAt: leaseExpiresAt, LastHeartbeatAt: now,
	}
	s.sessions[session.ID] = session
	return cloneSession(session), nil
}

func (s *Store) HeartbeatSession(
	_ context.Context,
	userID, agentID, sessionID string,
	now, leaseExpiresAt int64,
) (tracker.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.sessions[sessionID]
	if !ok || session.UserID != userID || session.AgentID != agentID {
		return tracker.Session{}, &tracker.Error{Code: protocol.ErrorNotFound, Message: "session not found"}
	}
	if now >= session.LeaseExpiresAt {
		return tracker.Session{}, &tracker.Error{Code: protocol.ErrorSessionExpired, Message: "session lease expired"}
	}
	agent := s.agents[agentID]
	if agent.Status != "online" {
		return tracker.Session{}, &tracker.Error{Code: protocol.ErrorAgentDisabled, Message: "worker agent is not online"}
	}
	session.LastHeartbeatAt = now
	session.LeaseExpiresAt = leaseExpiresAt
	s.sessions[sessionID] = session
	return cloneSession(session), nil
}

func (s *Store) FindAssignment(
	_ context.Context,
	userID, agentID, sessionID string,
	now int64,
) (*tracker.AssignmentCandidate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session, ok := s.sessions[sessionID]
	if !ok || session.UserID != userID || session.AgentID != agentID {
		return nil, &tracker.Error{Code: protocol.ErrorNotFound, Message: "session not found"}
	}
	if now >= session.LeaseExpiresAt {
		return nil, &tracker.Error{Code: protocol.ErrorSessionExpired, Message: "session lease expired"}
	}
	project, ok := s.projects[session.ProjectID]
	if !ok || project.Status != tracker.ProjectStatusActive {
		return nil, &tracker.Error{Code: protocol.ErrorShardNotActive, Message: "project is not active"}
	}
	keys := make([]string, 0, len(s.shards))
	for key, shard := range s.shards {
		if shard.ProjectID == session.ProjectID && shard.Status == tracker.ShardStatusActive &&
			shard.OwnerLeaseExpiresAt > now {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		shard := s.shards[key]
		owner, ok := s.agents[shard.OwnerAgentID]
		if !ok || owner.Kind != protocol.AgentKindShard || owner.Status != "online" ||
			owner.Endpoint == nil || owner.EndpointVersion == nil || !endpointUsable(owner.EndpointStatus) {
			continue
		}
		ownerUser := s.users[owner.UserID]
		if ownerUser.Status != tracker.UserStatusActive || !ownerUser.HasRole(tracker.RoleShardOwner) {
			continue
		}
		return &tracker.AssignmentCandidate{
			Session: cloneSession(session), Shard: cloneShard(shard), Agent: cloneAgent(owner),
		}, nil
	}
	return nil, nil
}

func (s *Store) FinishShardLoad(
	_ context.Context,
	userID, agentID, projectID, shardID string,
	generation int64,
	success bool,
	_ string,
	now int64,
) (tracker.Shard, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := shardKey(projectID, shardID)
	value, ok := s.shards[key]
	agent, agentOK := s.agents[agentID]
	user := s.users[userID]
	if !ok || !agentOK || agent.UserID != userID || value.OwnerAgentID != agentID ||
		value.Generation != generation || value.OwnerLeaseExpiresAt <= now ||
		value.SourceURI == nil || value.SourceFormat == nil || *value.SourceFormat != "jobs-jsonl-zstd-v1" ||
		value.SourceETag == nil || agent.Status != "online" ||
		value.Status != tracker.ShardStatusLoading ||
		user.Status != tracker.UserStatusActive || !user.HasRole(tracker.RoleShardOwner) {
		return tracker.Shard{}, &tracker.Error{
			Code:    protocol.ErrorStaleGeneration,
			Message: "source load no longer belongs to this owner generation",
		}
	}
	if success {
		value.Status = tracker.ShardStatusActive
	} else {
		value.Status = tracker.ShardStatusLoadFailed
		value.OwnerLeaseExpiresAt = 0
	}
	s.shards[key] = value
	return cloneShard(value), nil
}

func (s *Store) FinishShardRecovery(
	_ context.Context,
	userID, agentID, projectID, shardID string,
	generation int64,
	success bool,
	_ string,
	now int64,
) (tracker.Shard, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := shardKey(projectID, shardID)
	value, ok := s.shards[key]
	agent, agentOK := s.agents[agentID]
	user := s.users[userID]
	checkpoint, checkpointOK := s.checkpoints[key]
	if !ok || !agentOK || agent.UserID != userID || value.OwnerAgentID != agentID ||
		value.Generation != generation || value.OwnerLeaseExpiresAt <= now ||
		value.Status != tracker.ShardStatusRecovering || !checkpointOK ||
		checkpoint.Generation > generation || agent.Status != "online" ||
		user.Status != tracker.UserStatusActive || !user.HasRole(tracker.RoleShardOwner) {
		return tracker.Shard{}, &tracker.Error{
			Code:    protocol.ErrorStaleGeneration,
			Message: "checkpoint recovery no longer belongs to this owner generation",
		}
	}
	if success {
		value.Status = tracker.ShardStatusActive
	} else {
		value.Status = tracker.ShardStatusRecoveryFailed
		value.OwnerLeaseExpiresAt = 0
	}
	s.shards[key] = value
	return cloneShard(value), nil
}

func newID(prefix string) (string, error) {
	var value [18]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return prefix + base64.RawURLEncoding.EncodeToString(value[:]), nil
}

func shardKey(projectID, shardID string) string { return projectID + "\x00" + shardID }

func ownerStatus(status string) bool {
	return status == tracker.ShardStatusLoading || status == tracker.ShardStatusActive ||
		status == tracker.ShardStatusDraining || status == tracker.ShardStatusRecovering ||
		status == tracker.ShardStatusOffline
}

func endpointUsable(status string) bool {
	return status == tracker.EndpointHealthy || status == tracker.EndpointInsecure
}

func equalStringPointer(left, right *string) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneMap(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneRoles(input map[string]bool) map[string]bool {
	output := make(map[string]bool, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneUser(user tracker.User) tracker.User {
	user.Roles = cloneRoles(user.Roles)
	user.GitHubUserID = cloneInt64(user.GitHubUserID)
	user.GitHubAvatarURL = cloneString(user.GitHubAvatarURL)
	user.LastLoginAt = cloneInt64(user.LastLoginAt)
	return user
}

func cloneAgent(agent tracker.Agent) tracker.Agent {
	agent.Endpoint = cloneString(agent.Endpoint)
	agent.EndpointVersion = cloneInt64(agent.EndpointVersion)
	agent.TLSSPKISHA256 = cloneString(agent.TLSSPKISHA256)
	agent.LastHeartbeatAt = cloneInt64(agent.LastHeartbeatAt)
	agent.Attrs = cloneMap(agent.Attrs)
	return agent
}

func cloneShard(shard tracker.Shard) tracker.Shard {
	shard.SourceURI = cloneString(shard.SourceURI)
	shard.SourceFormat = cloneString(shard.SourceFormat)
	shard.SourceETag = cloneString(shard.SourceETag)
	return shard
}

func cloneSession(session tracker.Session) tracker.Session {
	session.Attrs = cloneMap(session.Attrs)
	return session
}
