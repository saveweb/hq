package tracker_test

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"git.saveweb.org/saveweb/hq/internal/access"
	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/internal/tracker/memory"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const trackerNow = int64(1_780_000_000)

type endpointChecker struct {
	status string
	err    error
	calls  int
}

type sourceSigner struct {
	uri string
	now int64
	ttl time.Duration
}

func (s *sourceSigner) PresignGet(_ context.Context, uri string, now int64, ttl time.Duration) (string, int64, error) {
	s.uri, s.now, s.ttl = uri, now, ttl
	return "https://objects.example/source?signature=secret", now + int64(ttl/time.Second), nil
}

func (c *endpointChecker) Check(_ context.Context, _ string, _ string, _ *string) (string, error) {
	c.calls++
	return c.status, c.err
}

type fixture struct {
	store     *memory.Store
	service   *tracker.Service
	checker   *endpointChecker
	publicKey ed25519.PublicKey
	now       int64
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	value := &fixture{store: memory.New(), checker: &endpointChecker{status: tracker.EndpointHealthy}, now: trackerNow}
	value.store.AddUser(tracker.User{
		ID: "user-owner", Status: tracker.UserStatusActive,
		Roles: map[string]bool{tracker.RoleShardOwner: true},
	}, "token-owner")
	value.store.AddUser(tracker.User{
		ID: "user-worker", Status: tracker.UserStatusActive,
		Roles: map[string]bool{tracker.RoleWorker: true},
	}, "token-worker")
	value.store.AddProject(tracker.Project{ID: "project-1", Status: tracker.ProjectStatusActive})
	publicKey, privateKey, err := access.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	value.publicKey = publicKey
	signer, err := access.NewSigner("https://tracker.example", "key-1", privateKey, func() int64 { return value.now })
	if err != nil {
		t.Fatal(err)
	}
	value.service, err = tracker.NewService(value.store, value.checker, signer, func() int64 { return value.now }, tracker.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func ptr[T any](value T) *T { return &value }

func (f *fixture) registerShard(t *testing.T) {
	t.Helper()
	response, err := f.service.UpsertAgent(context.Background(), "token-owner", "agent-shard-1", protocol.AgentUpsertRequest{
		Kind: protocol.AgentKindShard, Name: "test shard", Version: "0.1.0",
		Attrs: protocol.Attrs{}, Endpoint: ptr("https://shard.example"), EndpointVersion: ptr(int64(1)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Agent.EndpointStatus != f.checker.status || response.ServerTime != f.now {
		t.Fatalf("shard registration = %+v", response)
	}
}

func (f *fixture) registerWorker(t *testing.T) {
	t.Helper()
	_, err := f.service.UpsertAgent(context.Background(), "token-worker", "agent-worker-1", protocol.AgentUpsertRequest{
		Kind: protocol.AgentKindWorker, Name: "test worker", Version: "0.1.0", Attrs: protocol.Attrs{},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestControlPlaneIssuesOfflineVerifiableAssignment(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.registerShard(t)
	f.store.AddShard(tracker.Shard{
		ProjectID: "project-1", ID: "shard-1", Status: tracker.ShardStatusActive,
		OwnerAgentID: "agent-shard-1", Generation: 7,
	})
	heartbeat, err := f.service.HeartbeatAgent(ctx, "token-owner", "agent-shard-1", protocol.AgentHeartbeatRequest{
		Version: "0.1.0", Attrs: protocol.Attrs{"capacity": 64},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(heartbeat.OwnerAssignments) != 1 || len(heartbeat.SigningKeys) != 1 {
		t.Fatalf("shard heartbeat = %+v", heartbeat)
	}
	if f.checker.calls != 2 {
		t.Fatalf("endpoint checks = %d, want registration plus heartbeat", f.checker.calls)
	}
	if heartbeat.OwnerAssignments[0].OwnerLeaseExpiresAt != f.now+120 {
		t.Fatalf("owner lease = %d", heartbeat.OwnerAssignments[0].OwnerLeaseExpiresAt)
	}

	f.registerWorker(t)
	session, err := f.service.CreateSession(ctx, "token-worker", "agent-worker-1", protocol.CreateSessionRequest{
		ProjectID: "project-1", Attrs: protocol.Attrs{"sdk": "go"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if session.LeaseExpiresAt != f.now+120 || session.HeartbeatAfterSeconds != 30 {
		t.Fatalf("session = %+v", session)
	}
	assignment, err := f.service.GetAssignment(ctx, "token-worker", "agent-worker-1", protocol.GetAssignmentRequest{
		SessionID: session.SessionID, AcceptTypes: []string{protocol.JobTypeSeed},
	})
	if err != nil {
		t.Fatal(err)
	}
	if assignment.Assignment == nil || assignment.Assignment.ShardID != "shard-1" ||
		assignment.Assignment.OwnerAgentID != "agent-shard-1" {
		t.Fatalf("assignment = %+v", assignment)
	}
	verifier, err := access.NewVerifier("https://tracker.example", map[string]ed25519.PublicKey{"key-1": f.publicKey},
		func() int64 { return f.now + 1 }, access.DefaultSkewSec)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := verifier.Verify(assignment.Assignment.AccessToken, access.Scope{
		WorkerAgentID: "agent-worker-1", SessionID: session.SessionID,
		ProjectID: "project-1", ShardID: "shard-1", Generation: 7,
		OwnerAgentID: "agent-shard-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if claims.SessionExpiresAt != session.LeaseExpiresAt || claims.ExpiresAt != f.now+600 {
		t.Fatalf("access claims = %+v", claims)
	}
}

func TestSourceLoadingUsesExactURLAndGenerationCAS(t *testing.T) {
	f := newFixture(t)
	f.registerShard(t)
	sourceURI := "s3://sources/project-1/shard-1.jobs.jsonl.zst"
	sourceFormat := "jobs-jsonl-zstd-v1"
	sourceETag := "immutable-etag"
	f.store.AddShard(tracker.Shard{
		ProjectID: "project-1", ID: "shard-1", Status: tracker.ShardStatusLoading,
		OwnerAgentID: "agent-shard-1", Generation: 7,
		SourceURI: &sourceURI, SourceFormat: &sourceFormat, SourceETag: &sourceETag,
	})
	urlSigner := &sourceSigner{}
	_, privateKey, err := access.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := access.NewSigner("https://tracker.example", "key-source", privateKey, func() int64 { return f.now })
	if err != nil {
		t.Fatal(err)
	}
	config := tracker.DefaultConfig()
	config.SourceURLSigner = urlSigner
	service, err := tracker.NewService(f.store, f.checker, signer, func() int64 { return f.now }, config)
	if err != nil {
		t.Fatal(err)
	}
	heartbeat, err := service.HeartbeatAgent(context.Background(), "token-owner", "agent-shard-1", protocol.AgentHeartbeatRequest{
		Version: "0.1.0", Attrs: protocol.Attrs{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(heartbeat.OwnerAssignments) != 1 {
		t.Fatalf("heartbeat = %+v", heartbeat)
	}
	assignment := heartbeat.OwnerAssignments[0]
	if assignment.SourceDownloadURL == nil || assignment.SourceURLExpiresAt == nil ||
		*assignment.SourceURLExpiresAt != f.now+900 || urlSigner.uri != sourceURI ||
		urlSigner.now != f.now || urlSigner.ttl != 15*time.Minute {
		t.Fatalf("source assignment = %+v; signer = %+v", assignment, urlSigner)
	}
	result, err := service.ReportShardLoad(context.Background(), "token-owner", "agent-shard-1", "project-1", "shard-1",
		protocol.ShardLoadResultRequest{Generation: 7, Success: true})
	if err != nil || result.Status != tracker.ShardStatusActive {
		t.Fatalf("load result = %+v, %v", result, err)
	}
	if _, err := service.ReportShardLoad(context.Background(), "token-owner", "agent-shard-1", "project-1", "shard-1",
		protocol.ShardLoadResultRequest{Generation: 7, Success: true}); !tracker.IsCode(err, protocol.ErrorStaleGeneration) {
		t.Fatalf("replayed load result = %v", err)
	}
	heartbeat, err = service.HeartbeatAgent(context.Background(), "token-owner", "agent-shard-1", protocol.AgentHeartbeatRequest{
		Version: "0.1.0", Attrs: protocol.Attrs{},
	})
	if err != nil || len(heartbeat.OwnerAssignments) != 1 ||
		heartbeat.OwnerAssignments[0].SourceDownloadURL != nil || heartbeat.OwnerAssignments[0].Status != tracker.ShardStatusActive {
		t.Fatalf("active source heartbeat = %+v, %v", heartbeat, err)
	}
}

func TestFailedSourceLoadHasDistinctTerminalStatus(t *testing.T) {
	f := newFixture(t)
	f.registerShard(t)
	sourceURI := "s3://sources/shard-failed.zst"
	sourceFormat := "jobs-jsonl-zstd-v1"
	sourceETag := "failed-etag"
	f.store.AddShard(tracker.Shard{
		ProjectID: "project-1", ID: "shard-failed", Status: tracker.ShardStatusRecovering,
		OwnerAgentID: "agent-shard-1", Generation: 3, OwnerLeaseExpiresAt: f.now + 120,
		SourceURI: &sourceURI, SourceFormat: &sourceFormat, SourceETag: &sourceETag,
	})
	result, err := f.service.ReportShardLoad(context.Background(), "token-owner", "agent-shard-1", "project-1", "shard-failed",
		protocol.ShardLoadResultRequest{Generation: 3, Success: false, ErrorCode: "source_decode_failed"})
	if err != nil || result.Status != tracker.ShardStatusLoadFailed {
		t.Fatalf("failed load result = %+v, %v", result, err)
	}
}

func TestAgentRolesEndpointRulesAndVersionsAreExplicit(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	_, err := f.service.UpsertAgent(ctx, "token-worker", "bad-shard", protocol.AgentUpsertRequest{
		Kind: protocol.AgentKindShard, Name: "bad", Version: "0.1.0", Attrs: protocol.Attrs{},
		Endpoint: ptr("https://shard.example"), EndpointVersion: ptr(int64(1)),
	})
	if !tracker.IsCode(err, protocol.ErrorPermissionDenied) {
		t.Fatalf("worker registered shard: %v", err)
	}
	_, err = f.service.UpsertAgent(ctx, "token-worker", "bad-worker", protocol.AgentUpsertRequest{
		Kind: protocol.AgentKindWorker, Name: "bad", Version: "0.1.0", Attrs: protocol.Attrs{},
		Endpoint: ptr("https://worker.example"), EndpointVersion: ptr(int64(1)),
	})
	if !tracker.IsCode(err, protocol.ErrorInvalidRequest) {
		t.Fatalf("worker endpoint error = %v", err)
	}

	f.registerShard(t)
	_, err = f.service.UpsertAgent(ctx, "token-owner", "agent-shard-1", protocol.AgentUpsertRequest{
		Kind: protocol.AgentKindShard, Name: "test shard", Version: "0.1.1", Attrs: protocol.Attrs{},
		Endpoint: ptr("https://changed.example"), EndpointVersion: ptr(int64(1)),
	})
	if !tracker.IsCode(err, protocol.ErrorInvalidRequest) {
		t.Fatalf("same-version endpoint change error = %v", err)
	}
	_, err = f.service.UpsertAgent(ctx, "token-owner", "agent-shard-1", protocol.AgentUpsertRequest{
		Kind: protocol.AgentKindShard, Name: "test shard", Version: "0.1.1", Attrs: protocol.Attrs{},
		Endpoint: ptr("https://changed.example"), EndpointVersion: ptr(int64(2)),
	})
	if err != nil {
		t.Fatalf("versioned endpoint change failed: %v", err)
	}
	_, err = f.service.UpsertAgent(ctx, "token-owner", "agent-shard-1", protocol.AgentUpsertRequest{
		Kind: protocol.AgentKindShard, Name: "test shard", Version: "0.1.2", Attrs: protocol.Attrs{},
		Endpoint: ptr("https://shard.example"), EndpointVersion: ptr(int64(1)),
	})
	if !tracker.IsCode(err, protocol.ErrorStaleEndpointVersion) {
		t.Fatalf("stale endpoint version error = %v", err)
	}
}

func TestSessionCannotBeRevivedAfterExpiry(t *testing.T) {
	f := newFixture(t)
	f.registerWorker(t)
	ctx := context.Background()
	session, err := f.service.CreateSession(ctx, "token-worker", "agent-worker-1", protocol.CreateSessionRequest{
		ProjectID: "project-1", Attrs: protocol.Attrs{},
	})
	if err != nil {
		t.Fatal(err)
	}
	f.now = session.LeaseExpiresAt
	_, err = f.service.HeartbeatSession(ctx, "token-worker", "agent-worker-1", session.SessionID)
	if !tracker.IsCode(err, protocol.ErrorSessionExpired) {
		t.Fatalf("expired session heartbeat error = %v", err)
	}
	_, err = f.service.GetAssignment(ctx, "token-worker", "agent-worker-1", protocol.GetAssignmentRequest{
		SessionID: session.SessionID,
	})
	if !tracker.IsCode(err, protocol.ErrorSessionExpired) {
		t.Fatalf("expired session assignment error = %v", err)
	}
}

func TestUnhealthyShardDoesNotReceiveOwnerLeaseOrAssignments(t *testing.T) {
	f := newFixture(t)
	f.checker.status = tracker.EndpointUnreachable
	f.registerShard(t)
	f.store.AddShard(tracker.Shard{
		ProjectID: "project-1", ID: "shard-1", Status: tracker.ShardStatusActive,
		OwnerAgentID: "agent-shard-1", Generation: 7,
	})
	heartbeat, err := f.service.HeartbeatAgent(context.Background(), "token-owner", "agent-shard-1", protocol.AgentHeartbeatRequest{
		Version: "0.1.0", Attrs: protocol.Attrs{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(heartbeat.OwnerAssignments) != 0 {
		t.Fatalf("unhealthy shard received assignments: %+v", heartbeat.OwnerAssignments)
	}
	f.registerWorker(t)
	session, err := f.service.CreateSession(context.Background(), "token-worker", "agent-worker-1", protocol.CreateSessionRequest{
		ProjectID: "project-1", Attrs: protocol.Attrs{},
	})
	if err != nil {
		t.Fatal(err)
	}
	assignment, err := f.service.GetAssignment(context.Background(), "token-worker", "agent-worker-1", protocol.GetAssignmentRequest{
		SessionID: session.SessionID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if assignment.Assignment != nil || assignment.RetryAfterMS == 0 {
		t.Fatalf("unhealthy assignment = %+v", assignment)
	}
}

func TestInvalidMachineTokenIsStableError(t *testing.T) {
	f := newFixture(t)
	_, err := f.service.UpsertAgent(context.Background(), "wrong", "agent-worker-1", protocol.AgentUpsertRequest{})
	if !tracker.IsCode(err, protocol.ErrorInvalidMachineToken) {
		t.Fatalf("invalid token error = %v", err)
	}
	if errors.Is(err, access.ErrScope) {
		t.Fatal("machine token error leaked into shard access token domain")
	}
}

func TestRevokedRoleCannotRenewShardOwnerLease(t *testing.T) {
	f := newFixture(t)
	f.registerShard(t)
	f.store.AddShard(tracker.Shard{
		ProjectID: "project-1", ID: "shard-1", Status: tracker.ShardStatusActive,
		OwnerAgentID: "agent-shard-1", Generation: 1,
	})
	f.store.AddUser(tracker.User{
		ID: "user-owner", Status: tracker.UserStatusActive,
		Roles: map[string]bool{tracker.RoleWorker: true},
	}, "token-owner")
	_, err := f.service.HeartbeatAgent(context.Background(), "token-owner", "agent-shard-1", protocol.AgentHeartbeatRequest{
		Version: "0.1.0", Attrs: protocol.Attrs{},
	})
	if !tracker.IsCode(err, protocol.ErrorPermissionDenied) {
		t.Fatalf("heartbeat after role revoke = %v", err)
	}
}
