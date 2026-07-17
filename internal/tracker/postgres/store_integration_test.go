package postgres_test

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"os"
	"testing"

	"git.saveweb.org/saveweb/hq/internal/access"
	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/internal/tracker/postgres"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const integrationNow = int64(1_780_000_000)

type healthyChecker struct{}

func (healthyChecker) Check(context.Context, string, string, *string) (string, error) {
	return tracker.EndpointHealthy, nil
}

func TestPostgresStoreControlPlaneContract(t *testing.T) {
	databaseURL := os.Getenv("HQ_TEST_POSTGRES_URL")
	if databaseURL == "" {
		t.Skip("HQ_TEST_POSTGRES_URL is not set")
	}
	ctx := context.Background()
	store, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migration is not idempotent: %v", err)
	}

	if err := store.PutUserAndToken(ctx, tracker.User{
		ID: "owner", Status: tracker.UserStatusActive,
		Roles: map[string]bool{tracker.RoleShardOwner: true},
	}, "owner-token", integrationNow); err != nil {
		t.Fatal(err)
	}
	if err := store.PutUserAndToken(ctx, tracker.User{
		ID: "worker", Status: tracker.UserStatusActive,
		Roles: map[string]bool{tracker.RoleWorker: true},
	}, "worker-token", integrationNow); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateMachineToken(ctx, "not-a-token"); !tracker.IsCode(err, protocol.ErrorInvalidMachineToken) {
		t.Fatalf("invalid token error = %v", err)
	}
	githubID := int64(99)
	if err := store.PutUserAndToken(ctx, tracker.User{
		ID: "admin", GitHubUserID: &githubID, Status: tracker.UserStatusActive,
		Roles: map[string]bool{tracker.RoleAdmin: true},
	}, "admin-token", integrationNow); err != nil {
		t.Fatal(err)
	}
	if err := store.PutProject(ctx, tracker.Project{ID: "project-1", Status: tracker.ProjectStatusActive}, integrationNow); err != nil {
		t.Fatal(err)
	}

	publicKey, privateKey, err := access.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := access.NewSigner("https://tracker.test", "key-1", privateKey, func() int64 { return integrationNow })
	if err != nil {
		t.Fatal(err)
	}
	service, err := tracker.NewService(store, healthyChecker{}, signer, func() int64 { return integrationNow }, tracker.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	endpoint, endpointVersion := "https://shard.example", int64(1)
	_, err = service.UpsertAgent(ctx, "owner-token", "shard-1", protocol.AgentUpsertRequest{
		Kind: protocol.AgentKindShard, Name: "Shard", Version: "0.1.0", Attrs: protocol.Attrs{},
		Endpoint: &endpoint, EndpointVersion: &endpointVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutShard(ctx, tracker.Shard{
		ProjectID: "project-1", ID: "shard-a", Status: tracker.ShardStatusActive,
		OwnerAgentID: "shard-1", Generation: 3,
	}, integrationNow); err != nil {
		t.Fatal(err)
	}
	heartbeat, err := service.HeartbeatAgent(ctx, "owner-token", "shard-1", protocol.AgentHeartbeatRequest{
		Version: "0.1.0", Attrs: protocol.Attrs{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(heartbeat.OwnerAssignments) != 1 || heartbeat.OwnerAssignments[0].OwnerLeaseExpiresAt != integrationNow+120 {
		t.Fatalf("heartbeat = %+v", heartbeat)
	}
	sourceURI := "s3://sources/project-1/shard-source.jobs.jsonl.zst"
	sourceFormat := "jobs-jsonl-zstd-v1"
	sourceETag := "source-etag"
	if err := store.PutShard(ctx, tracker.Shard{
		ProjectID: "project-1", ID: "shard-source", Status: tracker.ShardStatusLoading,
		OwnerAgentID: "shard-1", Generation: 4,
		SourceURI: &sourceURI, SourceFormat: &sourceFormat, SourceETag: &sourceETag,
	}, integrationNow); err != nil {
		t.Fatal(err)
	}
	if _, err := store.HeartbeatAgent(ctx, "owner", "shard-1", "0.1.0", map[string]any{},
		tracker.EndpointHealthy, true, false, integrationNow, integrationNow+120); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.FinishShardLoad(ctx, "owner", "shard-1", "project-1", "shard-source", 4,
		true, "", integrationNow)
	if err != nil || loaded.Status != tracker.ShardStatusActive {
		t.Fatalf("finish source load = %+v, %v", loaded, err)
	}
	if _, err := store.FinishShardLoad(ctx, "owner", "shard-1", "project-1", "shard-source", 4,
		true, "", integrationNow); !tracker.IsCode(err, protocol.ErrorStaleGeneration) {
		t.Fatalf("replayed source load = %v", err)
	}
	changedETag := "silently-changed-etag"
	if err := store.PutShard(ctx, tracker.Shard{
		ProjectID: "project-1", ID: "shard-source", Status: tracker.ShardStatusLoading,
		OwnerAgentID: "shard-1", Generation: 4,
		SourceURI: &sourceURI, SourceFormat: &sourceFormat, SourceETag: &changedETag,
	}, integrationNow+1); !tracker.IsCode(err, protocol.ErrorStaleGeneration) {
		t.Fatalf("same-generation source mutation = %v", err)
	}
	if err := store.PutShard(ctx, tracker.Shard{
		ProjectID: "project-1", ID: "shard-source", Status: tracker.ShardStatusLoading,
		OwnerAgentID: "shard-1", Generation: 3,
		SourceURI: &sourceURI, SourceFormat: &sourceFormat, SourceETag: &sourceETag,
	}, integrationNow+1); !tracker.IsCode(err, protocol.ErrorStaleGeneration) {
		t.Fatalf("generation rollback = %v", err)
	}
	if err := store.PutShard(ctx, tracker.Shard{
		ProjectID: "project-1", ID: "shard-source", Status: tracker.ShardStatusLoading,
		OwnerAgentID: "shard-1", Generation: 5,
		SourceURI: &sourceURI, SourceFormat: &sourceFormat, SourceETag: &changedETag,
	}, integrationNow+1); !tracker.IsCode(err, protocol.ErrorStaleGeneration) {
		t.Fatalf("new-generation source mutation = %v", err)
	}

	_, err = service.UpsertAgent(ctx, "worker-token", "worker-1", protocol.AgentUpsertRequest{
		Kind: protocol.AgentKindWorker, Name: "Worker", Version: "0.1.0", Attrs: protocol.Attrs{},
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := service.CreateSession(ctx, "worker-token", "worker-1", protocol.CreateSessionRequest{
		ProjectID: "project-1", Attrs: protocol.Attrs{"integration": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutReceiver(ctx, tracker.Receiver{
		ProjectID: "project-1", ID: "receiver-1", Status: tracker.ReceiverStatusActive,
		SinkURI: "s3://receiver-output/stage-1", Format: "jobs-jsonl-zstd-v1",
	}, integrationNow); err != nil {
		t.Fatal(err)
	}
	receiver, err := store.GetReceiverTarget(ctx, "worker", "worker-1", session.SessionID, "receiver-1", integrationNow+1)
	if err != nil || receiver.SinkURI != "s3://receiver-output/stage-1" {
		t.Fatalf("receiver target = %+v, %v", receiver, err)
	}
	assignment, err := service.GetAssignment(ctx, "worker-token", "worker-1", protocol.GetAssignmentRequest{
		SessionID: session.SessionID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if assignment.Assignment == nil || assignment.Assignment.ShardID != "shard-a" || assignment.Assignment.Generation != 3 {
		t.Fatalf("assignment = %+v", assignment)
	}
	verifier, err := access.NewVerifier("https://tracker.test", map[string]ed25519.PublicKey{"key-1": publicKey},
		func() int64 { return integrationNow + 1 }, access.DefaultSkewSec)
	if err != nil {
		t.Fatal(err)
	}
	_, err = verifier.Verify(assignment.Assignment.AccessToken, access.Scope{
		WorkerAgentID: "worker-1", SessionID: session.SessionID, ProjectID: "project-1",
		ShardID: "shard-a", Generation: 3, OwnerAgentID: "shard-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	upload, err := store.ReserveCheckpoint(ctx, "owner", "shard-1", tracker.CheckpointUpload{
		ProjectID: "project-1", ShardID: "shard-a", Generation: 3,
		ID: "cp-integration-1", S3UploadID: "s3-integration-1",
		URI:       "s3://checkpoints/project-1/shard-a/cp-integration-1.sqlite.zst",
		SizeBytes: 1234, SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}, integrationNow)
	if err != nil || upload.Sequence != 1 {
		t.Fatalf("reserve checkpoint = %+v, %v", upload, err)
	}
	checkpoint, err := store.PublishCheckpoint(ctx, "owner", "shard-1", "project-1", "shard-a",
		upload.ID, 3, integrationNow+1)
	if err != nil || checkpoint.Sequence != 1 || checkpoint.Format != "sqlite-zstd-v1" {
		t.Fatalf("publish checkpoint = %+v, %v", checkpoint, err)
	}
	upload, err = store.ReserveCheckpoint(ctx, "owner", "shard-1", tracker.CheckpointUpload{
		ProjectID: "project-1", ShardID: "shard-a", Generation: 3,
		ID: "cp-integration-stale", S3UploadID: "s3-integration-stale",
		URI:       "s3://checkpoints/project-1/shard-a/cp-integration-stale.sqlite.zst",
		SizeBytes: 1235, SHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}, integrationNow+1)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutShard(ctx, tracker.Shard{
		ProjectID: "project-1", ID: "shard-a", Status: tracker.ShardStatusRecovering,
		OwnerAgentID: "shard-1", Generation: 4,
	}, integrationNow+2); !tracker.IsCode(err, protocol.ErrorStaleGeneration) {
		t.Fatalf("recovery before owner lease expiry = %v", err)
	}
	takeoverNow := integrationNow + 121
	if err := store.PutShard(ctx, tracker.Shard{
		ProjectID: "project-1", ID: "shard-a", Status: tracker.ShardStatusRecovering,
		OwnerAgentID: "shard-1", Generation: 4,
	}, takeoverNow); err != nil {
		t.Fatal(err)
	}
	recoveryHeartbeat, err := store.HeartbeatAgent(ctx, "owner", "shard-1", "0.1.0", map[string]any{},
		tracker.EndpointHealthy, true, false, takeoverNow, takeoverNow+120)
	var recoveryAssignment *protocol.OwnerAssignment
	for index := range recoveryHeartbeat.OwnerAssignments {
		if recoveryHeartbeat.OwnerAssignments[index].ShardID == "shard-a" {
			recoveryAssignment = &recoveryHeartbeat.OwnerAssignments[index]
		}
	}
	if err != nil || recoveryAssignment == nil || recoveryAssignment.Checkpoint == nil ||
		recoveryAssignment.Checkpoint.Sequence != 1 {
		t.Fatalf("checkpoint recovery heartbeat = %+v, %v", recoveryHeartbeat, err)
	}
	recovered, err := store.FinishShardRecovery(ctx, "owner", "shard-1", "project-1", "shard-a",
		4, true, "", takeoverNow+1)
	if err != nil || recovered.Status != tracker.ShardStatusActive {
		t.Fatalf("finish checkpoint recovery = %+v, %v", recovered, err)
	}
	if _, err := store.FinishShardRecovery(ctx, "owner", "shard-1", "project-1", "shard-a",
		4, true, "", takeoverNow+1); !tracker.IsCode(err, protocol.ErrorStaleGeneration) {
		t.Fatalf("replayed checkpoint recovery = %v", err)
	}
	if _, err := store.PublishCheckpoint(ctx, "owner", "shard-1", "project-1", "shard-a",
		upload.ID, 3, takeoverNow+1); !tracker.IsCode(err, protocol.ErrorStaleGeneration) {
		t.Fatalf("stale checkpoint publication = %v", err)
	}

	avatar := "https://avatars.test/admin"
	portalAdmin, err := store.UpsertGitHubUser(ctx, tracker.GitHubIdentity{
		UserID: 99, Login: "admin-login", AvatarURL: &avatar,
	}, true, integrationNow+1)
	if err != nil || portalAdmin.ID != "admin" || !portalAdmin.HasRole(tracker.RoleAdmin) {
		t.Fatalf("portal admin = %+v, %v", portalAdmin, err)
	}
	if err := store.AdminTransitionShard(ctx, "admin", "project-1", "shard-a", 3,
		tracker.ShardStatusDraining, "stale operator page", takeoverNow+4); !tracker.IsCode(err, protocol.ErrorStaleGeneration) {
		t.Fatalf("stale shard transition = %v", err)
	}
	if err := store.AdminTransitionShard(ctx, "admin", "project-1", "shard-a", 4,
		tracker.ShardStatusPaused, "must drain first", takeoverNow+4); !tracker.IsCode(err, protocol.ErrorInvalidRequest) {
		t.Fatalf("active to paused transition = %v", err)
	}
	if err := store.AdminTransitionShard(ctx, "admin", "project-1", "shard-a", 4,
		tracker.ShardStatusDraining, "stop new claims", takeoverNow+4); err != nil {
		t.Fatal(err)
	}
	if err := store.AdminTransitionShard(ctx, "admin", "project-1", "shard-a", 4,
		tracker.ShardStatusActive, "resume before pause", takeoverNow+5); err != nil {
		t.Fatal(err)
	}
	if err := store.AdminTransitionShard(ctx, "admin", "project-1", "shard-a", 4,
		tracker.ShardStatusDraining, "final drain", takeoverNow+6); err != nil {
		t.Fatal(err)
	}
	if err := store.AdminTransitionShard(ctx, "admin", "project-1", "shard-a", 4,
		tracker.ShardStatusPaused, "checkpoint is published", takeoverNow+7); err != nil {
		t.Fatal(err)
	}
	adminShards, err := store.ListAdminShards(ctx)
	pausedShard, found := findAdminShard(adminShards, "project-1", "shard-a")
	if err != nil || !found || pausedShard.Status != tracker.ShardStatusPaused || pausedShard.OwnerLeaseExpiresAt != 0 {
		t.Fatalf("paused shard = %+v, found=%v, err=%v", pausedShard, found, err)
	}
	if err := store.AdminTransitionShard(ctx, "admin", "project-1", "shard-source", 4,
		tracker.ShardStatusDraining, "test checkpoint guard", takeoverNow+8); err != nil {
		t.Fatal(err)
	}
	if err := store.AdminTransitionShard(ctx, "admin", "project-1", "shard-source", 4,
		tracker.ShardStatusPaused, "no checkpoint exists", takeoverNow+9); !tracker.IsCode(err, protocol.ErrorInvalidRequest) {
		t.Fatalf("pause without checkpoint = %v", err)
	}
	if err := store.AdminTransitionShard(ctx, "admin", "project-1", "shard-source", 4,
		tracker.ShardStatusActive, "resume after rejected pause", takeoverNow+10); err != nil {
		t.Fatal(err)
	}
	sessionHash := sha256.Sum256([]byte("integration-web-session"))
	if err := store.CreateWebSession(ctx, "admin", sessionHash[:], integrationNow, integrationNow+60); err != nil {
		t.Fatal(err)
	}
	authenticated, err := store.AuthenticateWebSession(ctx, sessionHash[:], integrationNow+1)
	if err != nil || authenticated.ID != "admin" {
		t.Fatalf("web session user = %+v, %v", authenticated, err)
	}
	newUser, err := store.UpsertGitHubUser(ctx, tracker.GitHubIdentity{
		UserID: 123, Login: "new-contributor",
	}, false, integrationNow+1)
	if err != nil || newUser.Status != tracker.UserStatusActive || !newUser.HasRole(tracker.RoleWorker) ||
		newUser.HasRole(tracker.RoleAdmin) {
		t.Fatalf("new OAuth user = %+v, %v", newUser, err)
	}
	teamAdmin, err := store.UpsertGitHubUser(ctx, tracker.GitHubIdentity{
		UserID: 123, Login: "new-contributor",
	}, true, integrationNow+2)
	if err != nil || !teamAdmin.HasRole(tracker.RoleAdmin) || !teamAdmin.HasRole(tracker.RoleShardOwner) {
		t.Fatalf("team admin = %+v, %v", teamAdmin, err)
	}
	newUser, err = store.UpsertGitHubUser(ctx, tracker.GitHubIdentity{
		UserID: 123, Login: "new-contributor",
	}, false, integrationNow+3)
	if err != nil || newUser.HasRole(tracker.RoleAdmin) || !newUser.HasRole(tracker.RoleWorker) {
		t.Fatalf("demoted team user = %+v, %v", newUser, err)
	}
	if err := store.UpdateUserAccess(ctx, "admin", newUser.ID, tracker.UserStatusActive,
		map[string]bool{tracker.RoleWorker: true}, "approved", integrationNow+4); err != nil {
		t.Fatal(err)
	}
	users, err := store.ListUsers(ctx)
	if err != nil || len(users) < 4 {
		t.Fatalf("users = %d, %v", len(users), err)
	}
	newMachineToken, err := store.ResetMachineToken(ctx, newUser.ID, integrationNow+3)
	if err != nil || len(newMachineToken) < 43 {
		t.Fatalf("new machine token = %q, %v", newMachineToken, err)
	}
	machineUser, err := store.AuthenticateMachineToken(ctx, newMachineToken)
	if err != nil || machineUser.ID != newUser.ID || !machineUser.HasRole(tracker.RoleWorker) {
		t.Fatalf("machine user = %+v, %v", machineUser, err)
	}
	if err := store.UpdateUserAccess(ctx, "admin", newUser.ID, tracker.UserStatusSuspended,
		map[string]bool{tracker.RoleWorker: true}, "suspended for policy test", integrationNow+5); err != nil {
		t.Fatal(err)
	}
	suspendedUser, err := store.UpsertGitHubUser(ctx, tracker.GitHubIdentity{
		UserID: 123, Login: "new-contributor",
	}, true, integrationNow+6)
	if err != nil || suspendedUser.Status != tracker.UserStatusSuspended ||
		suspendedUser.HasRole(tracker.RoleAdmin) {
		t.Fatalf("suspended OAuth user = %+v, %v", suspendedUser, err)
	}

	if err := store.AdminPutProject(ctx, "worker", tracker.Project{
		ID: "project-admin-denied", Status: tracker.ProjectStatusActive,
	}, "worker must not administer projects", integrationNow+4); err == nil {
		t.Fatal("non-admin project command unexpectedly succeeded")
	}
	if err := store.AdminPutProject(ctx, "admin", tracker.Project{
		ID: "project-admin", Status: tracker.ProjectStatusActive,
	}, "create from project administration", integrationNow+4); err != nil {
		t.Fatal(err)
	}
	adminSourceURI := "s3://sources/project-admin/shard-admin.jobs.jsonl.zst"
	adminSourceFormat := "jobs-jsonl-zstd-v1"
	adminSourceETag := "admin-source-etag"
	if err := store.AdminPutShard(ctx, "admin", tracker.Shard{
		ProjectID: "project-admin", ID: "shard-admin", Status: tracker.ShardStatusLoading,
		OwnerAgentID: "worker-1", Generation: 1,
		SourceURI: &adminSourceURI, SourceFormat: &adminSourceFormat, SourceETag: &adminSourceETag,
	}, "worker agent must not own shard", integrationNow+5); err == nil {
		t.Fatal("worker agent unexpectedly accepted as shard owner")
	}
	if err := store.AdminPutShard(ctx, "admin", tracker.Shard{
		ProjectID: "project-admin", ID: "shard-admin", Status: tracker.ShardStatusLoading,
		OwnerAgentID: "shard-1", Generation: 1,
		SourceURI: &adminSourceURI, SourceFormat: &adminSourceFormat, SourceETag: &adminSourceETag,
	}, "attach immutable source", integrationNow+5); err != nil {
		t.Fatal(err)
	}
	if err := store.AdminPutReceiver(ctx, "admin", tracker.Receiver{
		ProjectID: "project-admin", ID: "stage-output", Status: tracker.ReceiverStatusActive,
		SinkURI: "s3://receiver-output/project-admin/stage-output", Format: "jobs-jsonl-zstd-v1",
	}, "collect next-stage jobs", integrationNow+6); err != nil {
		t.Fatal(err)
	}
	projects, err := store.ListProjects(ctx)
	if err != nil || !hasProject(projects, "project-admin") {
		t.Fatalf("admin projects = %+v, %v", projects, err)
	}
	adminShards, err = store.ListAdminShards(ctx)
	if err != nil || !hasAdminShard(adminShards, "project-admin", "shard-admin") {
		t.Fatalf("admin shards = %+v, %v", adminShards, err)
	}
	receivers, err := store.ListReceivers(ctx)
	if err != nil || !hasReceiver(receivers, "project-admin", "stage-output") {
		t.Fatalf("admin receivers = %+v, %v", receivers, err)
	}
	shardAgents, err := store.ListShardAgents(ctx)
	if err != nil || !hasAgent(shardAgents, "shard-1") || hasAgent(shardAgents, "worker-1") {
		t.Fatalf("shard agents = %+v, %v", shardAgents, err)
	}
	audit, err := store.ListAuditEvents(ctx, 100)
	if err != nil || !hasAuditAction(audit, "receiver.put", "project-admin/stage-output") ||
		!hasAuditAction(audit, "shard.transition", "project-1/shard-a") {
		t.Fatalf("audit events = %+v, %v", audit, err)
	}
}

func hasProject(projects []tracker.Project, id string) bool {
	for _, project := range projects {
		if project.ID == id {
			return true
		}
	}
	return false
}

func hasAdminShard(shards []tracker.AdminShard, projectID, shardID string) bool {
	_, ok := findAdminShard(shards, projectID, shardID)
	return ok
}

func findAdminShard(shards []tracker.AdminShard, projectID, shardID string) (tracker.AdminShard, bool) {
	for _, shard := range shards {
		if shard.ProjectID == projectID && shard.ID == shardID {
			return shard, true
		}
	}
	return tracker.AdminShard{}, false
}

func hasReceiver(receivers []tracker.Receiver, projectID, receiverID string) bool {
	for _, receiver := range receivers {
		if receiver.ProjectID == projectID && receiver.ID == receiverID {
			return true
		}
	}
	return false
}

func hasAgent(agents []tracker.Agent, id string) bool {
	for _, agent := range agents {
		if agent.ID == id {
			return true
		}
	}
	return false
}

func hasAuditAction(events []tracker.AuditEvent, action, target string) bool {
	for _, event := range events {
		if event.Action == action && event.TargetID == target {
			return true
		}
	}
	return false
}
