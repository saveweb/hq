package postgres_test

import (
	"context"
	"crypto/ed25519"
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
}
