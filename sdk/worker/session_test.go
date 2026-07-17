package worker_test

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"git.saveweb.org/saveweb/hq/internal/access"
	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/shard"
	"git.saveweb.org/saveweb/hq/internal/shardhttp"
	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/internal/tracker/memory"
	"git.saveweb.org/saveweb/hq/internal/trackerhttp"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
	"git.saveweb.org/saveweb/hq/sdk/worker"
)

type healthyChecker struct{}

func (healthyChecker) Check(context.Context, string, string, *string) (string, error) {
	return tracker.EndpointHealthy, nil
}

type fixture struct {
	now           int64
	store         *memory.Store
	service       *tracker.Service
	signer        *access.Signer
	manager       *shard.Manager
	shardServer   *httptest.Server
	trackerServer *httptest.Server
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	now := time.Now().Unix()
	store := memory.New()
	store.AddUser(tracker.User{
		ID: "owner-user", Status: tracker.UserStatusActive,
		Roles: map[string]bool{tracker.RoleShardOwner: true},
	}, "owner-token")
	store.AddUser(tracker.User{
		ID: "worker-user", Status: tracker.UserStatusActive,
		Roles: map[string]bool{tracker.RoleWorker: true},
	}, "worker-token")
	store.AddProject(tracker.Project{ID: "project-1", Status: tracker.ProjectStatusActive})
	publicKey, privateKey, err := access.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := access.NewSigner("https://issuer.test", "key-1", privateKey, func() int64 { return now })
	if err != nil {
		t.Fatal(err)
	}
	service, err := tracker.NewService(store, healthyChecker{}, signer, func() int64 { return now }, tracker.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := shard.NewManager(shard.ManagerConfig{
		AgentID: "shard-agent", Issuer: "https://issuer.test", DataDir: t.TempDir(),
		Clock: func() int64 { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	shardHandler, err := shardhttp.New(manager, shardhttp.DefaultConfig("shard-agent"), logger)
	if err != nil {
		t.Fatal(err)
	}
	shardServer := httptest.NewServer(shardHandler)
	endpoint, endpointVersion := shardServer.URL, int64(1)
	_, err = service.UpsertAgent(context.Background(), "owner-token", "shard-agent", protocol.AgentUpsertRequest{
		Kind: protocol.AgentKindShard, Name: "Shard", Version: "test", Attrs: protocol.Attrs{},
		Endpoint: &endpoint, EndpointVersion: &endpointVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	store.AddShard(tracker.Shard{
		ProjectID: "project-1", ID: "shard-a", Status: tracker.ShardStatusActive,
		OwnerAgentID: "shard-agent", Generation: 1,
	})
	heartbeat, err := service.HeartbeatAgent(context.Background(), "owner-token", "shard-agent", protocol.AgentHeartbeatRequest{
		Version: "test", Attrs: protocol.Attrs{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(heartbeat.SigningKeys) != 1 {
		t.Fatal("tracker did not return signing key")
	}
	// Assert the heartbeat key also matches the fixture key before giving it to the shard.
	if heartbeat.SigningKeys[0].PublicKeyEd25519 != base64.RawURLEncoding.EncodeToString(ed25519.PublicKey(publicKey)) {
		t.Fatal("heartbeat signing key mismatch")
	}
	if err := manager.ApplyHeartbeat(context.Background(), heartbeat); err != nil {
		t.Fatal(err)
	}
	trackerServer := httptest.NewServer(trackerhttp.New(service, logger))
	value := &fixture{
		now: now, store: store, service: service, signer: signer, manager: manager,
		shardServer: shardServer, trackerServer: trackerServer,
	}
	t.Cleanup(func() {
		trackerServer.Close()
		shardServer.Close()
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})
	return value
}

func (f *fixture) openSession(t *testing.T) *worker.Session {
	t.Helper()
	session, err := worker.OpenSession(context.Background(), worker.Config{
		TrackerURL:   f.trackerServer.URL,
		MachineToken: "worker-token", AgentID: "worker-agent",
		AgentName: "Worker", AgentVersion: "test",
		AllowHTTPTracker: true, AllowHTTPShard: true,
	}, "project-1", protocol.Attrs{"sdk": "go-test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error(err)
		}
	})
	return session
}

func (f *fixture) seed(t *testing.T, sessionID string) {
	t.Helper()
	token, _, err := f.signer.Sign(access.Scope{
		WorkerAgentID: "worker-agent", SessionID: sessionID, ProjectID: "project-1",
		ShardID: "shard-a", Generation: 1, OwnerAgentID: "shard-agent",
	}, f.now+120, 120)
	if err != nil {
		t.Fatal(err)
	}
	authorization, err := f.manager.Authorize(token)
	if err != nil {
		t.Fatal(err)
	}
	_, err = authorization.Store.Enqueue(context.Background(), 1, f.now, []queue.JobSpec{{
		ID: "job-1", URL: "https://example.test/", Type: protocol.JobTypeSeed, Attrs: map[string]any{},
	}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGoSDKClaimsAndCompletesThroughTrackerAndShard(t *testing.T) {
	f := newFixture(t)
	session := f.openSession(t)
	f.seed(t, session.ID())
	batch, err := session.Claim(context.Background(), 16, 60, []string{protocol.JobTypeSeed})
	if err != nil {
		t.Fatal(err)
	}
	jobs := batch.Jobs()
	if len(jobs) != 1 || jobs[0].ID != "job-1" || batch.Route().Generation != 1 {
		t.Fatalf("batch = %+v, route = %+v", jobs, batch.Route())
	}
	result, err := batch.Complete(context.Background(), []protocol.CompleteItem{{
		JobID: jobs[0].ID, AttemptID: jobs[0].AttemptID,
		Outcome: protocol.Outcome{Kind: "success", Meta: protocol.Attrs{}},
		DiscoveredJobs: []protocol.JobSpecV1{{
			ID: "job-2", URL: "https://example.test/2", Type: protocol.JobTypeSeed, Attrs: protocol.Attrs{},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Results) != 1 || result.Results[0].Status != protocol.ItemStatusApplied {
		t.Fatalf("complete = %+v", result)
	}
}

func TestGoSDKNeverReplaysOutcomeAcrossGeneration(t *testing.T) {
	f := newFixture(t)
	session := f.openSession(t)
	f.seed(t, session.ID())
	batch, err := session.Claim(context.Background(), 1, 60, nil)
	if err != nil {
		t.Fatal(err)
	}
	job := batch.Jobs()[0]
	f.store.AddShard(tracker.Shard{
		ProjectID: "project-1", ID: "shard-a", Status: tracker.ShardStatusActive,
		OwnerAgentID: "shard-agent", Generation: 2,
	})
	heartbeat, err := f.service.HeartbeatAgent(context.Background(), "owner-token", "shard-agent", protocol.AgentHeartbeatRequest{
		Version: "test", Attrs: protocol.Attrs{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.manager.ApplyHeartbeat(context.Background(), heartbeat); err != nil {
		t.Fatal(err)
	}
	_, err = batch.Complete(context.Background(), []protocol.CompleteItem{{
		JobID: job.ID, AttemptID: job.AttemptID,
		Outcome: protocol.Outcome{Kind: "success", Meta: protocol.Attrs{}},
	}})
	if !errors.Is(err, worker.ErrRouteRetired) {
		t.Fatalf("completion after takeover = %v", err)
	}
}
