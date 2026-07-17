package worker_test

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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

func TestGoSDKLocalAdminPausesOnlyNewClaims(t *testing.T) {
	f := newFixture(t)
	session := f.openSession(t)
	t.Setenv("SAVEWEB_LOCAL_ADMIN_TOKEN", "")
	if _, err := session.StartLocalAdmin(worker.LocalAdminConfig{
		Listen: "0.0.0.0:0", Token: strings.Repeat("x", 43),
	}); err == nil {
		t.Fatal("worker local admin accepted a non-loopback listener")
	}
	admin, err := session.StartLocalAdmin(worker.LocalAdminConfig{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if !admin.TokenWasGenerated() || len(admin.Token()) != 43 || !strings.HasPrefix(admin.Address(), "127.0.0.1:") {
		t.Fatalf("local admin metadata: address=%q token length=%d generated=%v",
			admin.Address(), len(admin.Token()), admin.TokenWasGenerated())
	}
	if _, err := session.StartLocalAdmin(worker.LocalAdminConfig{
		Listen: "127.0.0.1:0", Token: strings.Repeat("x", 43),
	}); err == nil {
		t.Fatal("worker session started two local admin servers")
	}

	readStatus := func() worker.RuntimeStatus {
		request, requestError := http.NewRequest(
			http.MethodGet, "http://"+admin.Address()+"/api/v1/status", nil,
		)
		if requestError != nil {
			t.Fatal(requestError)
		}
		request.Header.Set("Authorization", "Bearer "+admin.Token())
		response, requestError := http.DefaultClient.Do(request)
		if requestError != nil {
			t.Fatal(requestError)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("local status HTTP %d", response.StatusCode)
		}
		var status worker.RuntimeStatus
		if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
			t.Fatal(err)
		}
		return status
	}
	status := readStatus()
	if status.AgentID != "worker-agent" || status.ProjectID != "project-1" || status.ClaimsPaused || status.Closed {
		t.Fatalf("initial local status = %+v", status)
	}
	webClient := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	loginForm := url.Values{"token": {admin.Token()}}.Encode()
	loginRequest, err := http.NewRequest(
		http.MethodPost, "http://"+admin.Address()+"/login", strings.NewReader(loginForm),
	)
	if err != nil {
		t.Fatal(err)
	}
	loginRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	loginRequest.Header.Set("Origin", "http://"+admin.Address())
	loginResponse, err := webClient.Do(loginRequest)
	if err != nil {
		t.Fatal(err)
	}
	loginResponse.Body.Close()
	if loginResponse.StatusCode != http.StatusSeeOther || len(loginResponse.Cookies()) != 1 {
		t.Fatalf("local admin login = %d cookies=%d", loginResponse.StatusCode, len(loginResponse.Cookies()))
	}
	pageRequest, err := http.NewRequest(http.MethodGet, "http://"+admin.Address()+"/admin", nil)
	if err != nil {
		t.Fatal(err)
	}
	pageRequest.AddCookie(loginResponse.Cookies()[0])
	pageResponse, err := webClient.Do(pageRequest)
	if err != nil {
		t.Fatal(err)
	}
	pageBody, err := io.ReadAll(pageResponse.Body)
	pageResponse.Body.Close()
	if err != nil || pageResponse.StatusCode != http.StatusOK ||
		!strings.Contains(string(pageBody), "Worker local administration") {
		t.Fatalf("local admin page = %d %q, %v", pageResponse.StatusCode, pageBody, err)
	}
	session.SetClaimsPaused(true)
	if _, err := session.Claim(context.Background(), 1, 60, nil); !errors.Is(err, worker.ErrClaimsPaused) {
		t.Fatalf("paused claim = %v", err)
	}
	if status = readStatus(); !status.ClaimsPaused {
		t.Fatalf("paused local status = %+v", status)
	}
	session.SetClaimsPaused(false)
}
