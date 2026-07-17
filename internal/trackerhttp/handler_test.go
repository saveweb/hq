package trackerhttp_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"git.saveweb.org/saveweb/hq/internal/access"
	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/internal/tracker/memory"
	"git.saveweb.org/saveweb/hq/internal/trackerhttp"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const testNow = int64(1_780_000_000)

type healthyChecker struct{}

func ptr[T any](value T) *T { return &value }

func (healthyChecker) Check(context.Context, string, string, *string) (string, error) {
	return tracker.EndpointHealthy, nil
}

type fixture struct {
	server    *httptest.Server
	store     *memory.Store
	publicKey ed25519.PublicKey
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	store := memory.New()
	store.AddUser(tracker.User{
		ID: "owner", Status: tracker.UserStatusActive,
		Roles: map[string]bool{tracker.RoleShardOwner: true},
	}, "owner-token")
	store.AddUser(tracker.User{
		ID: "worker", Status: tracker.UserStatusActive,
		Roles: map[string]bool{tracker.RoleWorker: true},
	}, "worker-token")
	store.AddProject(tracker.Project{ID: "project-1", Status: tracker.ProjectStatusActive})
	publicKey, privateKey, err := access.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := access.NewSigner("https://tracker.test", "key-1", privateKey, func() int64 { return testNow })
	if err != nil {
		t.Fatal(err)
	}
	service, err := tracker.NewService(store, healthyChecker{}, signer, func() int64 { return testNow }, tracker.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &fixture{server: httptest.NewServer(trackerhttp.New(service, logger)), store: store, publicKey: publicKey}
}

func (f *fixture) close() { f.server.Close() }

func (f *fixture) request(t *testing.T, method, path, token, agentID string, body any) *http.Response {
	t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, f.server.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if agentID != "" {
		request.Header.Set("X-Saveweb-Agent-ID", agentID)
	}
	request.Header.Set("X-Request-ID", "request-1")
	response, err := f.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func decode[T any](t *testing.T, response *http.Response) T {
	t.Helper()
	defer response.Body.Close()
	var value T
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func requireStatus(t *testing.T, response *http.Response, expected int) {
	t.Helper()
	if response.StatusCode == expected {
		return
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	t.Fatalf("status = %d, want %d; body = %s", response.StatusCode, expected, body)
}

func TestFullControlPlaneHTTPFlow(t *testing.T) {
	f := newFixture(t)
	defer f.close()

	shardEndpoint := "https://shard.example"
	endpointVersion := int64(1)
	response := f.request(t, http.MethodPut, "/api/v1/agents/shard-1", "owner-token", "shard-1", protocol.AgentUpsertRequest{
		Kind: protocol.AgentKindShard, Name: "Shard One", Version: "0.1.0", Attrs: protocol.Attrs{},
		Endpoint: &shardEndpoint, EndpointVersion: &endpointVersion,
	})
	requireStatus(t, response, http.StatusOK)
	if response.Header.Get("Cache-Control") == "" || response.Header.Get("X-Request-ID") != "request-1" {
		t.Fatal("API response is missing no-store or request ID headers")
	}
	_ = decode[protocol.AgentResponse](t, response)

	f.store.AddShard(tracker.Shard{
		ProjectID: "project-1", ID: "shard-a", Status: tracker.ShardStatusActive,
		OwnerAgentID: "shard-1", Generation: 9,
	})
	response = f.request(t, http.MethodPost, "/api/v1/agents/shard-1/heartbeat", "owner-token", "shard-1", protocol.AgentHeartbeatRequest{
		Version: "0.1.0", Attrs: protocol.Attrs{},
	})
	requireStatus(t, response, http.StatusOK)
	heartbeat := decode[protocol.AgentHeartbeatResponse](t, response)
	if len(heartbeat.OwnerAssignments) != 1 || len(heartbeat.SigningKeys) != 1 {
		t.Fatalf("heartbeat = %+v", heartbeat)
	}

	response = f.request(t, http.MethodPut, "/api/v1/agents/worker-1", "worker-token", "worker-1", protocol.AgentUpsertRequest{
		Kind: protocol.AgentKindWorker, Name: "Worker One", Version: "0.1.0", Attrs: protocol.Attrs{},
	})
	requireStatus(t, response, http.StatusOK)
	response.Body.Close()

	response = f.request(t, http.MethodPost, "/api/v1/worker/sessions", "worker-token", "worker-1", protocol.CreateSessionRequest{
		ProjectID: "project-1", Attrs: protocol.Attrs{"sdk": "go"},
	})
	requireStatus(t, response, http.StatusCreated)
	session := decode[protocol.SessionResponse](t, response)

	response = f.request(t, http.MethodPost, "/api/v1/worker/assignments", "worker-token", "worker-1", protocol.GetAssignmentRequest{
		SessionID: session.SessionID, AcceptTypes: []string{protocol.JobTypeSeed},
	})
	requireStatus(t, response, http.StatusOK)
	assignment := decode[protocol.GetAssignmentResponse](t, response)
	if assignment.Assignment == nil || assignment.Assignment.ShardID != "shard-a" || assignment.Assignment.Generation != 9 {
		t.Fatalf("assignment = %+v", assignment)
	}
	verifier, err := access.NewVerifier("https://tracker.test", map[string]ed25519.PublicKey{"key-1": f.publicKey},
		func() int64 { return testNow + 1 }, access.DefaultSkewSec)
	if err != nil {
		t.Fatal(err)
	}
	_, err = verifier.Verify(assignment.Assignment.AccessToken, access.Scope{
		WorkerAgentID: "worker-1", SessionID: session.SessionID, ProjectID: "project-1",
		ShardID: "shard-a", Generation: 9, OwnerAgentID: "shard-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	f.store.AddShard(tracker.Shard{
		ProjectID: "project-1", ID: "shard-loading", Status: tracker.ShardStatusLoading,
		OwnerAgentID: "shard-1", Generation: 10, OwnerLeaseExpiresAt: testNow + 120,
		SourceURI: ptr("s3://sources/shard-loading.zst"), SourceFormat: ptr("jobs-jsonl-zstd-v1"),
		SourceETag: ptr("etag-loading"),
	})
	response = f.request(t, http.MethodPost, "/api/v1/shards/project-1/shard-loading/load-result", "owner-token", "shard-1",
		protocol.ShardLoadResultRequest{Generation: 10, Success: true})
	requireStatus(t, response, http.StatusOK)
	loadResult := decode[protocol.ShardLoadResultResponse](t, response)
	if loadResult.Status != tracker.ShardStatusActive || loadResult.Generation != 10 {
		t.Fatalf("load result = %+v", loadResult)
	}
}

func TestHTTPBoundaryRejectsMissingCredentialsMismatchAndUnknownJSON(t *testing.T) {
	f := newFixture(t)
	defer f.close()

	response := f.request(t, http.MethodPut, "/api/v1/agents/worker-1", "", "", protocol.AgentUpsertRequest{})
	requireStatus(t, response, http.StatusUnauthorized)
	errorBody := decode[protocol.ErrorEnvelope](t, response)
	if errorBody.Error.Code != protocol.ErrorInvalidMachineToken {
		t.Fatalf("error = %+v", errorBody)
	}

	response = f.request(t, http.MethodPut, "/api/v1/agents/worker-1", "worker-token", "different-agent", protocol.AgentUpsertRequest{})
	requireStatus(t, response, http.StatusForbidden)
	response.Body.Close()

	raw := []byte(`{"kind":"worker","name":"Worker","version":"1","attrs":{},"surprise":true}`)
	request, err := http.NewRequest(http.MethodPut, f.server.URL+"/api/v1/agents/worker-1", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer worker-token")
	request.Header.Set("X-Saveweb-Agent-ID", "worker-1")
	response, err = f.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	requireStatus(t, response, http.StatusBadRequest)
	errorBody = decode[protocol.ErrorEnvelope](t, response)
	if errorBody.Error.Code != protocol.ErrorInvalidRequest {
		t.Fatalf("error = %+v", errorBody)
	}
}

func TestHealthIsPublic(t *testing.T) {
	f := newFixture(t)
	defer f.close()
	response := f.request(t, http.MethodGet, "/healthz", "", "", nil)
	requireStatus(t, response, http.StatusOK)
	health := decode[map[string]string](t, response)
	if health["status"] != "ok" {
		t.Fatalf("health = %+v", health)
	}
}
