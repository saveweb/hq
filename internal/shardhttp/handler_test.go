package shardhttp_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"git.saveweb.org/saveweb/hq/internal/access"
	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/shard"
	"git.saveweb.org/saveweb/hq/internal/shardhttp"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const httpNow = int64(1_780_000_000)

type fixture struct {
	manager *shard.Manager
	server  *httptest.Server
	token   string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	manager, err := shard.NewManager(shard.ManagerConfig{
		AgentID: "shard-1", Issuer: "https://tracker.test", DataDir: t.TempDir(),
		Clock: func() int64 { return httpNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := access.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := access.NewSigner("https://tracker.test", "key-1", privateKey, func() int64 { return httpNow })
	if err != nil {
		t.Fatal(err)
	}
	heartbeat := protocol.AgentHeartbeatResponse{
		ServerTime: httpNow, HeartbeatAfterSeconds: 30,
		SigningKeys: []protocol.SigningKey{{
			KeyID: "key-1", Algorithm: "EdDSA",
			PublicKeyEd25519: base64.RawURLEncoding.EncodeToString(ed25519.PublicKey(publicKey)),
			NotBefore:        httpNow - 60, NotAfter: httpNow + 3600,
		}},
		OwnerAssignments: []protocol.OwnerAssignment{{
			Route:  protocol.Route{ProjectID: "project-1", ShardID: "shard-a", Generation: 1},
			Status: "active", OwnerLeaseExpiresAt: httpNow + 120,
		}},
	}
	if err := manager.ApplyHeartbeat(context.Background(), heartbeat); err != nil {
		t.Fatal(err)
	}
	token, _, err := signer.Sign(access.Scope{
		WorkerAgentID: "worker-1", SessionID: "session-1", ProjectID: "project-1",
		ShardID: "shard-a", Generation: 1, OwnerAgentID: "shard-1",
	}, httpNow+120, 120)
	if err != nil {
		t.Fatal(err)
	}
	authorization, err := manager.Authorize(token)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authorization.Store.Enqueue(context.Background(), 1, httpNow, []queue.JobSpec{{
		ID: "job-1", URL: "https://example.test/1", Type: protocol.JobTypeSeed, Attrs: map[string]any{},
	}}); err != nil {
		t.Fatal(err)
	}
	config := shardhttp.DefaultConfig("shard-1")
	config.BasePath = "/edge"
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler, err := shardhttp.New(manager, config, logger)
	if err != nil {
		t.Fatal(err)
	}
	value := &fixture{manager: manager, server: httptest.NewServer(handler), token: token}
	t.Cleanup(func() {
		value.server.Close()
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})
	return value
}

func (f *fixture) post(t *testing.T, path, token string, body any) *http.Response {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, f.server.URL+"/edge"+path, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Cache-Control", "no-store, no-cache, max-age=0")
	request.Header.Set("Pragma", "no-cache")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
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

func requireStatus(t *testing.T, response *http.Response, status int) {
	t.Helper()
	if response.StatusCode == status {
		return
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	t.Fatalf("status = %d, want %d; body = %s", response.StatusCode, status, body)
}

func route() protocol.SessionRoute {
	return protocol.SessionRoute{
		Route:     protocol.Route{ProjectID: "project-1", ShardID: "shard-a", Generation: 1},
		SessionID: "session-1",
	}
}

func TestQueueHTTPFlow(t *testing.T) {
	f := newFixture(t)
	response := f.post(t, "/api/v1/queue/claim", f.token, protocol.ClaimRequest{
		SessionRoute: route(), MaxJobs: 1, LeaseSeconds: 60, AcceptTypes: []string{protocol.JobTypeSeed},
	})
	requireStatus(t, response, http.StatusOK)
	if response.Header.Get("Cloudflare-CDN-Cache-Control") != "no-store" {
		t.Fatal("queue response is missing explicit CDN no-store header")
	}
	claim := decode[protocol.ClaimResponse](t, response)
	if len(claim.Jobs) != 1 || claim.Jobs[0].ID != "job-1" {
		t.Fatalf("claim = %+v", claim)
	}

	response = f.post(t, "/api/v1/queue/complete", f.token, protocol.CompleteRequest{
		SessionRoute: route(), Items: []protocol.CompleteItem{{
			JobID: "job-1", AttemptID: claim.Jobs[0].AttemptID,
			Outcome: protocol.Outcome{Kind: "success", Meta: protocol.Attrs{}},
			DiscoveredJobs: []protocol.JobSpecV1{{
				ID: "job-2", URL: "https://example.test/2", Type: protocol.JobTypeSeed, Attrs: map[string]any{},
			}},
		}},
	})
	requireStatus(t, response, http.StatusOK)
	completed := decode[protocol.BatchResultResponse](t, response)
	if len(completed.Results) != 1 || completed.Results[0].Status != protocol.ItemStatusApplied {
		t.Fatalf("complete = %+v", completed)
	}

	response = f.post(t, "/api/v1/queue/claim", f.token, protocol.ClaimRequest{
		SessionRoute: route(), MaxJobs: 1, LeaseSeconds: 60,
	})
	requireStatus(t, response, http.StatusOK)
	claim = decode[protocol.ClaimResponse](t, response)
	if len(claim.Jobs) != 1 || claim.Jobs[0].ID != "job-2" {
		t.Fatalf("discovered claim = %+v", claim)
	}

	response = f.post(t, "/api/v1/queue/extend-lease", f.token, protocol.ExtendLeaseRequest{
		SessionRoute: route(), ExtendSeconds: 120,
		Items: []protocol.AttemptRef{{JobID: "job-2", AttemptID: claim.Jobs[0].AttemptID}},
	})
	requireStatus(t, response, http.StatusOK)
	extended := decode[protocol.BatchResultResponse](t, response)
	if extended.Results[0].LeaseExpiresAt == nil || *extended.Results[0].LeaseExpiresAt != httpNow+120 {
		t.Fatalf("extend = %+v", extended)
	}

	response = f.post(t, "/api/v1/queue/fail", f.token, protocol.FailRequest{
		SessionRoute: route(), Items: []protocol.FailItem{{
			JobID: "job-2", AttemptID: claim.Jobs[0].AttemptID, Retryable: false,
			Error: protocol.ExecutionError{Code: "fetch_failed", Message: "test", Details: protocol.Attrs{}},
		}},
	})
	requireStatus(t, response, http.StatusOK)
	failed := decode[protocol.BatchResultResponse](t, response)
	if failed.Results[0].JobStatus == nil || *failed.Results[0].JobStatus != protocol.JobStatusFailed {
		t.Fatalf("fail = %+v", failed)
	}
}

func TestEndpointChallengeAndAuthBeforeBody(t *testing.T) {
	f := newFixture(t)
	response := f.post(t, "/api/v1/shard/endpoint-challenge", "", protocol.EndpointChallenge{
		AgentID: "shard-1", Challenge: "01234567890123456789012345678901",
	})
	requireStatus(t, response, http.StatusOK)
	challenge := decode[protocol.EndpointChallenge](t, response)
	if challenge.AgentID != "shard-1" {
		t.Fatalf("challenge = %+v", challenge)
	}

	request := httptest.NewRequest(http.MethodPost, "/edge/api/v1/queue/claim", panicReader{})
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler := f.server.Config.Handler
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("invalid token status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

type panicReader struct{}

func (panicReader) Read([]byte) (int, error) { panic("unauthorized request body was read") }

func TestRouteOutsideTokenScopeIsRejected(t *testing.T) {
	f := newFixture(t)
	wrong := route()
	wrong.ShardID = "other"
	response := f.post(t, "/api/v1/queue/claim", f.token, protocol.ClaimRequest{
		SessionRoute: wrong, MaxJobs: 1, LeaseSeconds: 60,
	})
	requireStatus(t, response, http.StatusUnauthorized)
	envelope := decode[protocol.ErrorEnvelope](t, response)
	if envelope.Error.Code != protocol.ErrorInvalidAccessToken {
		t.Fatalf("error = %+v", envelope)
	}
}
