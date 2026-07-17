package trackerclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func TestClientSendsMachineBoundaryAndDecodesControlPlane(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer machine-token" ||
			request.Header.Get("X-Saveweb-Agent-ID") != "agent-1" ||
			!strings.Contains(request.Header.Get("Cache-Control"), "no-store") {
			t.Errorf("request headers = %+v", request.Header)
		}
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Cache-Control", "no-store")
		switch request.URL.Path {
		case "/api/v1/agents/agent-1":
			if request.Method != http.MethodPut {
				t.Errorf("method = %s", request.Method)
			}
			_ = json.NewEncoder(response).Encode(protocol.AgentResponse{
				Agent: protocol.Agent{ID: "agent-1", Kind: protocol.AgentKindWorker}, ServerTime: 100,
			})
		case "/api/v1/agents/agent-1/heartbeat":
			_ = json.NewEncoder(response).Encode(protocol.AgentHeartbeatResponse{ServerTime: 100, HeartbeatAfterSeconds: 30})
		case "/api/v1/worker/sessions":
			response.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(response).Encode(protocol.SessionResponse{SessionID: "session-1", LeaseExpiresAt: 200})
		case "/api/v1/worker/sessions/session-1/heartbeat":
			_ = json.NewEncoder(response).Encode(protocol.SessionResponse{SessionID: "session-1", LeaseExpiresAt: 220})
		case "/api/v1/worker/assignments":
			_ = json.NewEncoder(response).Encode(protocol.GetAssignmentResponse{RetryAfterMS: 250})
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	client, err := New(Config{
		BaseURL: server.URL, MachineToken: "machine-token", AgentID: "agent-1", AllowHTTP: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	agent, err := client.UpsertAgent(ctx, protocol.AgentUpsertRequest{})
	if err != nil || agent.Agent.ID != "agent-1" {
		t.Fatalf("agent = %+v, %v", agent, err)
	}
	heartbeat, err := client.HeartbeatAgent(ctx, protocol.AgentHeartbeatRequest{})
	if err != nil || heartbeat.HeartbeatAfterSeconds != 30 {
		t.Fatalf("heartbeat = %+v, %v", heartbeat, err)
	}
	session, err := client.CreateSession(ctx, protocol.CreateSessionRequest{})
	if err != nil || session.SessionID != "session-1" {
		t.Fatalf("session = %+v, %v", session, err)
	}
	session, err = client.HeartbeatSession(ctx, "session-1")
	if err != nil || session.LeaseExpiresAt != 220 {
		t.Fatalf("session heartbeat = %+v, %v", session, err)
	}
	assignment, err := client.GetAssignment(ctx, protocol.GetAssignmentRequest{})
	if err != nil || assignment.RetryAfterMS != 250 {
		t.Fatalf("assignment = %+v, %v", assignment, err)
	}
}

func TestClientReturnsStableAPIAndCacheErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Cache-Control", "no-store")
		response.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(response).Encode(protocol.ErrorEnvelope{Error: protocol.APIError{
			Code: protocol.ErrorInvalidMachineToken, Message: "bad token", Details: protocol.Attrs{},
		}})
	}))
	defer server.Close()
	client, _ := New(Config{BaseURL: server.URL, MachineToken: "bad", AgentID: "agent-1", AllowHTTP: true})
	_, err := client.HeartbeatAgent(context.Background(), protocol.AgentHeartbeatRequest{})
	var clientError *Error
	if !errors.As(err, &clientError) || clientError.Status != http.StatusUnauthorized ||
		clientError.API.Code != protocol.ErrorInvalidMachineToken {
		t.Fatalf("API error = %v", err)
	}

	cacheServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Cache-Control", "no-store")
		response.Header().Set("CF-Cache-Status", "HIT")
		_ = json.NewEncoder(response).Encode(protocol.AgentHeartbeatResponse{})
	}))
	defer cacheServer.Close()
	client, _ = New(Config{BaseURL: cacheServer.URL, MachineToken: "token", AgentID: "agent-1", AllowHTTP: true})
	_, err = client.HeartbeatAgent(context.Background(), protocol.AgentHeartbeatRequest{})
	if !errors.As(err, &clientError) || clientError.API.Code != protocol.ErrorCacheMisconfigured {
		t.Fatalf("cache error = %v", err)
	}
}

func TestClientRequiresExplicitHTTP(t *testing.T) {
	if _, err := New(Config{BaseURL: "http://tracker.test", MachineToken: "token", AgentID: "agent"}); err == nil {
		t.Fatal("HTTP tracker accepted without explicit opt-in")
	}
}
