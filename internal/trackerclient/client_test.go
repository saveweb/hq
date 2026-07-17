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
		case "/api/v1/shards/project-1/shard-1/load-result":
			_ = json.NewEncoder(response).Encode(protocol.ShardLoadResultResponse{
				ProjectID: "project-1", ShardID: "shard-1", Generation: 4, Status: "active",
			})
		case "/api/v1/shards/project-1/shard-1/checkpoints":
			response.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(response).Encode(protocol.BeginCheckpointResponse{
				ProjectID: "project-1", ShardID: "shard-1", Generation: 4,
				UploadID: "cp-1", PartSizeBytes: 8 << 20, CreatedAt: 100,
			})
		case "/api/v1/shards/project-1/shard-1/checkpoints/cp-1/parts":
			_ = json.NewEncoder(response).Encode(protocol.CheckpointPartURLResponse{
				UploadID: "cp-1", PartNumber: 1, URL: "https://objects.test/part",
				Headers: map[string]string{"Content-Md5": "AAAAAAAAAAAAAAAAAAAAAA=="}, ExpiresAt: 200,
			})
		case "/api/v1/shards/project-1/shard-1/checkpoints/cp-1/complete":
			_ = json.NewEncoder(response).Encode(protocol.CheckpointResponse{
				ProjectID: "project-1", ShardID: "shard-1", Generation: 4, Sequence: 1,
				URI: "s3://bucket/checkpoint", Format: "sqlite-zstd-v1", SHA256: strings.Repeat("a", 64),
				SizeBytes: 100, CreatedAt: 101,
			})
		case "/api/v1/shards/project-1/shard-1/checkpoints/cp-1/abort":
			_ = json.NewEncoder(response).Encode(protocol.AbortCheckpointResponse{UploadID: "cp-1", Status: "aborted"})
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
	loadResult, err := client.ReportShardLoad(ctx, "project-1", "shard-1", protocol.ShardLoadResultRequest{
		Generation: 4, Success: true,
	})
	if err != nil || loadResult.Status != "active" {
		t.Fatalf("load result = %+v, %v", loadResult, err)
	}
	checkpoint, err := client.BeginCheckpoint(ctx, "project-1", "shard-1", protocol.BeginCheckpointRequest{Generation: 4})
	if err != nil || checkpoint.UploadID != "cp-1" {
		t.Fatalf("begin checkpoint = %+v, %v", checkpoint, err)
	}
	part, err := client.PresignCheckpointPart(ctx, "project-1", "shard-1", "cp-1", protocol.CheckpointPartURLRequest{})
	if err != nil || part.PartNumber != 1 {
		t.Fatalf("checkpoint part = %+v, %v", part, err)
	}
	published, err := client.CompleteCheckpoint(ctx, "project-1", "shard-1", "cp-1", protocol.CompleteCheckpointRequest{})
	if err != nil || published.Sequence != 1 {
		t.Fatalf("complete checkpoint = %+v, %v", published, err)
	}
	aborted, err := client.AbortCheckpoint(ctx, "project-1", "shard-1", "cp-1", protocol.AbortCheckpointRequest{})
	if err != nil || aborted.Status != "aborted" {
		t.Fatalf("abort checkpoint = %+v, %v", aborted, err)
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
