package worker

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func TestPinnedTLSQueueClient(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer access-token" ||
			request.Header.Get("Cache-Control") == "" {
			t.Errorf("headers = %+v", request.Header)
		}
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(response).Encode(protocol.ClaimResponse{
			Route: protocol.Route{ProjectID: "p", ShardID: "s", Generation: 1}, Jobs: []protocol.ClaimedJob{},
		})
	}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	digest := sha256.Sum256(server.Certificate().RawSubjectPublicKeyInfo)
	pin := base64.RawURLEncoding.EncodeToString(digest[:])
	client, err := newRouteClient(protocol.Assignment{
		Route:        protocol.Route{ProjectID: "p", ShardID: "s", Generation: 1},
		OwnerAgentID: "owner", Endpoint: server.URL, TLSSPKISHA256: &pin,
		AccessToken: "access-token", AccessTokenExpires: time.Now().Unix() + 60,
	}, Config{RequestTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var response protocol.ClaimResponse
	err = client.do(context.Background(), "/api/v1/queue/claim", protocol.ClaimRequest{}, &response)
	if err != nil || response.Generation != 1 {
		t.Fatalf("response = %+v, %v", response, err)
	}

	wrong := sha256.Sum256([]byte("wrong"))
	wrongPin := base64.RawURLEncoding.EncodeToString(wrong[:])
	badClient, err := newRouteClient(protocol.Assignment{
		Route:        protocol.Route{ProjectID: "p", ShardID: "s", Generation: 1},
		OwnerAgentID: "owner", Endpoint: server.URL, TLSSPKISHA256: &wrongPin,
		AccessToken: "access-token", AccessTokenExpires: time.Now().Unix() + 60,
	}, Config{RequestTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer badClient.Close()
	if err := badClient.do(context.Background(), "/api/v1/queue/claim", protocol.ClaimRequest{}, &response); err == nil {
		t.Fatal("queue client accepted the wrong SPKI pin")
	}
}

func TestQueueClientRejectsCachedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Cache-Control", "no-store")
		response.Header().Set("CF-Cache-Status", "HIT")
		_ = json.NewEncoder(response).Encode(protocol.ClaimResponse{})
	}))
	defer server.Close()
	client, err := newRouteClient(protocol.Assignment{
		Route:        protocol.Route{ProjectID: "p", ShardID: "s", Generation: 1},
		OwnerAgentID: "owner", Endpoint: server.URL,
		AccessToken: "access-token", AccessTokenExpires: time.Now().Unix() + 60,
	}, Config{AllowHTTPShard: true, RequestTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var response protocol.ClaimResponse
	err = client.do(context.Background(), "/api/v1/queue/claim", protocol.ClaimRequest{}, &response)
	if !IsCode(err, protocol.ErrorCacheMisconfigured) {
		t.Fatalf("cache error = %v", err)
	}
}
