package worker

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func TestProjectQueueAppliesPolicyAndRetriesRateLimit(t *testing.T) {
	var policyCalls, claimCalls int
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Cache-Control", "no-store")
		switch request.URL.Path {
		case "/api/v1/projects/demo":
			policyCalls++
			_ = json.NewEncoder(response).Encode(protocol.ProjectPolicy{
				ProjectID: "demo", MaxJobsPerClaim: 2, PolicyVersion: 4, RefreshAfterMS: 60_000,
			})
		case "/api/v1/projects/demo/jobs/claim":
			claimCalls++
			var input protocol.ProjectClaimRequest
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Fatal(err)
			}
			if input.MaxJobs != 2 || input.PolicyVersion != 4 {
				t.Fatalf("claim input = %+v", input)
			}
			if claimCalls == 1 {
				response.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(response).Encode(protocol.ErrorEnvelope{Error: protocol.APIError{
					Code: protocol.ErrorProjectRateLimited, Message: "rate limited", Retryable: true, RetryAfterMS: 1, Details: protocol.Attrs{},
				}})
				return
			}
			_ = json.NewEncoder(response).Encode(protocol.ProjectClaimResponse{
				ProjectID: "demo", Jobs: []protocol.ClaimedJob{}, RetryAfterMS: 1000, PolicyVersion: 4,
			})
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	queue, err := OpenProjectQueue(Config{
		TrackerURL: server.URL, MachineToken: "token", WorkerID: "worker-1",
		AllowHTTPTracker: true, RequestTimeout: time.Second,
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	result, err := queue.Claim(context.Background(), 10, 30, nil)
	if err != nil || result.ProjectID != "demo" || policyCalls != 1 || claimCalls != 2 {
		t.Fatalf("result=%+v error=%v policy_calls=%d claim_calls=%d", result, err, policyCalls, claimCalls)
	}
}

func TestQPSIntervalSaturatesOnlyAtClockResolution(t *testing.T) {
	if got := qpsInterval(1e12); got != time.Nanosecond {
		t.Fatalf("high QPS interval = %v", got)
	}
	if got := qpsInterval(math.SmallestNonzeroFloat64); got != time.Duration(math.MaxInt64) {
		t.Fatalf("low QPS interval = %v", got)
	}
	if got := millisecondsDuration(math.MaxInt64); got != time.Duration(math.MaxInt64) {
		t.Fatalf("large retry delay = %v", got)
	}
}
