package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
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
				ProjectID: "demo", MaxJobsPerClaim: 2, RecommendedLeaseSeconds: 30, PolicyVersion: 4, RefreshAfterMS: 60_000,
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

	queue, err := OpenProjectQueue(context.Background(), Config{
		TrackerURL: server.URL, MachineToken: "token", WorkerID: "worker-1",
		ClientVersion:    "worker-v2",
		AllowHTTPTracker: true, RequestTimeout: time.Second,
	}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	result, err := queue.Claim(context.Background(), ClaimOptions{MaxJobs: 10})
	if err != nil || len(result.Jobs) != 0 || policyCalls != 1 || claimCalls != 2 {
		t.Fatalf("result=%+v error=%v policy_calls=%d claim_calls=%d", result, err, policyCalls, claimCalls)
	}
}

func TestJobAutoRenewsAndCompletes(t *testing.T) {
	var renewCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Cache-Control", "no-store")
		switch request.URL.Path {
		case "/api/v1/projects/demo":
			_ = json.NewEncoder(response).Encode(protocol.ProjectPolicy{ProjectID: "demo", MaxJobsPerClaim: 1, RecommendedLeaseSeconds: 1, PolicyVersion: 1, RefreshAfterMS: 60_000})
		case "/api/v1/projects/demo/jobs/claim":
			_ = json.NewEncoder(response).Encode(protocol.ProjectClaimResponse{ProjectID: "demo", Jobs: []protocol.ClaimedJob{{JobSpecV1: protocol.JobSpecV1{Value: "https://example.test"}, JobID: 7, AttemptID: "at_1", LeaseExpiresAt: time.Now().Unix() + 1}}, RetryAfterMS: 1000, PolicyVersion: 1})
		case "/api/v1/projects/demo/jobs/extend-lease":
			renewCalls.Add(1)
			_ = json.NewEncoder(response).Encode(protocol.BatchResultResponse{Results: []protocol.ItemResult{{JobID: 7, AttemptID: "at_1", Status: protocol.ItemStatusApplied}}})
		case "/api/v1/projects/demo/jobs/complete":
			_ = json.NewEncoder(response).Encode(protocol.BatchResultResponse{Results: []protocol.ItemResult{{JobID: 7, AttemptID: "at_1", Status: protocol.ItemStatusApplied}}})
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	queue, err := OpenProjectQueue(context.Background(), Config{TrackerURL: server.URL, MachineToken: "token", WorkerID: "worker-1", ClientVersion: "worker-v2", AllowHTTPTracker: true, RequestTimeout: time.Second}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	batch, err := queue.Claim(context.Background(), ClaimOptions{MaxJobs: 1})
	if err != nil || len(batch.Jobs) != 1 {
		t.Fatalf("claim = %+v, %v", batch, err)
	}
	job := batch.Jobs[0]
	deadline := time.Now().Add(2 * time.Second)
	for renewCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if renewCalls.Load() == 0 || job.Context().Err() != nil {
		t.Fatalf("renew calls = %d, cause = %v", renewCalls.Load(), context.Cause(job.Context()))
	}
	if err := job.Complete(context.Background(), protocol.Outcome{Kind: "success", Meta: protocol.Attrs{}}); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(context.Cause(job.Context()), ErrJobFinished) {
		t.Fatalf("job cause = %v", context.Cause(job.Context()))
	}
}

func TestJobContextCanceledWhenRenewalCannotReachTracker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Cache-Control", "no-store")
		switch request.URL.Path {
		case "/api/v1/projects/demo":
			_ = json.NewEncoder(response).Encode(protocol.ProjectPolicy{ProjectID: "demo", MaxJobsPerClaim: 1, RecommendedLeaseSeconds: 1, PolicyVersion: 1, RefreshAfterMS: 60_000})
		case "/api/v1/projects/demo/jobs/claim":
			_ = json.NewEncoder(response).Encode(protocol.ProjectClaimResponse{ProjectID: "demo", Jobs: []protocol.ClaimedJob{{JobSpecV1: protocol.JobSpecV1{Value: "https://example.test"}, JobID: 8, AttemptID: "at_2", LeaseExpiresAt: time.Now().Unix() + 1}}, RetryAfterMS: 1000, PolicyVersion: 1})
		case "/api/v1/projects/demo/jobs/extend-lease":
			response.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(response).Encode(protocol.ErrorEnvelope{Error: protocol.APIError{Code: protocol.ErrorInternal, Message: "unavailable", Retryable: true, Details: protocol.Attrs{}}})
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	queue, err := OpenProjectQueue(context.Background(), Config{TrackerURL: server.URL, MachineToken: "token", WorkerID: "worker-1", ClientVersion: "worker-v2", AllowHTTPTracker: true, RequestTimeout: time.Second}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	batch, err := queue.Claim(context.Background(), ClaimOptions{MaxJobs: 1})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-batch.Jobs[0].Context().Done():
		if !errors.Is(context.Cause(batch.Jobs[0].Context()), ErrLeaseLost) {
			t.Fatalf("job cause = %v", context.Cause(batch.Jobs[0].Context()))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("job context remained active after its lease deadline")
	}
}

func TestRenewalSplitsBatchesAtProtocolLimit(t *testing.T) {
	var claimCalls atomic.Int64
	var mu sync.Mutex
	renewed := 0
	batchSizes := []int{}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Cache-Control", "no-store")
		switch request.URL.Path {
		case "/api/v1/projects/demo":
			_ = json.NewEncoder(response).Encode(protocol.ProjectPolicy{ProjectID: "demo", MaxJobsPerClaim: 256, RecommendedLeaseSeconds: 1, PolicyVersion: 1, RefreshAfterMS: 60_000})
		case "/api/v1/projects/demo/jobs/claim":
			call := claimCalls.Add(1)
			count, firstID := 256, int64(1)
			if call == 2 {
				count, firstID = 1, 257
			}
			jobs := make([]protocol.ClaimedJob, count)
			for index := range jobs {
				jobID := firstID + int64(index)
				jobs[index] = protocol.ClaimedJob{JobSpecV1: protocol.JobSpecV1{Value: "work"}, JobID: jobID, AttemptID: fmt.Sprintf("at_%d", jobID), LeaseExpiresAt: time.Now().Unix() + 1}
			}
			_ = json.NewEncoder(response).Encode(protocol.ProjectClaimResponse{ProjectID: "demo", Jobs: jobs, RetryAfterMS: 1000, PolicyVersion: 1})
		case "/api/v1/projects/demo/jobs/extend-lease":
			var input protocol.ProjectExtendLeaseRequest
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Error(err)
				return
			}
			results := make([]protocol.ItemResult, len(input.Items))
			for index, item := range input.Items {
				results[index] = protocol.ItemResult{JobID: item.JobID, AttemptID: item.AttemptID, Status: protocol.ItemStatusApplied}
			}
			mu.Lock()
			batchSizes = append(batchSizes, len(input.Items))
			renewed += len(input.Items)
			mu.Unlock()
			_ = json.NewEncoder(response).Encode(protocol.BatchResultResponse{Results: results})
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	queue, err := OpenProjectQueue(context.Background(), Config{TrackerURL: server.URL, MachineToken: "token", WorkerID: "worker-1", ClientVersion: "worker-v2", AllowHTTPTracker: true, RequestTimeout: time.Second}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	if batch, err := queue.Claim(context.Background(), ClaimOptions{MaxJobs: 256}); err != nil || len(batch.Jobs) != 256 {
		t.Fatalf("first claim jobs = %d, %v", len(batch.Jobs), err)
	}
	if batch, err := queue.Claim(context.Background(), ClaimOptions{MaxJobs: 1}); err != nil || len(batch.Jobs) != 1 {
		t.Fatalf("second claim jobs = %d, %v", len(batch.Jobs), err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := renewed >= 257
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if renewed < 257 || len(batchSizes) < 2 {
		t.Fatalf("renewed = %d, batches = %v", renewed, batchSizes)
	}
	for _, size := range batchSizes {
		if size < 1 || size > 256 {
			t.Fatalf("renewal batch size = %d", size)
		}
	}
}

func TestQueueRootContextCancelsHeldJobs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Cache-Control", "no-store")
		switch request.URL.Path {
		case "/api/v1/projects/demo":
			_ = json.NewEncoder(response).Encode(protocol.ProjectPolicy{ProjectID: "demo", MaxJobsPerClaim: 1, RecommendedLeaseSeconds: 30, PolicyVersion: 1, RefreshAfterMS: 60_000})
		case "/api/v1/projects/demo/jobs/claim":
			_ = json.NewEncoder(response).Encode(protocol.ProjectClaimResponse{ProjectID: "demo", Jobs: []protocol.ClaimedJob{{JobSpecV1: protocol.JobSpecV1{Value: "work"}, JobID: 9, AttemptID: "at_9", LeaseExpiresAt: time.Now().Unix() + 30}}, RetryAfterMS: 1000, PolicyVersion: 1})
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	rootCtx, cancelRoot := context.WithCancel(context.Background())
	queue, err := OpenProjectQueue(rootCtx, Config{TrackerURL: server.URL, MachineToken: "token", WorkerID: "worker-1", ClientVersion: "worker-v2", AllowHTTPTracker: true, RequestTimeout: time.Second}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	batch, err := queue.Claim(context.Background(), ClaimOptions{MaxJobs: 1})
	if err != nil {
		t.Fatal(err)
	}
	cancelRoot()
	select {
	case <-batch.Jobs[0].Context().Done():
		if !errors.Is(context.Cause(batch.Jobs[0].Context()), context.Canceled) {
			t.Fatalf("job cause = %v", context.Cause(batch.Jobs[0].Context()))
		}
	case <-time.After(time.Second):
		t.Fatal("root context did not cancel held job")
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
