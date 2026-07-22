package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"git.saveweb.org/saveweb/hq/internal/trackerclient"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

// ProjectQueue is the direct client for the single-site PostgreSQL queue. It
// has no route discovery, shard lease, or background heartbeat.
type ProjectQueue struct {
	projectID string
	workerID  string
	client    *trackerclient.Client

	mu              sync.Mutex
	policy          protocol.ProjectPolicy
	policyFetchedAt time.Time
	nextClaimAt     time.Time
}

func OpenProjectQueue(config Config, projectID string) (*ProjectQueue, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	if projectID == "" {
		return nil, fmt.Errorf("worker: project ID is required")
	}
	client, err := trackerFor(config)
	if err != nil {
		return nil, err
	}
	return &ProjectQueue{projectID: projectID, workerID: config.WorkerID, client: client}, nil
}

func (q *ProjectQueue) Claim(ctx context.Context, maxJobs int, leaseSeconds int64, acceptTypes []string) (protocol.ProjectClaimResponse, error) {
	if maxJobs < 1 {
		return protocol.ProjectClaimResponse{}, fmt.Errorf("worker: max jobs must be positive")
	}
	for {
		policy, err := q.currentPolicy(ctx)
		if err != nil {
			return protocol.ProjectClaimResponse{}, err
		}
		if err := q.waitForClaim(ctx, policy.WorkerClaimQPS); err != nil {
			return protocol.ProjectClaimResponse{}, err
		}
		result, err := q.client.ClaimProjectJobs(ctx, q.projectID, protocol.ProjectClaimRequest{
			WorkerID: q.workerID, MaxJobs: min(maxJobs, policy.MaxJobsPerClaim), LeaseSeconds: leaseSeconds,
			AcceptTypes: append([]string(nil), acceptTypes...), PolicyVersion: policy.PolicyVersion,
		})
		if err != nil {
			var apiError *trackerclient.Error
			if errors.As(err, &apiError) && apiError.Status == 429 && apiError.API.Retryable && apiError.API.RetryAfterMS > 0 {
				if err := waitContext(ctx, jittered(millisecondsDuration(apiError.API.RetryAfterMS))); err != nil {
					return protocol.ProjectClaimResponse{}, err
				}
				continue
			}
			return protocol.ProjectClaimResponse{}, convertTrackerError(err)
		}
		if result.ProjectID != q.projectID {
			return protocol.ProjectClaimResponse{}, fmt.Errorf("worker: tracker returned a mismatched project")
		}
		if result.PolicyVersion != policy.PolicyVersion {
			q.invalidatePolicy()
		}
		return result, nil
	}
}

func (q *ProjectQueue) currentPolicy(ctx context.Context) (protocol.ProjectPolicy, error) {
	q.mu.Lock()
	policy := q.policy
	fetchedAt := q.policyFetchedAt
	q.mu.Unlock()
	refreshAfter := time.Duration(policy.RefreshAfterMS) * time.Millisecond
	if policy.ProjectID != "" && refreshAfter > 0 && time.Since(fetchedAt) < refreshAfter {
		return policy, nil
	}
	policy, err := q.client.ProjectPolicy(ctx, q.projectID)
	if err != nil {
		return protocol.ProjectPolicy{}, convertTrackerError(err)
	}
	if policy.ProjectID != q.projectID || policy.MaxJobsPerClaim < 1 || policy.MaxJobsPerClaim > 256 || policy.PolicyVersion < 1 || policy.RefreshAfterMS < 1 || !validQPS(policy.WorkerClaimQPS) || !validQPS(policy.DispatchQPS) {
		return protocol.ProjectPolicy{}, fmt.Errorf("worker: tracker returned an invalid project policy")
	}
	q.mu.Lock()
	if q.policy.PolicyVersion != 0 && q.policy.PolicyVersion != policy.PolicyVersion {
		q.nextClaimAt = time.Time{}
	}
	q.policy, q.policyFetchedAt = policy, time.Now()
	q.mu.Unlock()
	return policy, nil
}

func (q *ProjectQueue) invalidatePolicy() {
	q.mu.Lock()
	q.policyFetchedAt = time.Time{}
	q.mu.Unlock()
}

func (q *ProjectQueue) waitForClaim(ctx context.Context, qps *float64) error {
	if qps == nil {
		return nil
	}
	interval := qpsInterval(*qps)
	q.mu.Lock()
	now := time.Now()
	scheduled := now
	if q.nextClaimAt.IsZero() {
		scheduled = scheduled.Add(initialJitter(interval))
	} else {
		if q.nextClaimAt.After(scheduled) {
			scheduled = q.nextClaimAt
		}
		scheduled = scheduled.Add(jitter(interval))
	}
	q.nextClaimAt = scheduled.Add(interval)
	q.mu.Unlock()
	return waitContext(ctx, time.Until(scheduled))
}

func validQPS(value *float64) bool {
	return value == nil || (*value > 0 && !math.IsNaN(*value) && !math.IsInf(*value, 0))
}

func qpsInterval(qps float64) time.Duration {
	nanoseconds := float64(time.Second) / qps
	if nanoseconds < 1 {
		return time.Nanosecond
	}
	if nanoseconds >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(math.Ceil(nanoseconds))
}

func jitter(interval time.Duration) time.Duration {
	maximum := interval / 10
	if maximum < 1 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(maximum) + 1))
}

func initialJitter(interval time.Duration) time.Duration {
	if interval < 2 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(interval)))
}

func jittered(delay time.Duration) time.Duration {
	if delay > time.Duration(math.MaxInt64)-delay/10 {
		return time.Duration(math.MaxInt64)
	}
	return delay + jitter(delay)
}

func millisecondsDuration(milliseconds int64) time.Duration {
	if milliseconds <= 0 {
		return 0
	}
	if milliseconds > math.MaxInt64/int64(time.Millisecond) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func waitContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *ProjectQueue) Complete(ctx context.Context, items []protocol.ProjectCompleteItem) (protocol.BatchResultResponse, error) {
	result, err := q.client.CompleteProjectJobs(ctx, q.projectID, protocol.ProjectCompleteRequest{WorkerID: q.workerID, Items: items})
	return result, convertTrackerError(err)
}

func (q *ProjectQueue) Fail(ctx context.Context, items []protocol.FailItem) (protocol.BatchResultResponse, error) {
	result, err := q.client.FailProjectJobs(ctx, q.projectID, protocol.ProjectFailRequest{WorkerID: q.workerID, Items: items})
	return result, convertTrackerError(err)
}

func (q *ProjectQueue) ExtendLease(ctx context.Context, extendSeconds int64, items []protocol.AttemptRef) (protocol.BatchResultResponse, error) {
	result, err := q.client.ExtendProjectJobLeases(ctx, q.projectID, protocol.ProjectExtendLeaseRequest{WorkerID: q.workerID, ExtendSeconds: extendSeconds, Items: items})
	return result, convertTrackerError(err)
}
