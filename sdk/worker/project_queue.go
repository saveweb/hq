package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/saveweb/hq/internal/trackerclient"
	"github.com/saveweb/hq/pkg/protocol"
)

// ProjectQueue is the direct client for the single-site PostgreSQL queue.
type ProjectQueue struct {
	projectID string
	workerID  string
	client    *trackerclient.Client

	mu              sync.Mutex
	policy          protocol.ProjectPolicy
	policyFetchedAt time.Time
	nextClaimAt     time.Time
	jobs            map[string]*Job
	wakeRenewal     chan struct{}
	done            chan struct{}
	stopped         chan struct{}
	queueCtx        context.Context
	cancelQueue     context.CancelCauseFunc
	closeOnce       sync.Once
	closed          bool
}

type ClaimOptions struct {
	MaxJobs     int
	AcceptTypes []string
	Lease       time.Duration
}

type ClaimBatch struct {
	Jobs       []*Job
	RetryAfter time.Duration
}

func OpenProjectQueue(ctx context.Context, config Config, projectID string) (*ProjectQueue, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
	queueCtx, cancelQueue := context.WithCancelCause(ctx)
	q := &ProjectQueue{
		projectID: projectID, workerID: config.WorkerID, client: client,
		jobs: make(map[string]*Job), wakeRenewal: make(chan struct{}, 1), done: make(chan struct{}), stopped: make(chan struct{}),
		queueCtx: queueCtx, cancelQueue: cancelQueue,
	}
	go q.renewLeases()
	go func() {
		select {
		case <-ctx.Done():
			q.Close()
		case <-q.done:
		}
	}()
	return q, nil
}

func (q *ProjectQueue) Claim(ctx context.Context, options ClaimOptions) (ClaimBatch, error) {
	if options.MaxJobs < 1 {
		return ClaimBatch{}, fmt.Errorf("worker: max jobs must be positive")
	}
	ctx, cancel := q.requestContext(ctx)
	defer cancel()
	for {
		policy, err := q.currentPolicy(ctx)
		if err != nil {
			return ClaimBatch{}, err
		}
		leaseSeconds, err := claimLeaseSeconds(options.Lease, policy.RecommendedLeaseSeconds)
		if err != nil {
			return ClaimBatch{}, err
		}
		if err := q.waitForClaim(ctx, policy.WorkerClaimQPS); err != nil {
			return ClaimBatch{}, err
		}
		q.mu.Lock()
		closed := q.closed
		q.mu.Unlock()
		if closed {
			return ClaimBatch{}, ErrQueueClosed
		}
		requestStarted := time.Now()
		result, err := q.client.ClaimProjectJobs(ctx, q.projectID, protocol.ProjectClaimRequest{
			WorkerID: q.workerID, MaxJobs: min(options.MaxJobs, policy.MaxJobsPerClaim), LeaseSeconds: leaseSeconds,
			AcceptTypes: append([]string(nil), options.AcceptTypes...), PolicyVersion: policy.PolicyVersion,
		})
		if err != nil {
			var apiError *trackerclient.Error
			if errors.As(err, &apiError) && apiError.Status == 429 && apiError.API.Retryable && apiError.API.RetryAfterMS > 0 {
				if err := waitContext(ctx, jittered(millisecondsDuration(apiError.API.RetryAfterMS))); err != nil {
					return ClaimBatch{}, err
				}
				continue
			}
			return ClaimBatch{}, convertTrackerError(err)
		}
		if result.ProjectID != q.projectID {
			return ClaimBatch{}, fmt.Errorf("worker: tracker returned a mismatched project")
		}
		if result.PolicyVersion != policy.PolicyVersion {
			q.invalidatePolicy()
		}
		jobs, err := q.holdClaimedJobs(result.Jobs, time.Duration(leaseSeconds)*time.Second, requestStarted)
		if err != nil {
			return ClaimBatch{}, err
		}
		return ClaimBatch{Jobs: jobs, RetryAfter: millisecondsDuration(result.RetryAfterMS)}, nil
	}
}

func (q *ProjectQueue) requestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	requestCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(q.queueCtx, cancel)
	return requestCtx, func() {
		stop()
		cancel()
	}
}

func claimLeaseSeconds(override time.Duration, recommended int64) (int64, error) {
	if override == 0 {
		return recommended, nil
	}
	if override%time.Second != 0 || override < time.Second || override > time.Hour {
		return 0, fmt.Errorf("worker: lease must be a whole number of seconds between 1s and 1h")
	}
	return int64(override / time.Second), nil
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
	if policy.ProjectID != q.projectID || policy.MaxJobsPerClaim < 1 || policy.MaxJobsPerClaim > 256 || policy.RecommendedLeaseSeconds < 1 || policy.RecommendedLeaseSeconds > 3600 || policy.PolicyVersion < 1 || policy.RefreshAfterMS < 1 || !validQPS(policy.WorkerClaimQPS) || !validQPS(policy.DispatchQPS) {
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
