package worker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

var (
	ErrLeaseLost   = errors.New("worker: job lease lost")
	ErrQueueClosed = errors.New("worker: project queue closed")
	ErrJobFinished = errors.New("worker: job finished")
)

type Failure struct {
	Retryable bool
	Error     protocol.ExecutionError
}

type Job struct {
	JobID int64
	Spec  protocol.JobSpecV1

	queue     *ProjectQueue
	attemptID string
	lease     time.Duration
	ctx       context.Context
	cancel    context.CancelCauseFunc
	finishMu  sync.Mutex
	deadline  time.Time
	renewAt   time.Time
}

func (j *Job) Context() context.Context { return j.ctx }

func (j *Job) Complete(ctx context.Context, outcome protocol.Outcome, receipts ...protocol.WARCReceipt) error {
	j.finishMu.Lock()
	defer j.finishMu.Unlock()
	if cause := context.Cause(j.ctx); cause != nil {
		return cause
	}
	ctx, cancel := j.queue.requestContext(ctx)
	defer cancel()
	result, err := j.queue.client.CompleteProjectJobs(ctx, j.queue.projectID, protocol.ProjectCompleteRequest{
		WorkerID: j.queue.workerID,
		Items:    []protocol.ProjectCompleteItem{{JobID: j.JobID, AttemptID: j.attemptID, Outcome: outcome, WARCReceipts: append([]protocol.WARCReceipt(nil), receipts...)}},
	})
	if err != nil {
		return convertTrackerError(err)
	}
	return j.finishResult(result)
}

func (j *Job) Fail(ctx context.Context, failure Failure) error {
	j.finishMu.Lock()
	defer j.finishMu.Unlock()
	if cause := context.Cause(j.ctx); cause != nil {
		return cause
	}
	ctx, cancel := j.queue.requestContext(ctx)
	defer cancel()
	result, err := j.queue.client.FailProjectJobs(ctx, j.queue.projectID, protocol.ProjectFailRequest{
		WorkerID: j.queue.workerID,
		Items:    []protocol.FailItem{{JobID: j.JobID, AttemptID: j.attemptID, Retryable: failure.Retryable, Error: failure.Error}},
	})
	if err != nil {
		return convertTrackerError(err)
	}
	return j.finishResult(result)
}

func (j *Job) finishResult(result protocol.BatchResultResponse) error {
	if len(result.Results) != 1 || result.Results[0].JobID != j.JobID || result.Results[0].AttemptID != j.attemptID {
		return fmt.Errorf("worker: tracker returned a mismatched job result")
	}
	if result.Results[0].Status == protocol.ItemStatusApplied {
		j.queue.releaseJob(j, ErrJobFinished)
		return nil
	}
	j.queue.releaseJob(j, ErrLeaseLost)
	return ErrLeaseLost
}

func (q *ProjectQueue) Close() {
	q.closeOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		jobs := make([]*Job, 0, len(q.jobs))
		for _, job := range q.jobs {
			jobs = append(jobs, job)
		}
		q.jobs = make(map[string]*Job)
		q.cancelQueue(ErrQueueClosed)
		close(q.done)
		q.mu.Unlock()
		for _, job := range jobs {
			job.cancel(ErrQueueClosed)
		}
		<-q.stopped
	})
}

func (q *ProjectQueue) holdClaimedJobs(claimed []protocol.ClaimedJob, lease time.Duration, started time.Time) ([]*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrQueueClosed
	}
	for _, claimedJob := range claimed {
		if claimedJob.JobID < 1 || claimedJob.AttemptID == "" {
			return nil, fmt.Errorf("worker: tracker returned an invalid claimed job")
		}
	}
	jobs := make([]*Job, 0, len(claimed))
	for _, claimedJob := range claimed {
		jobCtx, cancel := context.WithCancelCause(q.queueCtx)
		job := &Job{
			JobID: claimedJob.JobID, Spec: claimedJob.JobSpecV1, queue: q,
			attemptID: claimedJob.AttemptID, lease: lease, ctx: jobCtx, cancel: cancel,
			deadline: started.Add(lease), renewAt: started.Add(lease / 4),
		}
		q.jobs[job.attemptID] = job
		jobs = append(jobs, job)
	}
	q.signalRenewal()
	return jobs, nil
}

func (q *ProjectQueue) releaseJob(job *Job, cause error) {
	q.mu.Lock()
	if q.jobs[job.attemptID] == job {
		delete(q.jobs, job.attemptID)
	}
	q.mu.Unlock()
	job.cancel(cause)
	q.signalRenewal()
}

func (q *ProjectQueue) signalRenewal() {
	select {
	case q.wakeRenewal <- struct{}{}:
	default:
	}
}

func (q *ProjectQueue) renewLeases() {
	defer close(q.stopped)
	for {
		delay, due, expired := q.renewalWork(time.Now())
		for _, job := range expired {
			q.releaseJob(job, ErrLeaseLost)
		}
		if len(due) > 0 {
			q.renewJobs(due)
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-q.wakeRenewal:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-q.done:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}
	}
}

func (q *ProjectQueue) renewalWork(now time.Time) (time.Duration, map[time.Duration][]*Job, []*Job) {
	q.mu.Lock()
	defer q.mu.Unlock()
	due := make(map[time.Duration][]*Job)
	expired := []*Job{}
	next := now.Add(time.Hour)
	for _, job := range q.jobs {
		if !job.deadline.After(now) {
			expired = append(expired, job)
			continue
		}
		if !job.renewAt.After(now) {
			due[job.lease] = append(due[job.lease], job)
			job.renewAt = now.Add(job.lease / 4)
		}
		if job.renewAt.Before(next) {
			next = job.renewAt
		}
		if job.deadline.Before(next) {
			next = job.deadline
		}
	}
	if len(q.jobs) == 0 {
		return time.Hour, due, expired
	}
	return max(time.Until(next), time.Millisecond), due, expired
}

func (q *ProjectQueue) renewJobs(groups map[time.Duration][]*Job) {
	leases := make([]time.Duration, 0, len(groups))
	for lease := range groups {
		leases = append(leases, lease)
		sort.Slice(groups[lease], func(i, j int) bool {
			return groups[lease][i].deadline.Before(groups[lease][j].deadline)
		})
	}
	sort.Slice(leases, func(i, j int) bool {
		return groups[leases[i]][0].deadline.Before(groups[leases[j]][0].deadline)
	})
	for _, lease := range leases {
		jobs := groups[lease]
		for start := 0; start < len(jobs); start += 256 {
			end := min(start+256, len(jobs))
			q.renewJobBatch(lease, jobs[start:end])
		}
	}
}

func (q *ProjectQueue) renewJobBatch(lease time.Duration, jobs []*Job) {
	items := make([]protocol.AttemptRef, 0, len(jobs))
	for _, job := range jobs {
		items = append(items, protocol.AttemptRef{JobID: job.JobID, AttemptID: job.attemptID})
	}
	started := time.Now()
	deadline := jobs[0].deadline
	for _, job := range jobs[1:] {
		if job.deadline.Before(deadline) {
			deadline = job.deadline
		}
	}
	ctx, cancel := context.WithDeadline(q.queueCtx, deadline)
	defer cancel()
	result, err := q.client.ExtendProjectJobLeases(ctx, q.projectID, protocol.ProjectExtendLeaseRequest{
		WorkerID: q.workerID, ExtendSeconds: int64(lease / time.Second), Items: items,
	})
	if err != nil {
		return
	}
	byAttempt := make(map[string]protocol.ItemResult, len(result.Results))
	for _, item := range result.Results {
		byAttempt[item.AttemptID] = item
	}
	for _, job := range jobs {
		item, ok := byAttempt[job.attemptID]
		if !ok || item.JobID != job.JobID {
			continue
		}
		if item.Status != protocol.ItemStatusApplied {
			q.releaseJob(job, ErrLeaseLost)
			continue
		}
		q.mu.Lock()
		if q.jobs[job.attemptID] == job {
			job.deadline = started.Add(lease)
		}
		q.mu.Unlock()
	}
}
