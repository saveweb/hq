# SavewebHQ Go worker SDK

The Go SDK claims jobs directly from one SavewebHQ project queue, keeps their
leases alive in bounded batches, and gives every claimed job a context that is
canceled when the attempt can no longer be processed safely.

## Open a queue

```go
queue, err := worker.OpenProjectQueue(rootCtx, worker.Config{
	TrackerURL:    "https://hq.saveweb.org",
	MachineToken:  machineToken,
	ClientVersion: "sinavideo/2.5.0",
}, "sinavideo")
if err != nil {
	return err
}
defer queue.Close()
```

Opening a queue generates a fresh seven-character `a-z0-9` worker ID. It stays
fixed for that queue instance and is available through `queue.WorkerID()`.

To identify the user behind a machine token without opening a project queue:

```go
userID, err := worker.WhoAmI(ctx, config)
```

`rootCtx` owns the queue lifetime. Canceling it or calling `Close` stops lease
renewal and cancels every held job. The context passed to `Claim` controls only
that claim request; canceling it after `Claim` returns does not cancel the jobs
that were returned.

`Claim` retries network failures, HTTP 5xx responses, and retryable rate limits
with bounded exponential backoff. Server-provided retry delays take precedence.
Permanent API errors such as invalid credentials or an unsupported client
version are returned immediately.

## Claim and process jobs

```go
batch, err := queue.Claim(claimCtx, worker.ClaimOptions{
	MaxJobs:     64,
	AcceptTypes: []string{protocol.JobTypeSeed},
})
if err != nil {
	return err
}

for _, job := range batch.Jobs {
	go processJob(job)
}
```

With no lease override, `Claim` uses the project's recommended lease from the
Tracker policy. The SDK renews held attempts every quarter of that duration.
Renewals use the existing batch endpoint and are split into groups of at most
256 attempts.

Set `ClaimOptions.Lease` only when a worker has a specific reason to override
the project recommendation. It must be a whole number of seconds from one
second through one hour.

## Finish a job

```go
func processJob(job *worker.Job) {
	outcome, receipts, err := archive(job.Context(), job.Spec)

	// job.Context() may already be canceled when the lease is lost. Use a
	// separate bounded context for the final Tracker request.
	finishCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err != nil {
		_ = job.Fail(finishCtx, worker.Failure{
			Retryable: true,
			Error: protocol.ExecutionError{
				Code:    "archive_failed",
				Message: err.Error(),
				Details: protocol.Attrs{},
			},
		})
		return
	}

	_ = job.Complete(finishCtx, outcome, receipts...)
}
```

`Complete` and `Fail` already know the job ID, attempt ID, project, and worker.
An uncertain transport error leaves the job held so the caller can retry while
the SDK continues renewing it. An applied result ends the handle. A stale
attempt ends it with `worker.ErrLeaseLost`.

## Context causes

Processing code should stop promptly when `job.Context()` is canceled:

```go
switch {
case errors.Is(context.Cause(job.Context()), worker.ErrLeaseLost):
	// Renewal could not preserve the lease, or Tracker rejected the attempt.
case errors.Is(context.Cause(job.Context()), worker.ErrQueueClosed):
	// The queue was closed explicitly.
case context.Cause(job.Context()) != nil:
	// The queue's root context was canceled.
}
```

After a successful `Complete` or `Fail`, the context ends with
`worker.ErrJobFinished`. This terminal cause is expected and does not indicate
that the outcome was lost.

The local lease deadline is conservative and uses Go's monotonic clock. Failed
renewal requests do not cancel a job immediately; the SDK retries on the next
quarter-lease interval and cancels only when the last known lease reaches its
deadline.
