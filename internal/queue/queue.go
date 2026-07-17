// Package queue defines the storage-independent queue domain.
package queue

import "context"

const (
	StatusTodo           = "todo"
	StatusWIP            = "wip"
	StatusDone           = "done"
	StatusFailed         = "failed"
	StatusResetExhausted = "reset_exhausted"

	ResultApplied        = "applied"
	ResultAlreadyApplied = "already_applied"
	ResultRejected       = "rejected"
)

type Error struct {
	Code      string
	Message   string
	Retryable bool
	Details   map[string]any
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

type Identity struct {
	ProjectID  string
	ShardID    string
	Generation int64
}

type JobSpec struct {
	ID    string
	URL   string
	Type  string
	Via   *string
	Hops  int
	Attrs map[string]any
}

type ClaimedJob struct {
	JobSpec
	AttemptID      string
	LeaseExpiresAt int64
}

type Outcome struct {
	Kind string
	Code *int
	URI  *string
	Meta map[string]any
}

type CompleteItem struct {
	JobID          string
	AttemptID      string
	Outcome        Outcome
	DiscoveredJobs []JobSpec
}

type ExecutionError struct {
	Code    string
	Message string
	Details map[string]any
}

type FailItem struct {
	JobID     string
	AttemptID string
	Retryable bool
	Error     ExecutionError
}

type AttemptRef struct {
	JobID     string
	AttemptID string
}

type ItemResult struct {
	JobID          string
	AttemptID      string
	Status         string
	JobStatus      string
	LeaseExpiresAt *int64
	Error          *Error
}

type EnqueueResult struct {
	Inserted  int
	Duplicate int
}

type RequeueResult struct {
	Requeued       int
	ResetExhausted int
}

type Stats struct {
	Todo           int64 `json:"todo"`
	WIP            int64 `json:"wip"`
	Done           int64 `json:"done"`
	Failed         int64 `json:"failed"`
	ResetExhausted int64 `json:"reset_exhausted"`
}

type Store interface {
	Identity() Identity
	SetFence(ctx context.Context, generation, now, ownerLeaseExpiresAt int64) error
	Enqueue(ctx context.Context, generation, now int64, jobs []JobSpec) (EnqueueResult, error)
	ClaimBatch(ctx context.Context, generation, now int64, sessionID string, acceptTypes []string, limit int, leaseSeconds int64) ([]ClaimedJob, error)
	CompleteBatch(ctx context.Context, generation, now int64, sessionID string, items []CompleteItem) ([]ItemResult, error)
	FailBatch(ctx context.Context, generation, now int64, sessionID string, maxResets int, items []FailItem) ([]ItemResult, error)
	ExtendLeaseBatch(ctx context.Context, generation, now int64, sessionID string, extendSeconds int64, items []AttemptRef) ([]ItemResult, error)
	RequeueExpired(ctx context.Context, generation, now int64, maxResets int, limit int) (RequeueResult, error)
	Stats(ctx context.Context) (Stats, error)
	Close() error
}

// Snapshotter is an optional backend capability. Snapshot must create a new,
// compact, self-contained database at destination without modifying the live
// queue.
type Snapshotter interface {
	Snapshot(ctx context.Context, destination string) error
}
