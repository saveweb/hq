// Package protocol contains the Project Queue HTTP types.
package protocol

type Attrs map[string]any

const (
	ItemStatusApplied        = "applied"
	ItemStatusRejected       = "rejected"
	JobStatusTodo            = "todo"
	JobStatusWIP             = "wip"
	JobStatusDone            = "done"
	JobStatusFailed          = "failed"
	JobStatusResetExhausted  = "reset_exhausted"
	ErrorInternal            = "internal_error"
	ErrorInvalidRequest      = "invalid_request"
	ErrorInvalidJob          = "invalid_job"
	ErrorInvalidMachineToken = "invalid_machine_token"
	ErrorPermissionDenied    = "permission_denied"
	ErrorNotFound            = "not_found"
	ErrorIdentityConflict    = "identity_conflict"
	ErrorStaleAttempt        = "stale_attempt"
	ErrorProjectNotActive    = "project_not_active"
)

type APIError struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	Retryable    bool   `json:"retryable"`
	RetryAfterMS int64  `json:"retry_after_ms"`
	Details      Attrs  `json:"details"`
}
type ErrorEnvelope struct {
	Error APIError `json:"error"`
}
type ClaimedJob struct {
	JobSpecV1
	JobID          int64  `json:"job_id"`
	AttemptID      string `json:"attempt_id"`
	LeaseExpiresAt int64  `json:"lease_expires_at"`
}
type Outcome struct {
	Kind string  `json:"kind"`
	Code *int    `json:"code"`
	URI  *string `json:"uri"`
	Meta Attrs   `json:"meta"`
}
type WARCReceipt struct {
	ID         string `json:"id"`
	Issuer     string `json:"issuer"`
	ObjectID   string `json:"object_id"`
	SHA256     string `json:"sha256"`
	SizeBytes  int64  `json:"size_bytes"`
	AcceptedAt int64  `json:"accepted_at"`
	Signature  string `json:"signature"`
}
type ProjectClaimRequest struct {
	WorkerID     string   `json:"worker_id"`
	MaxJobs      int      `json:"max_jobs"`
	LeaseSeconds int64    `json:"lease_seconds"`
	AcceptTypes  []string `json:"accept_types"`
}
type ProjectClaimResponse struct {
	ProjectID    string       `json:"project_id"`
	Jobs         []ClaimedJob `json:"jobs"`
	RetryAfterMS int64        `json:"retry_after_ms"`
}
type ProjectCompleteItem struct {
	JobID        int64         `json:"job_id"`
	AttemptID    string        `json:"attempt_id"`
	Outcome      Outcome       `json:"outcome"`
	WARCReceipts []WARCReceipt `json:"warc_receipts"`
}
type ProjectCompleteRequest struct {
	WorkerID string                `json:"worker_id"`
	Items    []ProjectCompleteItem `json:"items"`
}
type ExecutionError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details Attrs  `json:"details"`
}
type FailItem struct {
	JobID     int64          `json:"job_id"`
	AttemptID string         `json:"attempt_id"`
	Retryable bool           `json:"retryable"`
	Error     ExecutionError `json:"error"`
}
type ProjectFailRequest struct {
	WorkerID string     `json:"worker_id"`
	Items    []FailItem `json:"items"`
}
type AttemptRef struct {
	JobID     int64  `json:"job_id"`
	AttemptID string `json:"attempt_id"`
}
type ProjectExtendLeaseRequest struct {
	WorkerID      string       `json:"worker_id"`
	ExtendSeconds int64        `json:"extend_seconds"`
	Items         []AttemptRef `json:"items"`
}
type ItemResult struct {
	JobID          int64     `json:"job_id"`
	AttemptID      string    `json:"attempt_id"`
	Status         string    `json:"status"`
	JobStatus      *string   `json:"job_status"`
	LeaseExpiresAt *int64    `json:"lease_expires_at"`
	Error          *APIError `json:"error"`
}
type BatchResultResponse struct {
	Results []ItemResult `json:"results"`
}

type AdminProjectRequest struct {
	Status       string `json:"status"`
	IdentityMode string `json:"identity_mode,omitempty"`
	ClaimOrder   string `json:"claim_order,omitempty"`
}

type AdminProjectSummary struct {
	ID           string           `json:"id"`
	Status       string           `json:"status"`
	IdentityMode string           `json:"identity_mode"`
	ClaimOrder   string           `json:"claim_order"`
	JobCounts    map[string]int64 `json:"job_counts"`
	CreatedAt    int64            `json:"created_at"`
	UpdatedAt    int64            `json:"updated_at"`
}

type AdminProjectListResponse struct {
	Projects []AdminProjectSummary `json:"projects"`
}

type AdminEnqueueJobsRequest struct {
	Jobs []AdminEnqueueJob `json:"jobs"`
}

type AdminEnqueueJob struct {
	ID        string         `json:"id,omitempty"`
	Value     string         `json:"value"`
	Type      string         `json:"type,omitempty"`
	Via       *string        `json:"via,omitempty"`
	Hops      int            `json:"hops,omitempty"`
	Attrs     map[string]any `json:"attr,omitempty"`
	RandomKey *int32         `json:"random_key,omitempty"`
}

type AdminEnqueueJobsResponse struct {
	ProjectID string `json:"project_id"`
	Submitted int    `json:"submitted"`
	Inserted  int64  `json:"inserted"`
}

type AdminEnqueueSourceResponse struct {
	ProjectID         string `json:"project_id"`
	Jobs              int64  `json:"jobs"`
	Inserted          int64  `json:"inserted"`
	UncompressedBytes int64  `json:"uncompressed_bytes"`
}

type AdminUserRequest struct {
	Status string   `json:"status"`
	Roles  []string `json:"roles"`
}

type AdminUserSummary struct {
	ID                   string   `json:"id"`
	GitHubLogin          string   `json:"github_login,omitempty"`
	Status               string   `json:"status"`
	Roles                []string `json:"roles"`
	MachineTokenActive   bool     `json:"machine_token_active"`
	MachineTokenViewable bool     `json:"machine_token_viewable"`
	CreatedAt            int64    `json:"created_at"`
	UpdatedAt            int64    `json:"updated_at"`
}

type AdminUserListResponse struct {
	Users []AdminUserSummary `json:"users"`
}

type AdminMachineTokenResponse struct {
	UserID string `json:"user_id"`
	Token  string `json:"token"`
}

type AdminJob struct {
	JobSpecV1
	JobID          int64           `json:"job_id"`
	RandomKey      int32           `json:"random_key"`
	Status         string          `json:"status"`
	AttemptID      *string         `json:"attempt_id"`
	WorkerID       *string         `json:"worker_id"`
	LeaseExpiresAt *int64          `json:"lease_expires_at"`
	ResetCount     int             `json:"reset_count"`
	Outcome        *Outcome        `json:"outcome"`
	WARCReceipts   []WARCReceipt   `json:"warc_receipts"`
	ExecutionError *ExecutionError `json:"execution_error"`
	CreatedAt      int64           `json:"created_at"`
	UpdatedAt      int64           `json:"updated_at"`
	CompletedAt    *int64          `json:"completed_at"`
}

type AdminJobListResponse struct {
	Jobs           []AdminJob `json:"jobs"`
	NextAfterJobID *int64     `json:"next_after_job_id"`
}
