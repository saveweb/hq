package protocol

// Attrs is the bounded JSON extension object used by protocol messages.
type Attrs map[string]any

const (
	AgentKindShard  = "shard"
	AgentKindWorker = "worker"

	ItemStatusApplied        = "applied"
	ItemStatusAlreadyApplied = "already_applied"
	ItemStatusRejected       = "rejected"

	JobStatusTodo           = "todo"
	JobStatusWIP            = "wip"
	JobStatusDone           = "done"
	JobStatusFailed         = "failed"
	JobStatusResetExhausted = "reset_exhausted"
)

const (
	ErrorInternal                = "internal_error"
	ErrorInvalidRequest          = "invalid_request"
	ErrorInvalidJob              = "invalid_job"
	ErrorInvalidMachineToken     = "invalid_machine_token"
	ErrorInvalidAccessToken      = "invalid_access_token"
	ErrorPermissionDenied        = "permission_denied"
	ErrorAgentDisabled           = "agent_disabled"
	ErrorNotFound                = "not_found"
	ErrorStaleGeneration         = "stale_generation"
	ErrorStaleEndpointVersion    = "stale_endpoint_version"
	ErrorShardNotActive          = "shard_not_active"
	ErrorIdentityConflict        = "identity_conflict"
	ErrorRateLimited             = "rate_limited"
	ErrorBackpressure            = "backpressure"
	ErrorShardUnavailable        = "shard_unavailable"
	ErrorOwnerLeaseExpired       = "owner_lease_expired"
	ErrorLeaseExpired            = "lease_expired"
	ErrorStaleAttempt            = "stale_attempt"
	ErrorSessionExpired          = "session_expired"
	ErrorAttemptAlreadyFinalized = "attempt_already_finalized"
	ErrorUnsupportedOperation    = "unsupported_operation"
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

type Route struct {
	ProjectID  string `json:"project_id"`
	ShardID    string `json:"shard_id"`
	Generation int64  `json:"generation"`
}

type SessionRoute struct {
	Route
	SessionID string `json:"session_id"`
}

type Assignment struct {
	Route
	OwnerAgentID       string  `json:"owner_agent_id"`
	Endpoint           string  `json:"endpoint"`
	EndpointVersion    int64   `json:"endpoint_version"`
	TLSSPKISHA256      *string `json:"tls_spki_sha256"`
	AccessToken        string  `json:"access_token"`
	AccessTokenExpires int64   `json:"access_token_expires_at"`
}

type CreateSessionRequest struct {
	ProjectID string `json:"project_id"`
	Attrs     Attrs  `json:"attrs"`
}

type SessionResponse struct {
	SessionID             string `json:"session_id"`
	LeaseExpiresAt        int64  `json:"lease_expires_at"`
	HeartbeatAfterSeconds int64  `json:"heartbeat_after_seconds"`
}

type GetAssignmentRequest struct {
	SessionID   string   `json:"session_id"`
	AcceptTypes []string `json:"accept_types"`
}

type GetAssignmentResponse struct {
	Assignment   *Assignment `json:"assignment"`
	RetryAfterMS int64       `json:"retry_after_ms"`
}

type ClaimRequest struct {
	SessionRoute
	MaxJobs      int      `json:"max_jobs"`
	LeaseSeconds int64    `json:"lease_seconds"`
	AcceptTypes  []string `json:"accept_types"`
}

type ClaimedJob struct {
	JobSpecV1
	AttemptID      string `json:"attempt_id"`
	LeaseExpiresAt int64  `json:"lease_expires_at"`
}

type ClaimResponse struct {
	Route
	Jobs         []ClaimedJob `json:"jobs"`
	RetryAfterMS int64        `json:"retry_after_ms"`
}

type Outcome struct {
	Kind string  `json:"kind"`
	Code *int    `json:"code"`
	URI  *string `json:"uri"`
	Meta Attrs   `json:"meta"`
}

type CompleteItem struct {
	JobID          string      `json:"job_id"`
	AttemptID      string      `json:"attempt_id"`
	Outcome        Outcome     `json:"outcome"`
	DiscoveredJobs []JobSpecV1 `json:"discovered_jobs"`
}

type CompleteRequest struct {
	SessionRoute
	Items []CompleteItem `json:"items"`
}

type ExecutionError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details Attrs  `json:"details"`
}

type FailItem struct {
	JobID     string         `json:"job_id"`
	AttemptID string         `json:"attempt_id"`
	Retryable bool           `json:"retryable"`
	Error     ExecutionError `json:"error"`
}

type FailRequest struct {
	SessionRoute
	Items []FailItem `json:"items"`
}

type AttemptRef struct {
	JobID     string `json:"job_id"`
	AttemptID string `json:"attempt_id"`
}

type ExtendLeaseRequest struct {
	SessionRoute
	ExtendSeconds int64        `json:"extend_seconds"`
	Items         []AttemptRef `json:"items"`
}

type ItemResult struct {
	JobID          string    `json:"job_id"`
	AttemptID      string    `json:"attempt_id"`
	Status         string    `json:"status"`
	JobStatus      *string   `json:"job_status"`
	LeaseExpiresAt *int64    `json:"lease_expires_at"`
	Error          *APIError `json:"error"`
}

type BatchResultResponse struct {
	Results []ItemResult `json:"results"`
}

type EndpointChallenge struct {
	AgentID   string `json:"agent_id"`
	Challenge string `json:"challenge"`
}
