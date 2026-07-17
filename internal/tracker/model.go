// Package tracker implements the SavewebHQ control-plane use cases without
// binding them to HTTP or a particular database.
package tracker

import (
	"errors"

	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const (
	UserStatusPending   = "pending"
	UserStatusActive    = "active"
	UserStatusSuspended = "suspended"

	RoleAdmin      = "admin"
	RoleShardOwner = "shard_owner"
	RoleWorker     = "worker"

	ProjectStatusActive   = "active"
	ProjectStatusDraining = "draining"
	ProjectStatusArchived = "archived"

	ShardStatusLoading        = "loading"
	ShardStatusActive         = "active"
	ShardStatusDraining       = "draining"
	ShardStatusPaused         = "paused"
	ShardStatusOffline        = "offline"
	ShardStatusRecovering     = "recovering"
	ShardStatusLoadFailed     = "load_failed"
	ShardStatusRecoveryFailed = "recovery_failed"

	EndpointNotApplicable = "not_applicable"
	EndpointUnchecked     = "unchecked"
	EndpointHealthy       = "healthy"
	EndpointInsecure      = "insecure"
	EndpointUnreachable   = "unreachable"
	EndpointTLSFailed     = "tls_failed"
	EndpointCacheFailed   = "cache_misconfigured"
)

type Error struct {
	Code       string
	Message    string
	Retryable  bool
	RetryAfter int64
	Details    map[string]any
	Cause      error
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func (e *Error) Unwrap() error { return e.Cause }

func IsCode(err error, code string) bool {
	var domainError *Error
	return errors.As(err, &domainError) && domainError.Code == code
}

type User struct {
	ID              string
	GitHubUserID    *int64
	GitHubLogin     string
	GitHubAvatarURL *string
	Status          string
	Roles           map[string]bool
	LastLoginAt     *int64
}

func (u User) HasRole(role string) bool { return u.Roles[role] }

type GitHubIdentity struct {
	UserID    int64
	Login     string
	AvatarURL *string
}

type AuditEvent struct {
	ID        int64
	ActorID   string
	Action    string
	TargetID  string
	Reason    string
	CreatedAt int64
}

type Agent struct {
	ID              string
	UserID          string
	Kind            string
	Name            string
	Version         string
	Status          string
	Endpoint        *string
	EndpointVersion *int64
	TLSSPKISHA256   *string
	EndpointStatus  string
	Attrs           map[string]any
	LastHeartbeatAt *int64
}

type AgentUpsert struct {
	ID              string
	UserID          string
	Kind            string
	Name            string
	Version         string
	Endpoint        *string
	EndpointVersion *int64
	TLSSPKISHA256   *string
	EndpointStatus  string
	Attrs           map[string]any
	Now             int64
}

type Project struct {
	ID     string
	Status string
}

const (
	ReceiverStatusActive  = "active"
	ReceiverStatusRemoved = "removed"
)

type Receiver struct {
	ProjectID string
	ID        string
	Status    string
	SinkURI   string
	Format    string
}

type ReceiverObject struct {
	ProjectID  string
	ReceiverID string
	ObjectURI  string
	Format     string
	JobsCount  int64
	SizeBytes  int64
	SHA256     string
	CreatedAt  int64
}

type Shard struct {
	ProjectID           string
	ID                  string
	Status              string
	OwnerAgentID        string
	Generation          int64
	OwnerLeaseExpiresAt int64
	SourceURI           *string
	SourceFormat        *string
	SourceETag          *string
}

// AdminShard contains read-only operational details shown in the tracker
// administration page. Queue routing continues to use the smaller Shard
// value above.
type AdminShard struct {
	Shard
	LoadErrorCode             *string
	RecoveryErrorCode         *string
	CheckpointURI             *string
	CheckpointSequence        int64
	CheckpointGeneration      *int64
	CheckpointSize            *int64
	CheckpointAt              *int64
	CheckpointUploadID        *string
	CheckpointUploadStartedAt *int64
}

type CheckpointUpload struct {
	ProjectID  string
	ShardID    string
	Generation int64
	ID         string
	S3UploadID string
	URI        string
	Sequence   int64
	SizeBytes  int64
	SHA256     string
	CreatedAt  int64
}

type Checkpoint struct {
	ProjectID  string
	ShardID    string
	Generation int64
	Sequence   int64
	URI        string
	Format     string
	SizeBytes  int64
	SHA256     string
	CreatedAt  int64
}

type Session struct {
	ID              string
	ProjectID       string
	AgentID         string
	UserID          string
	Attrs           map[string]any
	CreatedAt       int64
	LeaseExpiresAt  int64
	LastHeartbeatAt int64
}

type AssignmentCandidate struct {
	Session Session
	Shard   Shard
	Agent   Agent
}

type AgentHeartbeat struct {
	Agent            Agent
	OwnerAssignments []protocol.OwnerAssignment
}

func invalidRequest(message string) *Error {
	return &Error{Code: protocol.ErrorInvalidRequest, Message: message}
}

func permissionDenied(message string) *Error {
	return &Error{Code: protocol.ErrorPermissionDenied, Message: message}
}

func notFound(message string) *Error {
	return &Error{Code: protocol.ErrorNotFound, Message: message}
}
