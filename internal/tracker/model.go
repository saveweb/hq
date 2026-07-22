// Package tracker contains the small domain model shared by the coordinator.
package tracker

import (
	"errors"

	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

var ErrWebSessionNotFound = errors.New("web session not found")

const (
	UserStatusPending       = "pending"
	UserStatusActive        = "active"
	UserStatusSuspended     = "suspended"
	RoleAdmin               = "admin"
	RoleWorker              = "worker"
	ProjectStatusActive     = "active"
	ProjectStatusDraining   = "draining"
	ProjectStatusArchived   = "archived"
	IdentityModeNone        = "none"
	IdentityModeExternalID  = "external_id"
	IdentityModeUniqueValue = "unique_value"
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
	var target *Error
	return errors.As(err, &target) && target.Code == code
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

type Project struct {
	ID                      string
	Status                  string
	IdentityMode            string
	ClaimOrder              string
	DispatchQPS             *float64
	WorkerClaimQPS          *float64
	MaxJobsPerClaim         int
	MaxResets               *int
	RecommendedLeaseSeconds *int64
	ClientVersions          []string
	PolicyVersion           int64
}

type ProjectClaimResult struct {
	Jobs          []protocol.ClaimedJob
	RetryAfterMS  int64
	PolicyVersion int64
}

const (
	ClaimOrderFIFO   = "fifo"
	ClaimOrderRandom = "random"
)

func InvalidRequest(message string) *Error {
	return &Error{Code: protocol.ErrorInvalidRequest, Message: message}
}
