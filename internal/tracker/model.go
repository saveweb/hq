// Package tracker contains the small domain model shared by the coordinator.
package tracker

import (
	"errors"

	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const (
	UserStatusPending     = "pending"
	UserStatusActive      = "active"
	UserStatusSuspended   = "suspended"
	RoleAdmin             = "admin"
	RoleWorker            = "worker"
	ProjectStatusActive   = "active"
	ProjectStatusDraining = "draining"
	ProjectStatusArchived = "archived"
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
	ID     string
	Status string
	Roles  map[string]bool
}

func (u User) HasRole(role string) bool { return u.Roles[role] }

type Project struct {
	ID     string
	Status string
}

func InvalidRequest(message string) *Error {
	return &Error{Code: protocol.ErrorInvalidRequest, Message: message}
}
