// Package worker is the public Go SDK for SavewebHQ workers.
package worker

import (
	"errors"
	"fmt"

	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

var (
	ErrNoAssignment  = errors.New("worker: no active shard assignment")
	ErrRouteRetired  = errors.New("worker: claimed shard route was retired; discard the local outcome")
	ErrSessionClosed = errors.New("worker: session is closed")
	ErrClaimsPaused  = errors.New("worker: new claims are paused by local administration")
)

type APIError struct {
	Status int
	API    protocol.APIError
}

func (e *APIError) Error() string {
	return fmt.Sprintf("worker HTTP %d: %s: %s", e.Status, e.API.Code, e.API.Message)
}

func IsCode(err error, code string) bool {
	var apiError *APIError
	return errors.As(err, &apiError) && apiError.API.Code == code
}
