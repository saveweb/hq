// Package worker is the public Go SDK for SavewebHQ workers.
package worker

import (
	"errors"
	"fmt"
	"time"

	"github.com/saveweb/hq/internal/trackerclient"
	"github.com/saveweb/hq/pkg/protocol"
)

type Config struct {
	TrackerURL       string
	MachineToken     string
	WorkerID         string
	ClientVersion    string
	AllowHTTPTracker bool
	RequestTimeout   time.Duration
}

func (c Config) normalized() (Config, error) {
	if c.TrackerURL == "" || c.MachineToken == "" || c.WorkerID == "" || c.ClientVersion == "" {
		return Config{}, fmt.Errorf("worker: tracker URL, machine token, worker ID, and client version are required")
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 45 * time.Second
	}
	if c.RequestTimeout < time.Second || c.RequestTimeout > 10*time.Minute {
		return Config{}, fmt.Errorf("worker: request timeout is outside 1s-10m")
	}
	return c, nil
}
func trackerFor(c Config) (*trackerclient.Client, error) {
	return trackerclient.New(trackerclient.Config{BaseURL: c.TrackerURL, MachineToken: c.MachineToken, WorkerID: c.WorkerID, ClientVersion: c.ClientVersion, AllowHTTP: c.AllowHTTPTracker, RequestTimeout: c.RequestTimeout})
}

type APIError struct {
	Status int
	API    protocol.APIError
}

func (e *APIError) Error() string {
	return fmt.Sprintf("worker HTTP %d: %s: %s", e.Status, e.API.Code, e.API.Message)
}
func IsCode(err error, code string) bool {
	var target *APIError
	return errors.As(err, &target) && target.API.Code == code
}
func convertTrackerError(err error) error {
	var target *trackerclient.Error
	if errors.As(err, &target) {
		return &APIError{Status: target.Status, API: target.API}
	}
	return err
}
