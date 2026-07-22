// Package worker is the public Go SDK for SavewebHQ workers.
package worker

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/saveweb/hq/internal/trackerclient"
	"github.com/saveweb/hq/pkg/protocol"
)

type Config struct {
	TrackerURL       string
	MachineToken     string
	ClientVersion    string
	AllowHTTPTracker bool
	RequestTimeout   time.Duration
}

func (c Config) normalized() (Config, error) {
	if c.TrackerURL == "" || c.MachineToken == "" || c.ClientVersion == "" {
		return Config{}, fmt.Errorf("worker: tracker URL, machine token, and client version are required")
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
	return trackerclient.New(trackerclient.Config{BaseURL: c.TrackerURL, MachineToken: c.MachineToken, ClientVersion: c.ClientVersion, AllowHTTP: c.AllowHTTPTracker, RequestTimeout: c.RequestTimeout})
}

// WhoAmI returns the user ID associated with the configured machine token.
func WhoAmI(ctx context.Context, config Config) (string, error) {
	config, err := config.normalized()
	if err != nil {
		return "", err
	}
	client, err := trackerFor(config)
	if err != nil {
		return "", err
	}
	result, err := client.WhoAmI(ctx)
	if err != nil {
		return "", convertTrackerError(err)
	}
	if result.UserID == "" {
		return "", fmt.Errorf("worker: tracker returned an invalid user ID")
	}
	return result.UserID, nil
}

const workerIDAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

func newWorkerID() (string, error) {
	result := make([]byte, 7)
	random := make([]byte, 7)
	for index := range result {
		for {
			if _, err := rand.Read(random[index : index+1]); err != nil {
				return "", fmt.Errorf("worker: generate worker ID: %w", err)
			}
			if random[index] < 252 {
				result[index] = workerIDAlphabet[int(random[index])%len(workerIDAlphabet)]
				break
			}
		}
	}
	return string(result), nil
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
