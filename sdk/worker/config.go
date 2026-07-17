package worker

import (
	"fmt"
	"time"

	"git.saveweb.org/saveweb/hq/internal/trackerclient"
)

type Config struct {
	TrackerURL        string
	MachineToken      string
	AgentID           string
	AgentName         string
	AgentVersion      string
	AllowHTTPTracker  bool
	AllowHTTPShard    bool
	RequestTimeout    time.Duration
	OnBackgroundError func(error)
}

func (c Config) normalized() (Config, error) {
	if c.TrackerURL == "" || c.MachineToken == "" || c.AgentID == "" {
		return Config{}, fmt.Errorf("worker: tracker URL and machine credentials are required")
	}
	if c.AgentName == "" {
		c.AgentName = "Saveweb worker"
	}
	if c.AgentVersion == "" {
		c.AgentVersion = "go-sdk-dev"
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 45 * time.Second
	}
	if c.RequestTimeout < time.Second || c.RequestTimeout > 10*time.Minute {
		return Config{}, fmt.Errorf("worker: request timeout is outside 1s-10m")
	}
	return c, nil
}

func trackerFor(config Config) (*trackerclient.Client, error) {
	return trackerclient.New(trackerclient.Config{
		BaseURL: config.TrackerURL, MachineToken: config.MachineToken,
		AgentID: config.AgentID, AllowHTTP: config.AllowHTTPTracker,
	})
}
