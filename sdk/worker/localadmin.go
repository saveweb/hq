package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"git.saveweb.org/saveweb/hq/internal/localadmin"
)

type RuntimeRoute struct {
	ProjectID            string `json:"project_id"`
	ShardID              string `json:"shard_id"`
	Generation           int64  `json:"generation"`
	OwnerAgentID         string `json:"owner_agent_id"`
	AccessTokenExpiresAt int64  `json:"access_token_expires_at"`
}

type RuntimeStatus struct {
	AgentID               string        `json:"agent_id"`
	ProjectID             string        `json:"project_id"`
	SessionID             string        `json:"session_id"`
	SessionLeaseExpiresAt int64         `json:"session_lease_expires_at"`
	ServerTime            int64         `json:"server_time"`
	ClaimsPaused          bool          `json:"claims_paused"`
	Closed                bool          `json:"closed"`
	LastBackgroundError   string        `json:"last_background_error,omitempty"`
	Route                 *RuntimeRoute `json:"route,omitempty"`
}

func (s *Session) RuntimeStatus() RuntimeStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := RuntimeStatus{
		AgentID: s.config.AgentID, ProjectID: s.projectID, SessionID: s.id,
		SessionLeaseExpiresAt: s.leaseExpiresAt, ServerTime: time.Now().Unix(),
		ClaimsPaused: s.claimsPaused, Closed: s.closed,
	}
	if s.lastBackgroundError != nil {
		status.LastBackgroundError = s.lastBackgroundError.Error()
	}
	if s.assignment != nil {
		assignment := s.assignment.assignment
		status.Route = &RuntimeRoute{
			ProjectID: assignment.ProjectID, ShardID: assignment.ShardID,
			Generation: assignment.Generation, OwnerAgentID: assignment.OwnerAgentID,
			AccessTokenExpiresAt: assignment.AccessTokenExpires,
		}
	}
	return status
}

type LocalAdminConfig struct {
	// Listen must resolve to an explicit 127.0.0.1 address. The default is
	// 127.0.0.1:9082; port 0 can be used by tests or embedding applications.
	Listen string
	// Token uses SAVEWEB_LOCAL_ADMIN_TOKEN when empty. If both are empty, a
	// fresh 256-bit token is generated and returned by LocalAdmin.Token.
	Token string
}

type LocalAdmin struct {
	session   *Session
	server    *http.Server
	address   string
	token     string
	generated bool
	errors    chan error
	once      sync.Once
	closeErr  error
}

func (s *Session) StartLocalAdmin(config LocalAdminConfig) (*LocalAdmin, error) {
	if config.Listen == "" {
		config.Listen = "127.0.0.1:9082"
	}
	token := config.Token
	if token == "" {
		token = os.Getenv("SAVEWEB_LOCAL_ADMIN_TOKEN")
	}
	generated := false
	if token == "" {
		var err error
		token, err = localadmin.GenerateToken()
		if err != nil {
			return nil, err
		}
		generated = true
	}
	listener, err := net.Listen("tcp", config.Listen)
	if err != nil {
		return nil, fmt.Errorf("worker local admin: listen: %w", err)
	}
	host, _, err := net.SplitHostPort(listener.Addr().String())
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).Equal(net.IPv4(127, 0, 0, 1)) {
		_ = listener.Close()
		return nil, fmt.Errorf("worker local admin: listener must bind explicit 127.0.0.1")
	}
	address := listener.Addr().String()
	handler, err := localadmin.NewWorkerServer(
		func(context.Context) (any, error) { return s.RuntimeStatus(), nil },
		s.SetClaimsPaused, token, "http://"+address, nil,
	)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	server := &http.Server{
		Handler: handler, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	admin := &LocalAdmin{
		session: s, server: server, address: address, token: token,
		generated: generated, errors: make(chan error, 1),
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = listener.Close()
		return nil, ErrSessionClosed
	}
	if s.localAdmin != nil {
		s.mu.Unlock()
		_ = listener.Close()
		return nil, fmt.Errorf("worker: local admin is already running")
	}
	s.localAdmin = admin
	s.mu.Unlock()
	go func() {
		serveError := server.Serve(listener)
		if serveError != nil && !errors.Is(serveError, http.ErrServerClosed) {
			admin.errors <- serveError
		}
		s.mu.Lock()
		if s.localAdmin == admin {
			s.localAdmin = nil
		}
		s.mu.Unlock()
		close(admin.errors)
	}()
	return admin, nil
}

func (a *LocalAdmin) Address() string { return a.address }

func (a *LocalAdmin) Token() string { return a.token }

func (a *LocalAdmin) TokenWasGenerated() bool { return a.generated }

func (a *LocalAdmin) Errors() <-chan error { return a.errors }

func (a *LocalAdmin) Close() error {
	a.once.Do(func() {
		a.closeErr = a.server.Close()
		if errors.Is(a.closeErr, http.ErrServerClosed) {
			a.closeErr = nil
		}
		a.session.mu.Lock()
		if a.session.localAdmin == a {
			a.session.localAdmin = nil
		}
		a.session.mu.Unlock()
	})
	return a.closeErr
}
