package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"git.saveweb.org/saveweb/hq/internal/trackerclient"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

type Session struct {
	mu                      sync.Mutex
	config                  Config
	tracker                 *trackerclient.Client
	id                      string
	projectID               string
	leaseExpiresAt          int64
	agentHeartbeatSeconds   int64
	sessionHeartbeatSeconds int64
	assignment              *routeClient
	assignmentSessionLease  int64
	lastBackgroundError     error
	ctx                     context.Context
	cancel                  context.CancelFunc
	loops                   sync.WaitGroup
	claimsPaused            bool
	localAdmin              *LocalAdmin
	closed                  bool
}

func OpenSession(ctx context.Context, config Config, projectID string, attrs protocol.Attrs) (*Session, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	if projectID == "" || attrs == nil {
		return nil, fmt.Errorf("worker: project ID and attrs are required")
	}
	attrs, err = cloneAttrs(attrs)
	if err != nil {
		return nil, err
	}
	tracker, err := trackerFor(config)
	if err != nil {
		return nil, err
	}
	agent, err := tracker.UpsertAgent(ctx, protocol.AgentUpsertRequest{
		Kind: protocol.AgentKindWorker, Name: config.AgentName, Version: config.AgentVersion,
		Attrs: attrs,
	})
	if err != nil {
		return nil, convertTrackerError(err)
	}
	session, err := tracker.CreateSession(ctx, protocol.CreateSessionRequest{ProjectID: projectID, Attrs: attrs})
	if err != nil {
		return nil, convertTrackerError(err)
	}
	loopContext, cancel := context.WithCancel(context.Background())
	result := &Session{
		config: config, tracker: tracker, id: session.SessionID, projectID: projectID,
		leaseExpiresAt:          session.LeaseExpiresAt,
		agentHeartbeatSeconds:   agent.HeartbeatAfterSeconds,
		sessionHeartbeatSeconds: session.HeartbeatAfterSeconds,
		ctx:                     loopContext, cancel: cancel,
	}
	result.loops.Add(2)
	go result.agentHeartbeatLoop(attrs)
	go result.sessionHeartbeatLoop()
	return result, nil
}

func (s *Session) ID() string { return s.id }

func (s *Session) ProjectID() string { return s.projectID }

// SubmitReceiver writes one immutable jobs-jsonl-zstd-v1 object through the
// trusted tracker gateway. A transport ambiguity may create a duplicate object;
// later stages deduplicate by stable job ID.
func (s *Session) SubmitReceiver(
	ctx context.Context,
	receiverID string,
	jobs []protocol.JobSpecV1,
) (protocol.ReceiverBatchResponse, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return protocol.ReceiverBatchResponse{}, ErrSessionClosed
	}
	sessionID := s.id
	s.mu.Unlock()
	result, err := s.tracker.SubmitReceiverBatch(ctx, receiverID, protocol.ReceiverBatchRequest{
		SessionID: sessionID, Jobs: jobs,
	})
	if err != nil {
		return protocol.ReceiverBatchResponse{}, convertTrackerError(err)
	}
	if result.ProjectID != s.projectID || result.ReceiverID != receiverID ||
		result.Format != "jobs-jsonl-zstd-v1" || result.JobsCount != int64(len(jobs)) {
		return protocol.ReceiverBatchResponse{}, fmt.Errorf("worker: tracker returned a mismatched receiver result")
	}
	return result, nil
}

func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	assignment := s.assignment
	s.assignment = nil
	admin := s.localAdmin
	s.localAdmin = nil
	s.cancel()
	s.mu.Unlock()
	if admin != nil {
		_ = admin.Close()
	}
	s.loops.Wait()
	if assignment != nil {
		assignment.Close()
	}
	return nil
}

func (s *Session) SetClaimsPaused(value bool) {
	s.mu.Lock()
	s.claimsPaused = value
	s.mu.Unlock()
}

func (s *Session) ClaimsPaused() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimsPaused
}

func (s *Session) LastBackgroundError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastBackgroundError
}

func (s *Session) reportBackgroundError(err error) {
	err = convertTrackerError(err)
	s.mu.Lock()
	s.lastBackgroundError = err
	handler := s.config.OnBackgroundError
	s.mu.Unlock()
	if handler != nil {
		handler(err)
	}
}

func (s *Session) agentHeartbeatLoop(attrs protocol.Attrs) {
	defer s.loops.Done()
	interval := s.agentHeartbeatSeconds
	if interval < 1 {
		interval = 30
	}
	timer := time.NewTimer(time.Duration(interval) * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-timer.C:
		}
		ctx, cancel := context.WithTimeout(s.ctx, s.config.RequestTimeout)
		response, err := s.tracker.HeartbeatAgent(ctx, protocol.AgentHeartbeatRequest{
			Version: s.config.AgentVersion, Attrs: attrs,
		})
		cancel()
		next := int64(5)
		if err != nil {
			s.reportBackgroundError(err)
		} else {
			next = response.HeartbeatAfterSeconds
			if next < 1 {
				next = 30
			}
		}
		timer.Reset(time.Duration(next) * time.Second)
	}
}

func (s *Session) sessionHeartbeatLoop() {
	defer s.loops.Done()
	interval := s.sessionHeartbeatSeconds
	if interval < 1 {
		interval = 30
	}
	timer := time.NewTimer(time.Duration(interval) * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-timer.C:
		}
		ctx, cancel := context.WithTimeout(s.ctx, s.config.RequestTimeout)
		response, err := s.tracker.HeartbeatSession(ctx, s.id)
		cancel()
		next := int64(5)
		if err != nil {
			s.reportBackgroundError(err)
		} else {
			s.mu.Lock()
			s.leaseExpiresAt = response.LeaseExpiresAt
			s.mu.Unlock()
			next = response.HeartbeatAfterSeconds
			if next < 1 {
				next = 30
			}
		}
		timer.Reset(time.Duration(next) * time.Second)
	}
}

func (s *Session) route(ctx context.Context, acceptTypes []string, expected *routeIdentity, force bool) (*routeClient, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrSessionClosed
	}
	now := time.Now().Unix()
	if !force && s.assignment != nil && s.assignment.assignment.AccessTokenExpires > now+30 &&
		s.assignmentSessionLease == s.leaseExpiresAt && (expected == nil || expected.matches(s.assignment.assignment)) {
		result := s.assignment
		s.mu.Unlock()
		return result, nil
	}
	leaseAtRequest := s.leaseExpiresAt
	s.mu.Unlock()

	response, err := s.tracker.GetAssignment(ctx, protocol.GetAssignmentRequest{
		SessionID: s.id, AcceptTypes: acceptTypes,
	})
	if err != nil {
		return nil, convertTrackerError(err)
	}
	if response.Assignment == nil {
		if expected != nil {
			return nil, ErrRouteRetired
		}
		return nil, ErrNoAssignment
	}
	if expected != nil && !expected.matches(*response.Assignment) {
		return nil, ErrRouteRetired
	}
	created, err := newRouteClient(*response.Assignment, s.config)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		created.Close()
		return nil, ErrSessionClosed
	}
	previous := s.assignment
	s.assignment = created
	s.assignmentSessionLease = leaseAtRequest
	s.mu.Unlock()
	if previous != nil {
		previous.Close()
	}
	return created, nil
}

func cloneAttrs(value protocol.Attrs) (protocol.Attrs, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("worker: attrs are not JSON encodable: %w", err)
	}
	var result protocol.Attrs
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("worker: clone attrs: %w", err)
	}
	return result, nil
}

func convertTrackerError(err error) error {
	var trackerError *trackerclient.Error
	if errors.As(err, &trackerError) {
		return &APIError{Status: trackerError.Status, API: trackerError.API}
	}
	return err
}
