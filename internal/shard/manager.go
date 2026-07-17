// Package shard implements the shard daemon's framework-independent runtime.
package shard

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"git.saveweb.org/saveweb/hq/internal/access"
	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/sqlitequeue"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

type StoreOpener func(ctx context.Context, path string, identity queue.Identity, now func() int64) (queue.Store, error)

type ManagerConfig struct {
	AgentID   string
	Issuer    string
	DataDir   string
	Clock     func() int64
	OpenStore StoreOpener
}

type routeKey struct {
	projectID string
	shardID   string
}

type ownedShard struct {
	assignment protocol.OwnerAssignment
	store      queue.Store
}

type Manager struct {
	mu            sync.RWMutex
	agentID       string
	issuer        string
	dataDir       string
	clock         func() int64
	clockOffset   int64
	openStore     StoreOpener
	verifier      *access.Verifier
	owned         map[routeKey]*ownedShard
	retiredStores []queue.Store
	claimsPaused  bool
	closed        bool
}

type Authorization struct {
	Claims       access.Claims
	Store        queue.Store
	Assignment   protocol.OwnerAssignment
	ClaimsPaused bool
}

type MaintenanceResult struct {
	Requeued       int
	ResetExhausted int
}

type RuntimeStatus struct {
	AgentID      string        `json:"agent_id"`
	ServerTime   int64         `json:"server_time"`
	ClaimsPaused bool          `json:"claims_paused"`
	Shards       []ShardStatus `json:"shards"`
}

type ShardStatus struct {
	ProjectID           string      `json:"project_id"`
	ShardID             string      `json:"shard_id"`
	Generation          int64       `json:"generation"`
	Status              string      `json:"status"`
	OwnerLeaseExpiresAt int64       `json:"owner_lease_expires_at"`
	Stats               queue.Stats `json:"stats"`
}

func NewManager(config ManagerConfig) (*Manager, error) {
	if !queue.ValidateIdentifier(config.AgentID) || config.Issuer == "" || config.DataDir == "" {
		return nil, fmt.Errorf("shard: invalid manager configuration")
	}
	if config.Clock == nil {
		config.Clock = func() int64 { return time.Now().Unix() }
	}
	if config.OpenStore == nil {
		config.OpenStore = func(ctx context.Context, path string, identity queue.Identity, now func() int64) (queue.Store, error) {
			return sqlitequeue.Open(ctx, path, identity, sqlitequeue.WithClock(now))
		}
	}
	return &Manager{
		agentID: config.AgentID, issuer: config.Issuer, dataDir: config.DataDir,
		clock: config.Clock, openStore: config.OpenStore, owned: make(map[routeKey]*ownedShard),
	}, nil
}

func (m *Manager) Now() int64 {
	m.mu.RLock()
	offset := m.clockOffset
	m.mu.RUnlock()
	return m.clock() + offset
}

func (m *Manager) ApplyHeartbeat(ctx context.Context, heartbeat protocol.AgentHeartbeatResponse) error {
	localNow := m.clock()
	if heartbeat.ServerTime < 0 || heartbeat.HeartbeatAfterSeconds < 1 {
		return fmt.Errorf("shard: invalid tracker heartbeat")
	}
	offset := heartbeat.ServerTime - localNow
	adjustedNow := localNow + offset
	keys := make(map[string]ed25519.PublicKey, len(heartbeat.SigningKeys))
	for _, value := range heartbeat.SigningKeys {
		if value.Algorithm != "EdDSA" || value.KeyID == "" || value.NotBefore < 0 || value.NotAfter <= value.NotBefore {
			return fmt.Errorf("shard: invalid tracker signing key metadata")
		}
		if adjustedNow+access.DefaultSkewSec < value.NotBefore || adjustedNow-access.DefaultSkewSec >= value.NotAfter {
			continue
		}
		decoded, err := base64.RawURLEncoding.DecodeString(value.PublicKeyEd25519)
		if err != nil || len(decoded) != ed25519.PublicKeySize {
			return fmt.Errorf("shard: invalid tracker signing public key")
		}
		if _, duplicate := keys[value.KeyID]; duplicate {
			return fmt.Errorf("shard: duplicate tracker signing key ID")
		}
		keys[value.KeyID] = ed25519.PublicKey(decoded)
	}
	if len(keys) == 0 {
		return fmt.Errorf("shard: heartbeat has no currently valid signing key")
	}
	verifier, err := access.NewVerifier(m.issuer, keys, m.Now, access.DefaultSkewSec)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("shard: manager is closed")
	}
	m.clockOffset = offset
	next := make(map[routeKey]*ownedShard, len(heartbeat.OwnerAssignments))
	opened := []queue.Store{}
	rollbackOpened := func() {
		for _, store := range opened {
			_ = store.Close()
		}
	}
	for _, assignment := range heartbeat.OwnerAssignments {
		if err := validateAssignment(assignment, adjustedNow); err != nil {
			rollbackOpened()
			return err
		}
		key := routeKey{projectID: assignment.ProjectID, shardID: assignment.ShardID}
		if _, duplicate := next[key]; duplicate {
			rollbackOpened()
			return fmt.Errorf("shard: duplicate owner assignment")
		}
		current := m.owned[key]
		if current == nil {
			if assignment.SourceURI != nil {
				rollbackOpened()
				return &queue.Error{Code: protocol.ErrorUnsupportedOperation, Message: "source/checkpoint loading is not implemented yet"}
			}
			path := m.databasePath(assignment.ProjectID, assignment.ShardID)
			store, openError := m.openStore(ctx, path, queue.Identity{
				ProjectID: assignment.ProjectID, ShardID: assignment.ShardID, Generation: assignment.Generation,
			}, m.Now)
			if openError != nil {
				rollbackOpened()
				return fmt.Errorf("shard: open queue %s/%s: %w", assignment.ProjectID, assignment.ShardID, openError)
			}
			opened = append(opened, store)
			current = &ownedShard{store: store}
		}
		if err := current.store.SetFence(ctx, assignment.Generation, adjustedNow, assignment.OwnerLeaseExpiresAt); err != nil {
			rollbackOpened()
			return fmt.Errorf("shard: install fence %s/%s: %w", assignment.ProjectID, assignment.ShardID, err)
		}
		current.assignment = assignment
		next[key] = current
	}
	for key, value := range m.owned {
		if _, retained := next[key]; !retained {
			m.retiredStores = append(m.retiredStores, value.store)
		}
	}
	m.owned = next
	m.verifier = verifier
	return nil
}

func (m *Manager) Authorize(token string) (Authorization, error) {
	m.mu.RLock()
	verifier := m.verifier
	agentID := m.agentID
	m.mu.RUnlock()
	if verifier == nil {
		return Authorization{}, queueError(protocol.ErrorShardUnavailable, "shard has not received tracker state", true)
	}
	claims, err := verifier.VerifyToken(token)
	if err != nil {
		if errors.Is(err, access.ErrSessionExpired) {
			return Authorization{}, queueError(protocol.ErrorSessionExpired, "worker session expired", false)
		}
		return Authorization{}, queueError(protocol.ErrorInvalidAccessToken, "invalid shard access token", false)
	}
	if claims.OwnerAgentID != agentID {
		return Authorization{}, staleOwnerOrGeneration()
	}
	key := routeKey{projectID: claims.ProjectID, shardID: claims.ShardID}
	m.mu.RLock()
	owned := m.owned[key]
	now := m.clock() + m.clockOffset
	if owned == nil {
		m.mu.RUnlock()
		return Authorization{}, queueError(protocol.ErrorShardNotActive, "shard is not owned here", false)
	}
	assignment := owned.assignment
	store := owned.store
	claimsPaused := m.claimsPaused
	m.mu.RUnlock()
	if assignment.Generation != claims.Generation {
		return Authorization{}, &queue.Error{
			Code: protocol.ErrorStaleGeneration, Message: "shard generation changed",
			Details: map[string]any{"current_generation": assignment.Generation},
		}
	}
	if now >= assignment.OwnerLeaseExpiresAt {
		return Authorization{}, &queue.Error{
			Code: protocol.ErrorOwnerLeaseExpired, Message: "owner lease expired", Retryable: true,
			Details: map[string]any{"owner_lease_expires_at": assignment.OwnerLeaseExpiresAt},
		}
	}
	return Authorization{
		Claims: claims, Store: store, Assignment: assignment, ClaimsPaused: claimsPaused,
	}, nil
}

func (m *Manager) Maintain(ctx context.Context, maxResets, limitPerShard int) (MaintenanceResult, error) {
	m.mu.RLock()
	now := m.clock() + m.clockOffset
	owned := make([]*ownedShard, 0, len(m.owned))
	for _, value := range m.owned {
		if (value.assignment.Status == trackerStatusActive || value.assignment.Status == trackerStatusDraining) &&
			now < value.assignment.OwnerLeaseExpiresAt {
			owned = append(owned, value)
		}
	}
	m.mu.RUnlock()
	var result MaintenanceResult
	for _, value := range owned {
		requeued, err := value.store.RequeueExpired(ctx, value.assignment.Generation, now, maxResets, limitPerShard)
		if err != nil {
			return result, err
		}
		result.Requeued += requeued.Requeued
		result.ResetExhausted += requeued.ResetExhausted
	}
	return result, nil
}

func (a Authorization) CheckRoute(route protocol.SessionRoute) error {
	if route.ProjectID != a.Claims.ProjectID || route.ShardID != a.Claims.ShardID || route.SessionID != a.Claims.SessionID {
		return queueError(protocol.ErrorInvalidAccessToken, "request route is outside token scope", false)
	}
	if route.Generation != a.Claims.Generation || route.Generation != a.Assignment.Generation {
		return &queue.Error{
			Code: protocol.ErrorStaleGeneration, Message: "shard generation changed",
			Details: map[string]any{"current_generation": a.Assignment.Generation},
		}
	}
	return nil
}

func (a Authorization) AllowsClaim() error {
	if a.ClaimsPaused {
		return queueError(protocol.ErrorShardNotActive, "new claims are paused by local administrator", true)
	}
	if a.Assignment.Status != trackerStatusActive {
		return queueError(protocol.ErrorShardNotActive, "shard is not accepting new claims", false)
	}
	return nil
}

func (m *Manager) SetClaimsPaused(value bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.claimsPaused = value
}

func (m *Manager) RuntimeStatus(ctx context.Context) (RuntimeStatus, error) {
	type captured struct {
		assignment protocol.OwnerAssignment
		store      queue.Store
	}
	m.mu.RLock()
	result := RuntimeStatus{
		AgentID: m.agentID, ServerTime: m.clock() + m.clockOffset,
		ClaimsPaused: m.claimsPaused, Shards: make([]ShardStatus, 0, len(m.owned)),
	}
	values := make([]captured, 0, len(m.owned))
	for _, owned := range m.owned {
		values = append(values, captured{assignment: owned.assignment, store: owned.store})
	}
	m.mu.RUnlock()
	for _, value := range values {
		stats, err := value.store.Stats(ctx)
		if err != nil {
			return RuntimeStatus{}, err
		}
		result.Shards = append(result.Shards, ShardStatus{
			ProjectID: value.assignment.ProjectID, ShardID: value.assignment.ShardID,
			Generation: value.assignment.Generation, Status: value.assignment.Status,
			OwnerLeaseExpiresAt: value.assignment.OwnerLeaseExpiresAt, Stats: stats,
		})
	}
	sort.Slice(result.Shards, func(i, j int) bool {
		if result.Shards[i].ProjectID != result.Shards[j].ProjectID {
			return result.Shards[i].ProjectID < result.Shards[j].ProjectID
		}
		return result.Shards[i].ShardID < result.Shards[j].ShardID
	})
	return result, nil
}

func (a Authorization) AllowsMutation() error {
	if a.Assignment.Status != trackerStatusActive && a.Assignment.Status != trackerStatusDraining {
		return queueError(protocol.ErrorShardNotActive, "shard is not accepting queue mutations", false)
	}
	return nil
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	seen := make(map[queue.Store]struct{})
	var joined error
	for _, value := range m.owned {
		seen[value.store] = struct{}{}
		joined = errors.Join(joined, value.store.Close())
	}
	for _, store := range m.retiredStores {
		if _, duplicate := seen[store]; duplicate {
			continue
		}
		seen[store] = struct{}{}
		joined = errors.Join(joined, store.Close())
	}
	m.owned = nil
	return joined
}

func (m *Manager) databasePath(projectID, shardID string) string {
	project := base64.RawURLEncoding.EncodeToString([]byte(projectID))
	shard := base64.RawURLEncoding.EncodeToString([]byte(shardID))
	return filepath.Join(m.dataDir, "queues", project, shard+".sqlite")
}

func validateAssignment(value protocol.OwnerAssignment, now int64) error {
	if !queue.ValidateIdentifier(value.ProjectID) || !queue.ValidateIdentifier(value.ShardID) ||
		value.Generation < 1 || value.OwnerLeaseExpiresAt <= now {
		return fmt.Errorf("shard: invalid or expired owner assignment")
	}
	switch value.Status {
	case trackerStatusLoading, trackerStatusActive, trackerStatusDraining,
		trackerStatusPaused, trackerStatusOffline, trackerStatusRecovering:
		return nil
	default:
		return fmt.Errorf("shard: invalid owner assignment status")
	}
}

func queueError(code, message string, retryable bool) *queue.Error {
	return &queue.Error{Code: code, Message: message, Retryable: retryable}
}

func staleOwnerOrGeneration() *queue.Error {
	return queueError(protocol.ErrorStaleGeneration, "token targets a different shard owner or generation", false)
}

const (
	trackerStatusLoading    = "loading"
	trackerStatusActive     = "active"
	trackerStatusDraining   = "draining"
	trackerStatusPaused     = "paused"
	trackerStatusOffline    = "offline"
	trackerStatusRecovering = "recovering"
)
