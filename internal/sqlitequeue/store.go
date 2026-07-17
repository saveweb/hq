// Package sqlitequeue implements queue.Store with one SQLite database per
// logical shard.
package sqlitequeue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
	_ "modernc.org/sqlite"
)

const schemaVersion = 1

var _ queue.Store = (*Store)(nil)

type Store struct {
	db         *sql.DB
	identityMu sync.RWMutex
	identity   queue.Identity
	now        func() int64
}

type Option func(*Store)

func WithClock(clock func() int64) Option {
	return func(store *Store) {
		if clock != nil {
			store.now = clock
		}
	}
}

func Open(ctx context.Context, path string, identity queue.Identity, options ...Option) (*Store, error) {
	if identity.ProjectID == "" || identity.ShardID == "" || identity.Generation < 1 {
		return nil, fmt.Errorf("sqlitequeue: invalid identity")
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("sqlitequeue: absolute path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absPath), 0o700); err != nil {
		return nil, fmt.Errorf("sqlitequeue: create database directory: %w", err)
	}
	dsn := sqliteDSN(absPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlitequeue: open: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlitequeue: ping: %w", err)
	}
	store := &Store{db: db, identity: identity, now: func() int64 { return time.Now().Unix() }}
	for _, option := range options {
		option(store)
	}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func sqliteDSN(path string) string {
	u := &url.URL{Scheme: "file", Path: filepath.ToSlash(path)}
	query := u.Query()
	query.Set("_txlock", "immediate")
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "journal_mode(WAL)")
	query.Add("_pragma", "synchronous(NORMAL)")
	u.RawQuery = query.Encode()
	return u.String()
}

func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlitequeue: begin migration: %w", err)
	}
	defer tx.Rollback()

	var version int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("sqlitequeue: read schema version: %w", err)
	}
	if version > schemaVersion {
		return fmt.Errorf("sqlitequeue: database schema %d is newer than supported %d", version, schemaVersion)
	}
	if version == 0 {
		for _, statement := range schemaV1 {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("sqlitequeue: apply schema: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx, "PRAGMA user_version = 1"); err != nil {
			return fmt.Errorf("sqlitequeue: set schema version: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO queue_meta
			(singleton, project_id, shard_id, generation, owner_lease_expires_at)
		VALUES (1, ?, ?, ?, 0)`, s.identity.ProjectID, s.identity.ShardID, s.identity.Generation); err != nil {
		return fmt.Errorf("sqlitequeue: initialize identity: %w", err)
	}
	var projectID, shardID string
	var generation int64
	if err := tx.QueryRowContext(ctx,
		"SELECT project_id, shard_id, generation FROM queue_meta WHERE singleton = 1",
	).Scan(&projectID, &shardID, &generation); err != nil {
		return fmt.Errorf("sqlitequeue: read identity: %w", err)
	}
	if projectID != s.identity.ProjectID || shardID != s.identity.ShardID {
		return fmt.Errorf("sqlitequeue: identity mismatch: database is %s/%s", projectID, shardID)
	}
	if generation > s.identity.Generation {
		return fmt.Errorf("sqlitequeue: database generation %d is newer than requested %d", generation, s.identity.Generation)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlitequeue: commit migration: %w", err)
	}
	return nil
}

var schemaV1 = []string{
	`CREATE TABLE queue_meta (
		singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
		project_id TEXT NOT NULL,
		shard_id TEXT NOT NULL,
		generation INTEGER NOT NULL CHECK (generation > 0),
		owner_lease_expires_at INTEGER NOT NULL CHECK (owner_lease_expires_at >= 0)
	) STRICT`,
	`CREATE TABLE jobs (
		id TEXT PRIMARY KEY,
		url TEXT NOT NULL,
		type TEXT NOT NULL CHECK (type IN ('seed', 'asset')),
		via TEXT,
		hops INTEGER NOT NULL CHECK (hops >= 0),
		attr_json TEXT NOT NULL CHECK (json_valid(attr_json) AND json_type(attr_json) = 'object'),
		status TEXT NOT NULL CHECK (status IN ('todo', 'wip', 'done', 'failed', 'reset_exhausted')),
		session_id TEXT,
		attempt_id TEXT,
		claim_generation INTEGER,
		lease_expires_at INTEGER,
		reset_count INTEGER NOT NULL DEFAULT 0 CHECK (reset_count >= 0),
		outcome_kind TEXT,
		outcome_code INTEGER,
		outcome_uri TEXT,
		outcome_meta_json TEXT,
		error_json TEXT,
		final_operation TEXT CHECK (final_operation IN ('complete', 'fail') OR final_operation IS NULL),
		final_payload_hash BLOB,
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL,
		CHECK (status != 'wip' OR (
			session_id IS NOT NULL AND attempt_id IS NOT NULL AND
			claim_generation IS NOT NULL AND lease_expires_at IS NOT NULL
		)),
		CHECK ((final_operation IS NULL) = (final_payload_hash IS NULL))
	) STRICT`,
	`CREATE INDEX jobs_claim_idx ON jobs (status, type, id)`,
	`CREATE INDEX jobs_lease_idx ON jobs (status, lease_expires_at)`,
}

func (s *Store) Identity() queue.Identity {
	s.identityMu.RLock()
	defer s.identityMu.RUnlock()
	return s.identity
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) SetFence(ctx context.Context, generation, now, ownerLeaseExpiresAt int64) error {
	if generation < 1 || now < 0 || ownerLeaseExpiresAt <= now {
		return &queue.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid generation or owner lease"}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlitequeue: begin fence update: %w", err)
	}
	defer tx.Rollback()

	var current int64
	if err := tx.QueryRowContext(ctx, "SELECT generation FROM queue_meta WHERE singleton = 1").Scan(&current); err != nil {
		return fmt.Errorf("sqlitequeue: read fence: %w", err)
	}
	if generation < current {
		return staleGeneration(current)
	}
	if generation > current {
		if _, err := tx.ExecContext(ctx, `
			UPDATE jobs
			SET status = 'todo', session_id = NULL, attempt_id = NULL,
				claim_generation = NULL, lease_expires_at = NULL, updated_at = ?
			WHERE status = 'wip'`, now); err != nil {
			return fmt.Errorf("sqlitequeue: recover in-flight jobs: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE queue_meta SET generation = ?, owner_lease_expires_at = ? WHERE singleton = 1`,
			generation, ownerLeaseExpiresAt); err != nil {
			return fmt.Errorf("sqlitequeue: advance fence: %w", err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, `
			UPDATE queue_meta
			SET owner_lease_expires_at = max(owner_lease_expires_at, ?)
			WHERE singleton = 1`, ownerLeaseExpiresAt); err != nil {
			return fmt.Errorf("sqlitequeue: renew fence: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlitequeue: commit fence: %w", err)
	}
	s.identityMu.Lock()
	s.identity.Generation = generation
	s.identityMu.Unlock()
	return nil
}

func checkFence(ctx context.Context, tx *sql.Tx, generation, now int64) error {
	var current, lease int64
	if err := tx.QueryRowContext(ctx,
		"SELECT generation, owner_lease_expires_at FROM queue_meta WHERE singleton = 1",
	).Scan(&current, &lease); err != nil {
		return fmt.Errorf("sqlitequeue: read fence: %w", err)
	}
	if generation != current {
		return staleGeneration(current)
	}
	if now >= lease {
		return &queue.Error{
			Code:    protocol.ErrorOwnerLeaseExpired,
			Message: "owner lease expired",
			Details: map[string]any{"owner_lease_expires_at": lease},
		}
	}
	return nil
}

func (s *Store) checkBeforeCommit(ctx context.Context, tx *sql.Tx, generation, operationNow int64) error {
	return checkFence(ctx, tx, generation, max(operationNow, s.now()))
}

func staleGeneration(current int64) *queue.Error {
	return &queue.Error{
		Code:    protocol.ErrorStaleGeneration,
		Message: "shard generation changed",
		Details: map[string]any{"current_generation": current},
	}
}

func asQueueError(err error) (*queue.Error, bool) {
	var value *queue.Error
	ok := errors.As(err, &value)
	return value, ok
}
