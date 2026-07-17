package sqlitequeue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"git.saveweb.org/saveweb/hq/internal/queue"
)

var _ queue.Snapshotter = (*Store)(nil)

func (s *Store) Snapshot(ctx context.Context, destination string) error {
	absPath, err := filepath.Abs(destination)
	if err != nil {
		return fmt.Errorf("sqlitequeue: snapshot path: %w", err)
	}
	if _, err := os.Lstat(absPath); err == nil || !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return fmt.Errorf("sqlitequeue: snapshot destination already exists")
		}
		return fmt.Errorf("sqlitequeue: inspect snapshot destination: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absPath), 0o700); err != nil {
		return fmt.Errorf("sqlitequeue: create snapshot directory: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, "VACUUM INTO ?", absPath); err != nil {
		_ = os.Remove(absPath)
		return fmt.Errorf("sqlitequeue: vacuum snapshot: %w", err)
	}
	remove := true
	defer func() {
		if remove {
			_ = os.Remove(absPath)
		}
	}()
	if err := os.Chmod(absPath, 0o600); err != nil {
		return fmt.Errorf("sqlitequeue: protect snapshot: %w", err)
	}
	if err := verifySnapshot(ctx, absPath); err != nil {
		return err
	}
	remove = false
	return nil
}

// VerifyCheckpoint validates the standalone SQLite file before a recovering
// shard installs it. It does not migrate or mutate the file.
func VerifyCheckpoint(ctx context.Context, path string, identity queue.Identity) error {
	if identity.ProjectID == "" || identity.ShardID == "" || identity.Generation < 1 {
		return fmt.Errorf("sqlitequeue: invalid checkpoint identity")
	}
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(path))
	if err != nil {
		return fmt.Errorf("sqlitequeue: open checkpoint verification: %w", err)
	}
	defer db.Close()
	var result string
	if err := db.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&result); err != nil {
		return fmt.Errorf("sqlitequeue: verify checkpoint: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("sqlitequeue: checkpoint quick_check failed")
	}
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version != schemaVersion {
		return fmt.Errorf("sqlitequeue: unsupported checkpoint schema version")
	}
	var projectID, shardID string
	var generation int64
	if err := db.QueryRowContext(ctx, `
		SELECT project_id, shard_id, generation FROM queue_meta WHERE singleton=1
	`).Scan(&projectID, &shardID, &generation); err != nil {
		return fmt.Errorf("sqlitequeue: read checkpoint identity: %w", err)
	}
	if projectID != identity.ProjectID || shardID != identity.ShardID || generation != identity.Generation {
		return fmt.Errorf("sqlitequeue: checkpoint identity mismatch")
	}
	return nil
}

func verifySnapshot(ctx context.Context, path string) error {
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(path))
	if err != nil {
		return fmt.Errorf("sqlitequeue: open snapshot verification: %w", err)
	}
	defer db.Close()
	var result string
	if err := db.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&result); err != nil {
		return fmt.Errorf("sqlitequeue: verify snapshot: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("sqlitequeue: snapshot quick_check failed")
	}
	return nil
}

func sqliteReadOnlyDSN(path string) string {
	value := &url.URL{Scheme: "file", Path: filepath.ToSlash(path)}
	query := value.Query()
	query.Set("mode", "ro")
	query.Add("_pragma", "query_only(1)")
	value.RawQuery = query.Encode()
	return value.String()
}
