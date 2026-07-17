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
