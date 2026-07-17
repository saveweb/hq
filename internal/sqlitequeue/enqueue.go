package sqlitequeue

import (
	"context"
	"database/sql"
	"fmt"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func (s *Store) Enqueue(
	ctx context.Context,
	generation, now int64,
	jobs []queue.JobSpec,
) (queue.EnqueueResult, error) {
	normalized := make([]queue.NormalizedJob, len(jobs))
	for i, job := range jobs {
		value, err := queue.NormalizeJob(job)
		if err != nil {
			return queue.EnqueueResult{}, err
		}
		normalized[i] = value
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return queue.EnqueueResult{}, fmt.Errorf("sqlitequeue: begin enqueue: %w", err)
	}
	defer tx.Rollback()
	if err := checkFence(ctx, tx, generation, now); err != nil {
		return queue.EnqueueResult{}, err
	}

	var result queue.EnqueueResult
	for _, job := range normalized {
		inserted, err := enqueueOne(ctx, tx, now, job)
		if err != nil {
			return queue.EnqueueResult{}, err
		}
		if inserted {
			result.Inserted++
		} else {
			result.Duplicate++
		}
	}
	if err := s.checkBeforeCommit(ctx, tx, generation, now); err != nil {
		return queue.EnqueueResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return queue.EnqueueResult{}, fmt.Errorf("sqlitequeue: commit enqueue: %w", err)
	}
	return result, nil
}

func enqueueOne(ctx context.Context, tx *sql.Tx, now int64, job queue.NormalizedJob) (bool, error) {
	result, err := tx.ExecContext(ctx, `
		INSERT INTO jobs (
			id, url, type, via, hops, attr_json, status, reset_count, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, 'todo', 0, ?, ?)
		ON CONFLICT(id) DO NOTHING`,
		job.ID, job.URL, job.Type, job.Via, job.Hops, job.AttrsJSON, now, now)
	if err != nil {
		return false, fmt.Errorf("sqlitequeue: insert job %q: %w", job.ID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("sqlitequeue: rows affected for %q: %w", job.ID, err)
	}
	if rows == 1 {
		return true, nil
	}

	var existingType, existingURL, existingAttrs string
	if err := tx.QueryRowContext(ctx,
		"SELECT type, url, attr_json FROM jobs WHERE id = ?", job.ID,
	).Scan(&existingType, &existingURL, &existingAttrs); err != nil {
		return false, fmt.Errorf("sqlitequeue: read conflicting job %q: %w", job.ID, err)
	}
	if existingType == job.Type && existingURL == job.URL && existingAttrs == job.AttrsJSON {
		return false, nil
	}
	return false, &queue.Error{
		Code:    protocol.ErrorIdentityConflict,
		Message: "job ID already exists with different identity fields",
		Details: map[string]any{"job_id": job.ID},
	}
}
