package sqlitequeue

import (
	"context"
	"database/sql"
	"fmt"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func (s *Store) ExtendLeaseBatch(
	ctx context.Context,
	generation, now int64,
	sessionID string,
	extendSeconds int64,
	items []queue.AttemptRef,
) ([]queue.ItemResult, error) {
	if !queue.ValidateIdentifier(sessionID) || extendSeconds < 30 || extendSeconds > 3600 ||
		len(items) < 1 || len(items) > 256 || now < 0 || now > (1<<63-1)-extendSeconds {
		return nil, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid extend-lease batch"}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("sqlitequeue: begin extend lease: %w", err)
	}
	defer tx.Rollback()
	if err := checkFence(ctx, tx, generation, now); err != nil {
		return nil, err
	}

	results := make([]queue.ItemResult, len(items))
	for i, item := range items {
		if !queue.ValidateIdentifier(item.JobID) || !queue.ValidateIdentifier(item.AttemptID) {
			results[i] = rejected(item.JobID, item.AttemptID, &queue.Error{
				Code: protocol.ErrorInvalidRequest, Message: "invalid job or attempt ID",
			})
			continue
		}
		state, err := readAttemptState(ctx, tx, item.JobID)
		if err == sql.ErrNoRows {
			results[i] = rejected(item.JobID, item.AttemptID, &queue.Error{Code: protocol.ErrorNotFound, Message: "job not found"})
			continue
		}
		if err != nil {
			return nil, err
		}
		if state.status != queue.StatusWIP || !state.attemptID.Valid || state.attemptID.String != item.AttemptID ||
			!state.sessionID.Valid || state.sessionID.String != sessionID {
			results[i] = rejected(item.JobID, item.AttemptID, staleAttempt())
			continue
		}
		if !state.claimGeneration.Valid || state.claimGeneration.Int64 != generation {
			results[i] = rejected(item.JobID, item.AttemptID, staleGeneration(state.claimGeneration.Int64))
			continue
		}
		if !state.leaseExpiresAt.Valid || now >= state.leaseExpiresAt.Int64 {
			results[i] = rejected(item.JobID, item.AttemptID, &queue.Error{Code: protocol.ErrorLeaseExpired, Message: "job lease expired"})
			continue
		}
		newLease := max(state.leaseExpiresAt.Int64, now+extendSeconds)
		result, err := tx.ExecContext(ctx, `
			UPDATE jobs SET lease_expires_at = ?, updated_at = ?
			WHERE id = ? AND status = 'wip' AND session_id = ? AND attempt_id = ?
				AND claim_generation = ? AND lease_expires_at > ?`,
			newLease, now, item.JobID, sessionID, item.AttemptID, generation, now)
		if err != nil {
			return nil, fmt.Errorf("sqlitequeue: extend lease for %q: %w", item.JobID, err)
		}
		count, err := result.RowsAffected()
		if err != nil || count != 1 {
			return nil, fmt.Errorf("sqlitequeue: extend lease invariant failed for %q", item.JobID)
		}
		lease := newLease
		results[i] = appliedResult(item.JobID, item.AttemptID, queue.ResultApplied, queue.StatusWIP, &lease)
	}
	if err := s.checkBeforeCommit(ctx, tx, generation, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sqlitequeue: commit extend lease: %w", err)
	}
	return results, nil
}

func (s *Store) RequeueExpired(
	ctx context.Context,
	generation, now int64,
	maxResets, limit int,
) (queue.RequeueResult, error) {
	if maxResets < 0 || limit < 1 || limit > 10000 || now < 0 {
		return queue.RequeueResult{}, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid requeue request"}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return queue.RequeueResult{}, fmt.Errorf("sqlitequeue: begin requeue: %w", err)
	}
	defer tx.Rollback()
	if err := checkFence(ctx, tx, generation, now); err != nil {
		return queue.RequeueResult{}, err
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT id, reset_count FROM jobs
		WHERE status = 'wip' AND lease_expires_at <= ?
		ORDER BY lease_expires_at, id LIMIT ?`, now, limit)
	if err != nil {
		return queue.RequeueResult{}, fmt.Errorf("sqlitequeue: select expired jobs: %w", err)
	}
	type expiredJob struct {
		id         string
		resetCount int
	}
	var expired []expiredJob
	for rows.Next() {
		var job expiredJob
		if err := rows.Scan(&job.id, &job.resetCount); err != nil {
			rows.Close()
			return queue.RequeueResult{}, fmt.Errorf("sqlitequeue: scan expired job: %w", err)
		}
		expired = append(expired, job)
	}
	if err := rows.Close(); err != nil {
		return queue.RequeueResult{}, fmt.Errorf("sqlitequeue: close expired jobs: %w", err)
	}
	if err := rows.Err(); err != nil {
		return queue.RequeueResult{}, fmt.Errorf("sqlitequeue: iterate expired jobs: %w", err)
	}

	var result queue.RequeueResult
	for _, job := range expired {
		nextReset := job.resetCount + 1
		if nextReset > maxResets {
			updated, err := tx.ExecContext(ctx, `
				UPDATE jobs SET status = 'reset_exhausted', reset_count = ?, updated_at = ?
				WHERE id = ? AND status = 'wip' AND lease_expires_at <= ?`,
				nextReset, now, job.id, now)
			if err != nil {
				return queue.RequeueResult{}, fmt.Errorf("sqlitequeue: exhaust expired job %q: %w", job.id, err)
			}
			if count, _ := updated.RowsAffected(); count != 1 {
				return queue.RequeueResult{}, fmt.Errorf("sqlitequeue: requeue invariant failed for %q", job.id)
			}
			result.ResetExhausted++
			continue
		}
		updated, err := tx.ExecContext(ctx, `
			UPDATE jobs SET status = 'todo', session_id = NULL, attempt_id = NULL,
				claim_generation = NULL, lease_expires_at = NULL, reset_count = ?, updated_at = ?
			WHERE id = ? AND status = 'wip' AND lease_expires_at <= ?`,
			nextReset, now, job.id, now)
		if err != nil {
			return queue.RequeueResult{}, fmt.Errorf("sqlitequeue: requeue expired job %q: %w", job.id, err)
		}
		if count, _ := updated.RowsAffected(); count != 1 {
			return queue.RequeueResult{}, fmt.Errorf("sqlitequeue: requeue invariant failed for %q", job.id)
		}
		result.Requeued++
	}
	if err := s.checkBeforeCommit(ctx, tx, generation, now); err != nil {
		return queue.RequeueResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return queue.RequeueResult{}, fmt.Errorf("sqlitequeue: commit requeue: %w", err)
	}
	return result, nil
}
