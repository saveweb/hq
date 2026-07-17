package sqlitequeue

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func (s *Store) ClaimBatch(
	ctx context.Context,
	generation, now int64,
	sessionID string,
	acceptTypes []string,
	limit int,
	leaseSeconds int64,
) ([]queue.ClaimedJob, error) {
	if !queue.ValidateIdentifier(sessionID) {
		return nil, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid session ID"}
	}
	if limit < 1 || limit > 256 {
		return nil, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: "claim limit must be between 1 and 256"}
	}
	if leaseSeconds < 30 || leaseSeconds > 3600 || now < 0 || now > (1<<63-1)-leaseSeconds {
		return nil, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid claim time or lease"}
	}
	types, err := normalizeAcceptTypes(acceptTypes)
	if err != nil {
		return nil, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("sqlitequeue: begin claim: %w", err)
	}
	defer tx.Rollback()
	if err := checkFence(ctx, tx, generation, now); err != nil {
		return nil, err
	}

	query := `SELECT id, url, type, via, hops, attr_json FROM jobs WHERE status = 'todo'`
	args := make([]any, 0, len(types)+1)
	if len(types) > 0 {
		query += " AND type IN (" + strings.TrimSuffix(strings.Repeat("?,", len(types)), ",") + ")"
		for _, value := range types {
			args = append(args, value)
		}
	}
	query += " ORDER BY id LIMIT ?"
	args = append(args, limit)

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlitequeue: select claim candidates: %w", err)
	}
	var jobs []queue.ClaimedJob
	for rows.Next() {
		job, err := scanUnclaimedJob(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("sqlitequeue: close claim candidates: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlitequeue: iterate claim candidates: %w", err)
	}

	leaseExpiresAt := now + leaseSeconds
	for i := range jobs {
		attemptID, err := newAttemptID()
		if err != nil {
			return nil, err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE jobs
			SET status = 'wip', session_id = ?, attempt_id = ?, claim_generation = ?,
				lease_expires_at = ?, updated_at = ?
			WHERE id = ? AND status = 'todo'`,
			sessionID, attemptID, generation, leaseExpiresAt, now, jobs[i].ID)
		if err != nil {
			return nil, fmt.Errorf("sqlitequeue: claim job %q: %w", jobs[i].ID, err)
		}
		count, err := result.RowsAffected()
		if err != nil || count != 1 {
			return nil, fmt.Errorf("sqlitequeue: claim invariant failed for %q", jobs[i].ID)
		}
		jobs[i].AttemptID = attemptID
		jobs[i].LeaseExpiresAt = leaseExpiresAt
	}
	if err := s.checkBeforeCommit(ctx, tx, generation, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sqlitequeue: commit claim: %w", err)
	}
	if jobs == nil {
		jobs = []queue.ClaimedJob{}
	}
	return jobs, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanUnclaimedJob(row rowScanner) (queue.ClaimedJob, error) {
	var job queue.ClaimedJob
	var via sql.NullString
	var attrs string
	if err := row.Scan(&job.ID, &job.URL, &job.Type, &via, &job.Hops, &attrs); err != nil {
		return queue.ClaimedJob{}, fmt.Errorf("sqlitequeue: scan job: %w", err)
	}
	if via.Valid {
		job.Via = &via.String
	}
	decoder := json.NewDecoder(bytes.NewBufferString(attrs))
	decoder.UseNumber()
	if err := decoder.Decode(&job.Attrs); err != nil {
		return queue.ClaimedJob{}, fmt.Errorf("sqlitequeue: decode attrs for %q: %w", job.ID, err)
	}
	return job, nil
}

func normalizeAcceptTypes(values []string) ([]string, error) {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != protocol.JobTypeSeed && value != protocol.JobTypeAsset {
			return nil, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: "accept_types contains an unsupported job type"}
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result, nil
}
