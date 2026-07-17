package sqlitequeue

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

type normalizedComplete struct {
	item            queue.CompleteItem
	outcomeMetaJSON string
	discovered      []queue.NormalizedJob
	payloadHash     []byte
}

func (s *Store) CompleteBatch(
	ctx context.Context,
	generation, now int64,
	sessionID string,
	items []queue.CompleteItem,
) ([]queue.ItemResult, error) {
	if !queue.ValidateIdentifier(sessionID) || len(items) < 1 || len(items) > 256 || now < 0 {
		return nil, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid complete batch"}
	}
	normalized := make([]normalizedComplete, len(items))
	for i, item := range items {
		value, err := normalizeComplete(item)
		if err != nil {
			normalized[i] = normalizedComplete{item: item}
			continue
		}
		normalized[i] = value
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("sqlitequeue: begin complete: %w", err)
	}
	defer tx.Rollback()
	if err := checkFence(ctx, tx, generation, now); err != nil {
		return nil, err
	}

	results := make([]queue.ItemResult, len(items))
	for i, item := range normalized {
		name := fmt.Sprintf("complete_%d", i)
		if err := beginSavepoint(ctx, tx, name); err != nil {
			return nil, err
		}
		if item.payloadHash == nil {
			_, validationError := normalizeComplete(item.item)
			results[i] = rejected(item.item.JobID, item.item.AttemptID, validationError)
			if err := rollbackSavepoint(ctx, tx, name); err != nil {
				return nil, err
			}
			continue
		}
		result, err := completeOne(ctx, tx, generation, now, sessionID, item)
		if err != nil {
			return nil, err
		}
		results[i] = result
		if result.Status == queue.ResultRejected {
			if err := rollbackSavepoint(ctx, tx, name); err != nil {
				return nil, err
			}
		} else if err := releaseSavepoint(ctx, tx, name); err != nil {
			return nil, err
		}
	}
	if err := s.checkBeforeCommit(ctx, tx, generation, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sqlitequeue: commit complete: %w", err)
	}
	return results, nil
}

func normalizeComplete(item queue.CompleteItem) (normalizedComplete, *queue.Error) {
	if !queue.ValidateIdentifier(item.JobID) || !queue.ValidateIdentifier(item.AttemptID) {
		return normalizedComplete{}, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid job or attempt ID"}
	}
	if len(item.DiscoveredJobs) > 256 {
		return normalizedComplete{}, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: "too many discovered jobs"}
	}
	outcome, metaJSON, err := queue.NormalizeOutcome(item.Outcome)
	if err != nil {
		return normalizedComplete{}, err
	}
	discovered := make([]queue.NormalizedJob, len(item.DiscoveredJobs))
	for i, job := range item.DiscoveredJobs {
		value, err := queue.NormalizeJob(job)
		if err != nil {
			return normalizedComplete{}, err
		}
		discovered[i] = value
	}
	hashInput := struct {
		Outcome struct {
			Kind string          `json:"kind"`
			Code *int            `json:"code"`
			URI  *string         `json:"uri"`
			Meta json.RawMessage `json:"meta"`
		} `json:"outcome"`
		Discovered []wireJob `json:"discovered_jobs"`
	}{}
	hashInput.Outcome.Kind = outcome.Kind
	hashInput.Outcome.Code = outcome.Code
	hashInput.Outcome.URI = outcome.URI
	hashInput.Outcome.Meta = json.RawMessage(metaJSON)
	hashInput.Discovered = make([]wireJob, len(discovered))
	for i, job := range discovered {
		hashInput.Discovered[i] = toWireJob(job)
	}
	hash, hashErr := queue.PayloadHash(hashInput)
	if hashErr != nil {
		return normalizedComplete{}, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: "complete payload is not JSON encodable"}
	}
	item.Outcome = outcome
	return normalizedComplete{
		item:            item,
		outcomeMetaJSON: metaJSON,
		discovered:      discovered,
		payloadHash:     hash,
	}, nil
}

type wireJob struct {
	ID    string          `json:"id"`
	URL   string          `json:"url"`
	Type  string          `json:"type"`
	Via   *string         `json:"via"`
	Hops  int             `json:"hops"`
	Attrs json.RawMessage `json:"attr"`
}

func toWireJob(job queue.NormalizedJob) wireJob {
	return wireJob{
		ID: job.ID, URL: job.URL, Type: job.Type, Via: job.Via, Hops: job.Hops,
		Attrs: json.RawMessage(job.AttrsJSON),
	}
}

func completeOne(
	ctx context.Context,
	tx *sql.Tx,
	generation, now int64,
	sessionID string,
	item normalizedComplete,
) (queue.ItemResult, error) {
	state, err := readAttemptState(ctx, tx, item.item.JobID)
	if err == sql.ErrNoRows {
		return rejected(item.item.JobID, item.item.AttemptID, &queue.Error{
			Code: protocol.ErrorNotFound, Message: "job not found",
		}), nil
	}
	if err != nil {
		return queue.ItemResult{}, err
	}
	if state.status != queue.StatusWIP {
		if state.attemptID.Valid && state.attemptID.String == item.item.AttemptID {
			if state.finalOperation.Valid && state.finalOperation.String == "complete" &&
				bytes.Equal(state.finalPayloadHash, item.payloadHash) {
				return appliedResult(item.item.JobID, item.item.AttemptID, queue.ResultAlreadyApplied, state.status, nil), nil
			}
			return rejected(item.item.JobID, item.item.AttemptID, &queue.Error{
				Code: protocol.ErrorAttemptAlreadyFinalized, Message: "attempt already has a different final result",
			}), nil
		}
		return rejected(item.item.JobID, item.item.AttemptID, staleAttempt()), nil
	}
	if !state.attemptID.Valid || state.attemptID.String != item.item.AttemptID ||
		!state.sessionID.Valid || state.sessionID.String != sessionID {
		return rejected(item.item.JobID, item.item.AttemptID, staleAttempt()), nil
	}
	if !state.claimGeneration.Valid || state.claimGeneration.Int64 != generation {
		return rejected(item.item.JobID, item.item.AttemptID, staleGeneration(state.claimGeneration.Int64)), nil
	}
	if !state.leaseExpiresAt.Valid || now >= state.leaseExpiresAt.Int64 {
		return rejected(item.item.JobID, item.item.AttemptID, &queue.Error{
			Code: protocol.ErrorLeaseExpired, Message: "job lease expired",
		}), nil
	}
	for _, discovered := range item.discovered {
		if _, err := enqueueOne(ctx, tx, now, discovered); err != nil {
			if domainError, ok := asQueueError(err); ok {
				return rejected(item.item.JobID, item.item.AttemptID, domainError), nil
			}
			return queue.ItemResult{}, err
		}
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = 'done', outcome_kind = ?, outcome_code = ?, outcome_uri = ?,
			outcome_meta_json = ?, error_json = NULL, final_operation = 'complete',
			final_payload_hash = ?, updated_at = ?
		WHERE id = ? AND status = 'wip' AND session_id = ? AND attempt_id = ?
			AND claim_generation = ? AND lease_expires_at > ?`,
		item.item.Outcome.Kind, nullableInt(item.item.Outcome.Code), nullableString(item.item.Outcome.URI),
		item.outcomeMetaJSON, item.payloadHash, now, item.item.JobID, sessionID,
		item.item.AttemptID, generation, now)
	if err != nil {
		return queue.ItemResult{}, fmt.Errorf("sqlitequeue: complete job %q: %w", item.item.JobID, err)
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return queue.ItemResult{}, fmt.Errorf("sqlitequeue: complete invariant failed for %q", item.item.JobID)
	}
	return appliedResult(item.item.JobID, item.item.AttemptID, queue.ResultApplied, queue.StatusDone, nil), nil
}

type attemptState struct {
	status           string
	sessionID        sql.NullString
	attemptID        sql.NullString
	claimGeneration  sql.NullInt64
	leaseExpiresAt   sql.NullInt64
	finalOperation   sql.NullString
	finalPayloadHash []byte
	resetCount       int
}

func readAttemptState(ctx context.Context, tx *sql.Tx, jobID string) (attemptState, error) {
	var state attemptState
	err := tx.QueryRowContext(ctx, `
		SELECT status, session_id, attempt_id, claim_generation, lease_expires_at,
			final_operation, final_payload_hash, reset_count
		FROM jobs WHERE id = ?`, jobID).Scan(
		&state.status, &state.sessionID, &state.attemptID, &state.claimGeneration,
		&state.leaseExpiresAt, &state.finalOperation, &state.finalPayloadHash, &state.resetCount,
	)
	if err != nil && err != sql.ErrNoRows {
		return attemptState{}, fmt.Errorf("sqlitequeue: read attempt %q: %w", jobID, err)
	}
	return state, err
}

func staleAttempt() *queue.Error {
	return &queue.Error{Code: protocol.ErrorStaleAttempt, Message: "attempt is no longer current"}
}

func rejected(jobID, attemptID string, err *queue.Error) queue.ItemResult {
	return queue.ItemResult{
		JobID: jobID, AttemptID: attemptID, Status: queue.ResultRejected, Error: err,
	}
}

func appliedResult(jobID, attemptID, status, jobStatus string, lease *int64) queue.ItemResult {
	return queue.ItemResult{
		JobID: jobID, AttemptID: attemptID, Status: status, JobStatus: jobStatus,
		LeaseExpiresAt: lease,
	}
}

func beginSavepoint(ctx context.Context, tx *sql.Tx, name string) error {
	if _, err := tx.ExecContext(ctx, "SAVEPOINT "+name); err != nil {
		return fmt.Errorf("sqlitequeue: begin savepoint: %w", err)
	}
	return nil
}

func rollbackSavepoint(ctx context.Context, tx *sql.Tx, name string) error {
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO "+name); err != nil {
		return fmt.Errorf("sqlitequeue: rollback savepoint: %w", err)
	}
	return releaseSavepoint(ctx, tx, name)
}

func releaseSavepoint(ctx context.Context, tx *sql.Tx, name string) error {
	if _, err := tx.ExecContext(ctx, "RELEASE "+name); err != nil {
		return fmt.Errorf("sqlitequeue: release savepoint: %w", err)
	}
	return nil
}

func nullableInt(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}
