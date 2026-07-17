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

type normalizedFail struct {
	item        queue.FailItem
	errorJSON   string
	payloadHash []byte
}

func (s *Store) FailBatch(
	ctx context.Context,
	generation, now int64,
	sessionID string,
	maxResets int,
	items []queue.FailItem,
) ([]queue.ItemResult, error) {
	if !queue.ValidateIdentifier(sessionID) || maxResets < 0 || len(items) < 1 || len(items) > 256 || now < 0 {
		return nil, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid fail batch"}
	}
	normalized := make([]normalizedFail, len(items))
	validationErrors := make([]*queue.Error, len(items))
	for i, item := range items {
		value, err := normalizeFail(item)
		if err != nil {
			validationErrors[i] = err
			continue
		}
		normalized[i] = value
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("sqlitequeue: begin fail: %w", err)
	}
	defer tx.Rollback()
	if err := checkFence(ctx, tx, generation, now); err != nil {
		return nil, err
	}

	results := make([]queue.ItemResult, len(items))
	for i, item := range normalized {
		if validationErrors[i] != nil {
			original := items[i]
			results[i] = rejected(original.JobID, original.AttemptID, validationErrors[i])
			continue
		}
		result, err := failOne(ctx, tx, generation, now, sessionID, maxResets, item)
		if err != nil {
			return nil, err
		}
		results[i] = result
	}
	if err := s.checkBeforeCommit(ctx, tx, generation, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sqlitequeue: commit fail: %w", err)
	}
	return results, nil
}

func normalizeFail(item queue.FailItem) (normalizedFail, *queue.Error) {
	if !queue.ValidateIdentifier(item.JobID) || !queue.ValidateIdentifier(item.AttemptID) {
		return normalizedFail{}, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid job or attempt ID"}
	}
	executionError, errorJSON, err := queue.NormalizeExecutionError(item.Error)
	if err != nil {
		return normalizedFail{}, err
	}
	hashInput := struct {
		Retryable bool            `json:"retryable"`
		Error     json.RawMessage `json:"error"`
	}{item.Retryable, json.RawMessage(errorJSON)}
	hash, hashErr := queue.PayloadHash(hashInput)
	if hashErr != nil {
		return normalizedFail{}, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: "fail payload is not JSON encodable"}
	}
	item.Error = executionError
	return normalizedFail{item: item, errorJSON: errorJSON, payloadHash: hash}, nil
}

func failOne(
	ctx context.Context,
	tx *sql.Tx,
	generation, now int64,
	sessionID string,
	maxResets int,
	item normalizedFail,
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
			if state.finalOperation.Valid && state.finalOperation.String == "fail" &&
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

	nextReset := state.resetCount
	if item.item.Retryable {
		nextReset++
	}
	jobStatus := queue.StatusFailed
	var statement string
	var args []any
	if item.item.Retryable && nextReset <= maxResets {
		jobStatus = queue.StatusTodo
		statement = `UPDATE jobs SET
			status = 'todo', session_id = NULL, attempt_id = NULL,
			claim_generation = NULL, lease_expires_at = NULL, reset_count = ?,
			error_json = ?, final_operation = NULL, final_payload_hash = NULL, updated_at = ?
			WHERE id = ? AND status = 'wip' AND session_id = ? AND attempt_id = ?
				AND claim_generation = ? AND lease_expires_at > ?`
		args = []any{nextReset, item.errorJSON, now, item.item.JobID, sessionID, item.item.AttemptID, generation, now}
	} else {
		if item.item.Retryable {
			jobStatus = queue.StatusResetExhausted
		}
		statement = `UPDATE jobs SET
			status = ?, reset_count = ?, error_json = ?, final_operation = 'fail',
			final_payload_hash = ?, updated_at = ?
			WHERE id = ? AND status = 'wip' AND session_id = ? AND attempt_id = ?
				AND claim_generation = ? AND lease_expires_at > ?`
		args = []any{jobStatus, nextReset, item.errorJSON, item.payloadHash, now, item.item.JobID,
			sessionID, item.item.AttemptID, generation, now}
	}
	result, err := tx.ExecContext(ctx, statement, args...)
	if err != nil {
		return queue.ItemResult{}, fmt.Errorf("sqlitequeue: fail job %q: %w", item.item.JobID, err)
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return queue.ItemResult{}, fmt.Errorf("sqlitequeue: fail invariant failed for %q", item.item.JobID)
	}
	return appliedResult(item.item.JobID, item.item.AttemptID, queue.ResultApplied, jobStatus, nil), nil
}
