package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const maxProjectBatch = 256

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type storedJobSpec struct {
	Type  string         `json:"type,omitempty"`
	Via   *string        `json:"via,omitempty"`
	Hops  int            `json:"hops,omitempty"`
	Attrs map[string]any `json:"attr,omitempty"`
}

func (s *Store) EnqueueProjectJobs(ctx context.Context, projectID string, jobs []protocol.JobSpecV1, now int64) (int64, error) {
	if !queue.ValidateIdentifier(projectID) || len(jobs) == 0 || len(jobs) > 100_000 {
		return 0, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid project or job batch"}
	}
	var inserted int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var status, identityMode string
		if err := tx.QueryRow(ctx, `SELECT status,identity_mode FROM tracker_projects WHERE id=$1`, projectID).Scan(&status, &identityMode); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return &tracker.Error{Code: protocol.ErrorNotFound, Message: "project not found"}
			}
			return err
		}
		if status == tracker.ProjectStatusArchived {
			return &tracker.Error{Code: protocol.ErrorProjectNotActive, Message: "archived project cannot accept jobs"}
		}
		for _, job := range jobs {
			normalized, validationError := queue.NormalizeJob(queue.JobSpec{ID: job.ID, Value: job.Value, Type: job.Type, Via: job.Via, Hops: job.Hops, Attrs: job.Attrs})
			if validationError != nil {
				return &tracker.Error{Code: protocol.ErrorInvalidJob, Message: validationError.Message}
			}
			if identityMode == tracker.IdentityModeExternalID && normalized.ID == "" {
				return &tracker.Error{Code: protocol.ErrorInvalidJob, Message: "id is required for external_id projects"}
			}
			if identityMode != tracker.IdentityModeExternalID && normalized.ID != "" {
				return &tracker.Error{Code: protocol.ErrorInvalidJob, Message: "id is allowed only for external_id projects"}
			}
			spec, err := json.Marshal(storedJobSpec{
				Type: normalized.Type, Via: normalized.Via, Hops: normalized.Hops, Attrs: normalized.Attrs,
			})
			if err != nil {
				return err
			}
			var tag pgconn.CommandTag
			var digest []byte
			switch identityMode {
			case tracker.IdentityModeNone:
				tag, err = tx.Exec(ctx, `INSERT INTO tracker_jobs(project_id,value,spec,status,created_at,updated_at) VALUES ($1,$2,$3,'todo',$4,$4)`, projectID, normalized.Value, spec, now)
			case tracker.IdentityModeExternalID:
				tag, err = tx.Exec(ctx, `
					INSERT INTO tracker_jobs(project_id,external_id,value,spec,status,created_at,updated_at)
					VALUES ($1,$2,$3,$4,'todo',$5,$5)
					ON CONFLICT (project_id,external_id) WHERE external_id IS NOT NULL DO NOTHING
				`, projectID, normalized.ID, normalized.Value, spec, now)
			case tracker.IdentityModeUniqueValue:
				sum := sha256.Sum256([]byte(normalized.Value))
				digest = sum[:]
				tag, err = tx.Exec(ctx, `
					INSERT INTO tracker_jobs(project_id,value,unique_value_digest,spec,status,created_at,updated_at)
					VALUES ($1,$2,$3,$4,'todo',$5,$5)
					ON CONFLICT (project_id,unique_value_digest) WHERE unique_value_digest IS NOT NULL DO NOTHING
				`, projectID, normalized.Value, digest, spec, now)
			default:
				return fmt.Errorf("unknown project identity mode %q", identityMode)
			}
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				var existingValue string
				var existingSpec []byte
				var row pgx.Row
				if identityMode == tracker.IdentityModeExternalID {
					row = tx.QueryRow(ctx, `SELECT value,spec FROM tracker_jobs WHERE project_id=$1 AND external_id=$2`, projectID, normalized.ID)
				} else {
					row = tx.QueryRow(ctx, `SELECT value,spec FROM tracker_jobs WHERE project_id=$1 AND unique_value_digest=$2`, projectID, digest)
				}
				if err := row.Scan(&existingValue, &existingSpec); err != nil {
					return err
				}
				if identityMode == tracker.IdentityModeUniqueValue && existingValue != normalized.Value {
					return &tracker.Error{Code: protocol.ErrorIdentityConflict, Message: "value digest collision"}
				}
				equalSpec, err := equalJSON(existingSpec, spec)
				if err != nil {
					return err
				}
				if existingValue != normalized.Value || !equalSpec {
					return &tracker.Error{Code: protocol.ErrorIdentityConflict, Message: "job identity already exists with a different spec"}
				}
			}
			inserted += tag.RowsAffected()
		}
		return nil
	})
	return inserted, storeError("enqueue project jobs", err)
}

func (s *Store) ClaimProjectJobs(ctx context.Context, userID, projectID string, request protocol.ProjectClaimRequest, now int64) ([]protocol.ClaimedJob, error) {
	if !queue.ValidateIdentifier(projectID) || !queue.ValidateIdentifier(request.WorkerID) || request.MaxJobs < 1 || request.MaxJobs > maxProjectBatch || request.LeaseSeconds < 1 || request.LeaseSeconds > 3600 || len(request.AcceptTypes) > 16 {
		return nil, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid claim request"}
	}
	for _, jobType := range request.AcceptTypes {
		if jobType != protocol.JobTypeSeed && jobType != protocol.JobTypeAsset {
			return nil, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "accept_types may contain only seed or asset"}
		}
	}
	leaseExpiresAt := now + request.LeaseSeconds
	result := make([]protocol.ClaimedJob, 0, request.MaxJobs)
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := authorizeProjectWorker(ctx, tx, userID, projectID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE tracker_jobs SET
				status=CASE WHEN reset_count + 1 > 3 THEN 'reset_exhausted' ELSE 'todo' END,
				reset_count=reset_count+1, attempt_id=NULL, worker_id=NULL, lease_expires_at=NULL, updated_at=$2
			WHERE project_id=$1 AND status='wip' AND lease_expires_at <= $2
		`, projectID, now); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT job_id,external_id,value,spec FROM tracker_jobs
			WHERE project_id=$1 AND status='todo'
				AND (COALESCE(cardinality($2::text[]), 0) = 0 OR COALESCE(spec->>'type','seed') = ANY($2::text[]))
			ORDER BY created_at,job_id
			FOR UPDATE SKIP LOCKED LIMIT $3
		`, projectID, request.AcceptTypes, request.MaxJobs)
		if err != nil {
			return err
		}
		defer rows.Close()
		type selected struct {
			jobID      int64
			externalID *string
			value      string
			spec       []byte
		}
		selectedJobs := make([]selected, 0, request.MaxJobs)
		for rows.Next() {
			var item selected
			if err := rows.Scan(&item.jobID, &item.externalID, &item.value, &item.spec); err != nil {
				return err
			}
			selectedJobs = append(selectedJobs, item)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, selectedJob := range selectedJobs {
			attemptID, err := newID("at_")
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE tracker_jobs SET status='wip',attempt_id=$3,worker_id=$4,lease_expires_at=$5,updated_at=$6 WHERE project_id=$1 AND job_id=$2`, projectID, selectedJob.jobID, attemptID, request.WorkerID, leaseExpiresAt, now); err != nil {
				return err
			}
			var stored storedJobSpec
			if err := json.Unmarshal(selectedJob.spec, &stored); err != nil {
				return err
			}
			spec := protocol.JobSpecV1{Value: selectedJob.value, Type: stored.Type, Via: stored.Via, Hops: stored.Hops, Attrs: stored.Attrs}
			if spec.Type == "" {
				spec.Type = protocol.JobTypeSeed
			}
			if selectedJob.externalID != nil {
				spec.ID = *selectedJob.externalID
			}
			result = append(result, protocol.ClaimedJob{JobSpecV1: spec, JobID: selectedJob.jobID, AttemptID: attemptID, LeaseExpiresAt: leaseExpiresAt})
		}
		return nil
	})
	return result, storeError("claim project jobs", err)
}

func authorizeProjectWorker(ctx context.Context, tx pgx.Tx, userID, projectID string) error {
	var userStatus, projectStatus string
	var roles []string
	err := tx.QueryRow(ctx, `SELECT u.status,u.roles,p.status FROM tracker_users u CROSS JOIN tracker_projects p WHERE u.id=$1 AND p.id=$2`, userID, projectID).Scan(&userStatus, &roles, &projectStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return &tracker.Error{Code: protocol.ErrorNotFound, Message: "user or project not found"}
	}
	if err != nil {
		return err
	}
	if userStatus != tracker.UserStatusActive || !roleMap(roles)[tracker.RoleWorker] {
		return &tracker.Error{Code: protocol.ErrorPermissionDenied, Message: "active worker role required"}
	}
	if projectStatus != tracker.ProjectStatusActive {
		return &tracker.Error{Code: protocol.ErrorProjectNotActive, Message: "project is not active"}
	}
	return nil
}

func (s *Store) CompleteProjectJobs(ctx context.Context, userID, projectID string, request protocol.ProjectCompleteRequest, now int64) (protocol.BatchResultResponse, error) {
	if !queue.ValidateIdentifier(projectID) || !queue.ValidateIdentifier(request.WorkerID) || len(request.Items) == 0 || len(request.Items) > maxProjectBatch {
		return protocol.BatchResultResponse{}, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid completion batch"}
	}
	return s.finishProjectJobs(ctx, userID, projectID, now, func(tx pgx.Tx) ([]protocol.ItemResult, error) {
		results := make([]protocol.ItemResult, 0, len(request.Items))
		for _, item := range request.Items {
			if item.JobID < 1 || !queue.ValidateIdentifier(item.AttemptID) {
				return nil, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid job or attempt ID"}
			}
			if _, _, validationError := queue.NormalizeOutcome(queue.Outcome{Kind: item.Outcome.Kind, Code: item.Outcome.Code, URI: item.Outcome.URI, Meta: item.Outcome.Meta}); validationError != nil {
				return nil, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: validationError.Message}
			}
			if err := validateWARCReceipts(item.WARCReceipts); err != nil {
				return nil, err
			}
			outcome, err := json.Marshal(item.Outcome)
			if err != nil {
				return nil, err
			}
			receipts, err := json.Marshal(item.WARCReceipts)
			if err != nil {
				return nil, err
			}
			tag, err := tx.Exec(ctx, `UPDATE tracker_jobs SET status='done',attempt_id=NULL,worker_id=NULL,lease_expires_at=NULL,outcome=$6,warc_receipts=$7,execution_error=NULL,updated_at=$5,completed_at=$5 WHERE project_id=$1 AND job_id=$2 AND status='wip' AND attempt_id=$3 AND worker_id=$4 AND lease_expires_at>$5`, projectID, item.JobID, item.AttemptID, request.WorkerID, now, outcome, receipts)
			if err != nil {
				return nil, err
			}
			results = append(results, projectItemResult(item.JobID, item.AttemptID, tag.RowsAffected(), protocol.JobStatusDone))
		}
		return results, nil
	})
}

func (s *Store) FailProjectJobs(ctx context.Context, userID, projectID string, request protocol.ProjectFailRequest, now int64) (protocol.BatchResultResponse, error) {
	if !queue.ValidateIdentifier(projectID) || !queue.ValidateIdentifier(request.WorkerID) || len(request.Items) == 0 || len(request.Items) > maxProjectBatch {
		return protocol.BatchResultResponse{}, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid failure batch"}
	}
	return s.finishProjectJobs(ctx, userID, projectID, now, func(tx pgx.Tx) ([]protocol.ItemResult, error) {
		results := make([]protocol.ItemResult, 0, len(request.Items))
		for _, item := range request.Items {
			if item.JobID < 1 || !queue.ValidateIdentifier(item.AttemptID) {
				return nil, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid job or attempt ID"}
			}
			if _, _, validationError := queue.NormalizeExecutionError(queue.ExecutionError{Code: item.Error.Code, Message: item.Error.Message, Details: item.Error.Details}); validationError != nil {
				return nil, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: validationError.Message}
			}
			executionError, err := json.Marshal(item.Error)
			if err != nil {
				return nil, err
			}
			status := protocol.JobStatusFailed
			if item.Retryable {
				status = protocol.JobStatusTodo
			}
			var actualStatus string
			err = tx.QueryRow(ctx, `UPDATE tracker_jobs SET status=CASE WHEN $8 AND reset_count + 1 > 3 THEN 'reset_exhausted' ELSE $6 END,attempt_id=NULL,worker_id=NULL,lease_expires_at=NULL,execution_error=$7,reset_count=reset_count+CASE WHEN $8 THEN 1 ELSE 0 END,updated_at=$5,completed_at=CASE WHEN $8 AND reset_count + 1 <= 3 THEN NULL ELSE $5 END WHERE project_id=$1 AND job_id=$2 AND status='wip' AND attempt_id=$3 AND worker_id=$4 AND lease_expires_at>$5 RETURNING status`, projectID, item.JobID, item.AttemptID, request.WorkerID, now, status, executionError, item.Retryable).Scan(&actualStatus)
			if errors.Is(err, pgx.ErrNoRows) {
				results = append(results, projectItemResult(item.JobID, item.AttemptID, 0, status))
				continue
			}
			if err != nil {
				return nil, err
			}
			results = append(results, projectItemResult(item.JobID, item.AttemptID, 1, actualStatus))
		}
		return results, nil
	})
}

func (s *Store) ExtendProjectJobLeases(ctx context.Context, userID, projectID string, request protocol.ProjectExtendLeaseRequest, now int64) (protocol.BatchResultResponse, error) {
	if !queue.ValidateIdentifier(projectID) || !queue.ValidateIdentifier(request.WorkerID) || len(request.Items) == 0 || len(request.Items) > maxProjectBatch || request.ExtendSeconds < 1 || request.ExtendSeconds > 3600 {
		return protocol.BatchResultResponse{}, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid lease extension batch"}
	}
	return s.finishProjectJobs(ctx, userID, projectID, now, func(tx pgx.Tx) ([]protocol.ItemResult, error) {
		results := make([]protocol.ItemResult, 0, len(request.Items))
		extended := now + request.ExtendSeconds
		for _, item := range request.Items {
			if item.JobID < 1 || !queue.ValidateIdentifier(item.AttemptID) {
				return nil, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid job or attempt ID"}
			}
			tag, err := tx.Exec(ctx, `UPDATE tracker_jobs SET lease_expires_at=GREATEST(lease_expires_at,$6),updated_at=$5 WHERE project_id=$1 AND job_id=$2 AND status='wip' AND attempt_id=$3 AND worker_id=$4 AND lease_expires_at>$5`, projectID, item.JobID, item.AttemptID, request.WorkerID, now, extended)
			if err != nil {
				return nil, err
			}
			result := projectItemResult(item.JobID, item.AttemptID, tag.RowsAffected(), protocol.JobStatusWIP)
			if tag.RowsAffected() == 1 {
				result.LeaseExpiresAt = &extended
			}
			results = append(results, result)
		}
		return results, nil
	})
}

func (s *Store) finishProjectJobs(ctx context.Context, userID, projectID string, now int64, fn func(pgx.Tx) ([]protocol.ItemResult, error)) (protocol.BatchResultResponse, error) {
	var response protocol.BatchResultResponse
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := authorizeProjectWorker(ctx, tx, userID, projectID); err != nil {
			return err
		}
		results, err := fn(tx)
		response.Results = results
		return err
	})
	return response, storeError("update project jobs", err)
}

func projectItemResult(jobID int64, attemptID string, affected int64, status string) protocol.ItemResult {
	if affected == 1 {
		return protocol.ItemResult{JobID: jobID, AttemptID: attemptID, Status: protocol.ItemStatusApplied, JobStatus: &status}
	}
	return protocol.ItemResult{JobID: jobID, AttemptID: attemptID, Status: protocol.ItemStatusRejected, Error: &protocol.APIError{Code: protocol.ErrorStaleAttempt, Message: "attempt is stale, expired, or already finalized"}}
}

func equalJSON(left, right []byte) (bool, error) {
	var leftValue, rightValue any
	if err := json.Unmarshal(left, &leftValue); err != nil {
		return false, fmt.Errorf("decode stored job spec: %w", err)
	}
	if err := json.Unmarshal(right, &rightValue); err != nil {
		return false, fmt.Errorf("decode new job spec: %w", err)
	}
	leftCanonical, _ := json.Marshal(leftValue)
	rightCanonical, _ := json.Marshal(rightValue)
	return bytes.Equal(leftCanonical, rightCanonical), nil
}

func validateWARCReceipts(receipts []protocol.WARCReceipt) error {
	if len(receipts) > 16 {
		return &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "at most 16 WARC receipts are allowed per job"}
	}
	for _, receipt := range receipts {
		if !queue.ValidateIdentifier(receipt.ID) || receipt.Issuer == "" || len(receipt.Issuer) > 512 ||
			receipt.ObjectID == "" || len(receipt.ObjectID) > 1024 || !sha256Pattern.MatchString(receipt.SHA256) ||
			receipt.SizeBytes < 1 || receipt.AcceptedAt < 1 || receipt.Signature == "" || len(receipt.Signature) > 4096 {
			return &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid WARC receipt"}
		}
	}
	return nil
}

func (s *Store) ProjectJobCounts(ctx context.Context, projectID string) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT status,count(*) FROM tracker_jobs WHERE project_id=$1 GROUP BY status`, projectID)
	if err != nil {
		return nil, fmt.Errorf("tracker postgres: job counts: %w", err)
	}
	defer rows.Close()
	result := map[string]int64{}
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		result[status] = count
	}
	return result, rows.Err()
}
