package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/saveweb/hq/internal/queue"
	"github.com/saveweb/hq/internal/tracker"
	"github.com/saveweb/hq/pkg/protocol"
)

const maxProjectBatch = 256

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type storedJobSpec struct {
	Type  string         `json:"type,omitempty"`
	Via   *string        `json:"via,omitempty"`
	Hops  int            `json:"hops,omitempty"`
	Attrs map[string]any `json:"attr,omitempty"`
}

type preparedEnqueueJob struct {
	identity        string
	value           string
	spec            string
	randomKey       int32
	customRandomKey bool
}

func (s *Store) EnqueueProjectJobs(ctx context.Context, projectID string, jobs []protocol.AdminEnqueueJob, now int64) (int64, error) {
	if !queue.ValidateIdentifier(projectID) || len(jobs) == 0 {
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
		prepared, err := prepareEnqueueJobs(jobs, identityMode)
		if err != nil {
			return err
		}
		inserted, err = bulkEnqueueJobs(ctx, tx, projectID, identityMode, prepared, now)
		return err
	})
	return inserted, storeError("enqueue project jobs", err)
}

func prepareEnqueueJobs(jobs []protocol.AdminEnqueueJob, identityMode string) ([]preparedEnqueueJob, error) {
	prepared := make([]preparedEnqueueJob, 0, len(jobs))
	var seen map[string]int
	if identityMode != tracker.IdentityModeNone {
		seen = make(map[string]int, len(jobs))
	}
	for _, job := range jobs {
		normalized, validationError := queue.NormalizeJob(queue.JobSpec{
			ID: job.ID, Value: job.Value, Type: job.Type, Via: job.Via, Hops: job.Hops, Attrs: job.Attrs,
		})
		if validationError != nil {
			return nil, &tracker.Error{Code: protocol.ErrorInvalidJob, Message: validationError.Message}
		}
		if identityMode == tracker.IdentityModeExternalID && normalized.ID == "" {
			return nil, &tracker.Error{Code: protocol.ErrorInvalidJob, Message: "id is required for external_id projects"}
		}
		if identityMode != tracker.IdentityModeExternalID && normalized.ID != "" {
			return nil, &tracker.Error{Code: protocol.ErrorInvalidJob, Message: "id is allowed only for external_id projects"}
		}
		spec, err := json.Marshal(storedJobSpec{
			Type: normalized.Type, Via: normalized.Via, Hops: normalized.Hops, Attrs: normalized.Attrs,
		})
		if err != nil {
			return nil, err
		}
		value := preparedEnqueueJob{value: normalized.Value, spec: string(spec)}
		if job.RandomKey != nil {
			value.randomKey = *job.RandomKey
			value.customRandomKey = true
		}
		switch identityMode {
		case tracker.IdentityModeNone:
			if !value.customRandomKey {
				value.randomKey = newRandomKey()
			}
			prepared = append(prepared, value)
			continue
		case tracker.IdentityModeExternalID:
			value.identity = normalized.ID
		case tracker.IdentityModeUniqueValue:
			sum := sha256.Sum256([]byte(normalized.Value))
			value.identity = hex.EncodeToString(sum[:])
		default:
			return nil, fmt.Errorf("unknown project identity mode %q", identityMode)
		}
		if previousIndex, exists := seen[value.identity]; exists {
			previous := &prepared[previousIndex]
			if identityMode == tracker.IdentityModeUniqueValue && previous.value != value.value {
				return nil, &tracker.Error{Code: protocol.ErrorIdentityConflict, Message: "value digest collision"}
			}
			if previous.value != value.value || previous.spec != value.spec {
				return nil, &tracker.Error{Code: protocol.ErrorIdentityConflict, Message: "job identity already exists with a different spec"}
			}
			if previous.customRandomKey && value.customRandomKey && previous.randomKey != value.randomKey {
				return nil, &tracker.Error{Code: protocol.ErrorIdentityConflict, Message: "job identity has conflicting random keys"}
			}
			if value.customRandomKey && !previous.customRandomKey {
				previous.randomKey = value.randomKey
				previous.customRandomKey = true
			}
			continue
		}
		if !value.customRandomKey {
			value.randomKey = newRandomKey()
		}
		seen[value.identity] = len(prepared)
		prepared = append(prepared, value)
	}
	return prepared, nil
}

func newRandomKey() int32 { return int32(rand.Uint32()) }

func bulkEnqueueJobs(ctx context.Context, tx pgx.Tx, projectID, identityMode string, jobs []preparedEnqueueJob, now int64) (int64, error) {
	identities := make([]string, len(jobs))
	values := make([]string, len(jobs))
	specs := make([]string, len(jobs))
	randomKeys := make([]int32, len(jobs))
	for index, job := range jobs {
		identities[index], values[index], specs[index] = job.identity, job.value, job.spec
		randomKeys[index] = job.randomKey
	}
	var (
		tag pgconn.CommandTag
		err error
	)
	switch identityMode {
	case tracker.IdentityModeNone:
		tag, err = tx.Exec(ctx, `
			INSERT INTO tracker_jobs(project_id,value,spec,random_key,status,created_at,updated_at)
			SELECT $1,input.value,input.spec_text::jsonb,input.random_key,'todo',$5,$5
			FROM unnest($2::text[],$3::text[],$4::integer[]) WITH ORDINALITY AS input(value,spec_text,random_key,ordinal)
			ORDER BY input.ordinal
		`, projectID, values, specs, randomKeys, now)
	case tracker.IdentityModeExternalID:
		tag, err = tx.Exec(ctx, `
			INSERT INTO tracker_jobs(project_id,external_id,value,spec,random_key,status,created_at,updated_at)
			SELECT $1,input.identity,input.value,input.spec_text::jsonb,input.random_key,'todo',$6,$6
			FROM unnest($2::text[],$3::text[],$4::text[],$5::integer[]) WITH ORDINALITY AS input(identity,value,spec_text,random_key,ordinal)
			ORDER BY input.ordinal
			ON CONFLICT (project_id,external_id) WHERE external_id IS NOT NULL DO NOTHING
		`, projectID, identities, values, specs, randomKeys, now)
	case tracker.IdentityModeUniqueValue:
		tag, err = tx.Exec(ctx, `
			INSERT INTO tracker_jobs(project_id,value,unique_value_digest,spec,random_key,status,created_at,updated_at)
			SELECT $1,input.value,decode(input.identity,'hex'),input.spec_text::jsonb,input.random_key,'todo',$6,$6
			FROM unnest($2::text[],$3::text[],$4::text[],$5::integer[]) WITH ORDINALITY AS input(identity,value,spec_text,random_key,ordinal)
			ORDER BY input.ordinal
			ON CONFLICT (project_id,unique_value_digest) WHERE unique_value_digest IS NOT NULL DO NOTHING
		`, projectID, identities, values, specs, randomKeys, now)
	default:
		return 0, fmt.Errorf("unknown project identity mode %q", identityMode)
	}
	if err != nil {
		return 0, err
	}
	if identityMode == tracker.IdentityModeNone {
		return tag.RowsAffected(), nil
	}
	if tag.RowsAffected() == int64(len(jobs)) {
		return tag.RowsAffected(), nil
	}
	if err := validateEnqueueIdentities(ctx, tx, projectID, identityMode, identities, values, specs); err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func validateEnqueueIdentities(ctx context.Context, tx pgx.Tx, projectID, identityMode string, identities, values, specs []string) error {
	identityLookup := "stored.external_id=input.identity"
	if identityMode == tracker.IdentityModeUniqueValue {
		identityLookup = "stored.unique_value_digest=decode(input.identity,'hex')"
	}
	// LIMIT keeps this as a parameterized lookup against the composite unique index.
	query := fmt.Sprintf(`
		SELECT count(*),
			count(*) FILTER (WHERE stored.value=input.value),
			count(*) FILTER (WHERE stored.value=input.value AND stored.spec=input.spec_text::jsonb)
		FROM unnest($2::text[],$3::text[],$4::text[]) AS input(identity,value,spec_text)
		CROSS JOIN LATERAL (
			SELECT stored.value,stored.spec
			FROM tracker_jobs AS stored
			WHERE stored.project_id=$1 AND %s
			LIMIT 1
		) AS stored
	`, identityLookup)
	var total, matchingValues, matchingSpecs int64
	if err := tx.QueryRow(ctx, query, projectID, identities, values, specs).Scan(&total, &matchingValues, &matchingSpecs); err != nil {
		return err
	}
	if total != int64(len(identities)) {
		return fmt.Errorf("bulk enqueue identity verification returned %d of %d jobs", total, len(identities))
	}
	if identityMode == tracker.IdentityModeUniqueValue && matchingValues != total {
		return &tracker.Error{Code: protocol.ErrorIdentityConflict, Message: "value digest collision"}
	}
	if matchingSpecs != total {
		return &tracker.Error{Code: protocol.ErrorIdentityConflict, Message: "job identity already exists with a different spec"}
	}
	return nil
}

func (s *Store) ProjectPolicy(ctx context.Context, userID, projectID string) (protocol.ProjectPolicy, error) {
	if !queue.ValidateIdentifier(projectID) {
		return protocol.ProjectPolicy{}, tracker.InvalidRequest("invalid project ID")
	}
	var result protocol.ProjectPolicy
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		project, err := authorizeProjectWorker(ctx, tx, userID, projectID)
		if err != nil {
			return err
		}
		result = protocol.ProjectPolicy{
			ProjectID: projectID, DispatchQPS: project.dispatchQPS, WorkerClaimQPS: project.workerClaimQPS,
			MaxJobsPerClaim: project.maxJobsPerClaim, RecommendedLeaseSeconds: project.recommendedLeaseSeconds,
			PolicyVersion: project.policyVersion, RefreshAfterMS: 60_000,
		}
		return nil
	})
	return result, storeError("get project policy", err)
}

func (s *Store) ClaimProjectJobs(ctx context.Context, userID, projectID string, request protocol.ProjectClaimRequest, nowNS int64) (tracker.ProjectClaimResult, error) {
	if !queue.ValidateIdentifier(projectID) || !queue.ValidateIdentifier(request.WorkerID) || request.MaxJobs < 1 || request.MaxJobs > maxProjectBatch || request.LeaseSeconds < 1 || request.LeaseSeconds > 3600 || len(request.AcceptTypes) > 16 || request.PolicyVersion < 1 {
		return tracker.ProjectClaimResult{}, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid claim request"}
	}
	for _, jobType := range request.AcceptTypes {
		if jobType != protocol.JobTypeSeed && jobType != protocol.JobTypeAsset {
			return tracker.ProjectClaimResult{}, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "accept_types may contain only seed or asset"}
		}
	}
	now := nowNS / 1_000_000_000
	leaseExpiresAt := now + request.LeaseSeconds
	result := tracker.ProjectClaimResult{RetryAfterMS: 1000}
	var rateLimitError *tracker.Error
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		project, err := authorizeProjectWorker(ctx, tx, userID, projectID)
		if err != nil {
			return err
		}
		if project.dispatchQPS != nil {
			if err := tx.QueryRow(ctx, `
				SELECT status,claim_order,dispatch_qps,worker_claim_qps,max_jobs_per_claim,policy_version,
					dispatch_tokens,dispatch_refilled_at_ns
				FROM tracker_projects WHERE id=$1 FOR UPDATE
			`, projectID).Scan(
				&project.status, &project.claimOrder, &project.dispatchQPS, &project.workerClaimQPS,
				&project.maxJobsPerClaim, &project.policyVersion, &project.dispatchTokens, &project.dispatchRefilledAtNS,
			); err != nil {
				return err
			}
			if project.status != tracker.ProjectStatusActive {
				return &tracker.Error{Code: protocol.ErrorProjectNotActive, Message: "project is not active"}
			}
		}
		result.PolicyVersion = project.policyVersion
		claimLimit := min(request.MaxJobs, project.maxJobsPerClaim)
		if project.dispatchQPS != nil {
			tokens := refillDispatchTokens(project, nowNS)
			available := maxProjectBatch
			if tokens < maxProjectBatch {
				available = int(math.Floor(tokens))
			}
			claimLimit = min(claimLimit, available)
			project.dispatchTokens = &tokens
		}
		if _, err := tx.Exec(ctx, `
			UPDATE tracker_jobs SET
				status=CASE WHEN reset_count + 1 > $3 THEN 'reset_exhausted' ELSE 'todo' END,
				reset_count=reset_count+1, attempt_id=NULL, worker_id=NULL, lease_expires_at=NULL, updated_at=$2
			WHERE project_id=$1 AND status='wip' AND lease_expires_at <= $2
		`, projectID, now, project.maxResets); err != nil {
			return err
		}
		if claimLimit < 1 {
			var hasTodo bool
			if err := tx.QueryRow(ctx, `
				SELECT EXISTS(
					SELECT 1 FROM tracker_jobs
					WHERE project_id=$1 AND status='todo'
						AND (COALESCE(cardinality($2::text[]), 0) = 0 OR COALESCE(spec->>'type','seed') = ANY($2::text[]))
				)
			`, projectID, request.AcceptTypes).Scan(&hasTodo); err != nil {
				return err
			}
			tokens := *project.dispatchTokens
			if err := updateDispatchBucket(ctx, tx, projectID, tokens, nowNS); err != nil {
				return err
			}
			if hasTodo {
				retryAfterMS := dispatchRetryAfterMS(tokens, *project.dispatchQPS)
				rateLimitError = &tracker.Error{
					Code: protocol.ErrorProjectRateLimited, Message: "project dispatch rate limit exceeded",
					Retryable: true, RetryAfter: retryAfterMS,
					Details: map[string]any{"project_id": projectID, "policy_version": project.policyVersion},
				}
			}
			return recordClaimBucket(ctx, tx, project.workerClaimQPS, projectID, request.WorkerID, request.PolicyVersion, nowNS/1_000_000, 0)
		}
		orderBy := "created_at,job_id"
		if project.claimOrder == tracker.ClaimOrderRandom {
			orderBy = "random_key,job_id"
		} else if project.claimOrder != tracker.ClaimOrderFIFO {
			return fmt.Errorf("unknown project claim order %q", project.claimOrder)
		}
		rows, err := tx.Query(ctx, fmt.Sprintf(`
			SELECT job_id,external_id,value,spec FROM tracker_jobs
			WHERE project_id=$1 AND status='todo'
				AND (COALESCE(cardinality($2::text[]), 0) = 0 OR COALESCE(spec->>'type','seed') = ANY($2::text[]))
			ORDER BY %s
			FOR UPDATE SKIP LOCKED LIMIT $3
		`, orderBy), projectID, request.AcceptTypes, claimLimit)
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
		selectedJobs := make([]selected, 0, claimLimit)
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
			result.Jobs = append(result.Jobs, protocol.ClaimedJob{JobSpecV1: spec, JobID: selectedJob.jobID, AttemptID: attemptID, LeaseExpiresAt: leaseExpiresAt})
		}
		if len(result.Jobs) > 0 && project.dispatchQPS != nil {
			tokens := *project.dispatchTokens - float64(len(result.Jobs))
			result.RetryAfterMS = dispatchRetryAfterMS(tokens, *project.dispatchQPS)
			if err := updateDispatchBucket(ctx, tx, projectID, tokens, nowNS); err != nil {
				return err
			}
		}
		return recordClaimBucket(ctx, tx, project.workerClaimQPS, projectID, request.WorkerID, request.PolicyVersion, nowNS/1_000_000, len(result.Jobs))
	})
	if err != nil {
		return tracker.ProjectClaimResult{}, storeError("claim project jobs", err)
	}
	if rateLimitError != nil {
		return tracker.ProjectClaimResult{}, rateLimitError
	}
	if result.Jobs == nil {
		result.Jobs = []protocol.ClaimedJob{}
	}
	return result, nil
}

type workerProject struct {
	status                  string
	claimOrder              string
	dispatchQPS             *float64
	workerClaimQPS          *float64
	maxJobsPerClaim         int
	maxResets               int
	recommendedLeaseSeconds int64
	policyVersion           int64
	dispatchTokens          *float64
	dispatchRefilledAtNS    *int64
}

func authorizeProjectWorker(ctx context.Context, tx pgx.Tx, userID, projectID string) (workerProject, error) {
	var userStatus string
	var roles []string
	var project workerProject
	err := tx.QueryRow(ctx, `
		SELECT u.status,u.roles,p.status,p.claim_order,p.dispatch_qps,p.worker_claim_qps,
			p.max_jobs_per_claim,p.max_resets,p.recommended_lease_seconds,p.policy_version,p.dispatch_tokens,p.dispatch_refilled_at_ns
		FROM tracker_users u CROSS JOIN tracker_projects p WHERE u.id=$1 AND p.id=$2
	`, userID, projectID).Scan(
		&userStatus, &roles, &project.status, &project.claimOrder, &project.dispatchQPS,
		&project.workerClaimQPS, &project.maxJobsPerClaim, &project.maxResets, &project.recommendedLeaseSeconds, &project.policyVersion,
		&project.dispatchTokens, &project.dispatchRefilledAtNS,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return workerProject{}, &tracker.Error{Code: protocol.ErrorNotFound, Message: "user or project not found"}
	}
	if err != nil {
		return workerProject{}, err
	}
	if userStatus != tracker.UserStatusActive || !roleMap(roles)[tracker.RoleWorker] {
		return workerProject{}, &tracker.Error{Code: protocol.ErrorPermissionDenied, Message: "active worker role required"}
	}
	if project.status != tracker.ProjectStatusActive {
		return workerProject{}, &tracker.Error{Code: protocol.ErrorProjectNotActive, Message: "project is not active"}
	}
	return project, nil
}

func refillDispatchTokens(project workerProject, nowNS int64) float64 {
	capacity := 1.0
	if *project.dispatchQPS > 1000 {
		// High-throughput projects may accumulate at most 100ms of work so
		// batch claims remain practical without creating long idle bursts.
		capacity = math.Min(*project.dispatchQPS/10, maxProjectBatch)
	}
	if project.dispatchTokens == nil || project.dispatchRefilledAtNS == nil {
		return capacity
	}
	elapsedNS := max(int64(0), nowNS-*project.dispatchRefilledAtNS)
	return math.Min(capacity, *project.dispatchTokens+float64(elapsedNS)*(*project.dispatchQPS)/1_000_000_000)
}

func dispatchRetryAfterMS(tokens, qps float64) int64 {
	if tokens >= 1 {
		return 0
	}
	milliseconds := math.Ceil((1 - tokens) * 1000 / qps)
	if milliseconds >= math.MaxInt64 {
		return math.MaxInt64
	}
	return max(int64(1), int64(milliseconds))
}

func updateDispatchBucket(ctx context.Context, tx pgx.Tx, projectID string, tokens float64, nowNS int64) error {
	_, err := tx.Exec(ctx, `
		UPDATE tracker_projects SET dispatch_tokens=$2,dispatch_refilled_at_ns=$3 WHERE id=$1
	`, projectID, tokens, nowNS)
	return err
}

func recordClaimBucket(ctx context.Context, tx pgx.Tx, workerClaimQPS *float64, projectID, workerID string, policyVersion, nowMS int64, jobs int) error {
	if workerClaimQPS == nil {
		return nil
	}
	bucketStartedAtMS := nowMS - nowMS%60_000
	_, err := tx.Exec(ctx, `
		INSERT INTO tracker_worker_claim_buckets(
			project_id,worker_id,bucket_started_at_ms,claim_requests,jobs_dispatched,policy_version
		) VALUES($1,$2,$3,1,$4,$5)
		ON CONFLICT(project_id,worker_id,bucket_started_at_ms) DO UPDATE SET
			claim_requests=tracker_worker_claim_buckets.claim_requests+1,
			jobs_dispatched=tracker_worker_claim_buckets.jobs_dispatched+EXCLUDED.jobs_dispatched,
			policy_version=EXCLUDED.policy_version
	`, projectID, workerID, bucketStartedAtMS, jobs, policyVersion)
	return err
}

func (s *Store) CompleteProjectJobs(ctx context.Context, userID, projectID string, request protocol.ProjectCompleteRequest, now int64) (protocol.BatchResultResponse, error) {
	if !queue.ValidateIdentifier(projectID) || !queue.ValidateIdentifier(request.WorkerID) || len(request.Items) == 0 || len(request.Items) > maxProjectBatch {
		return protocol.BatchResultResponse{}, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid completion batch"}
	}
	return s.finishProjectJobs(ctx, userID, projectID, now, func(tx pgx.Tx, _ workerProject) ([]protocol.ItemResult, error) {
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
	return s.finishProjectJobs(ctx, userID, projectID, now, func(tx pgx.Tx, project workerProject) ([]protocol.ItemResult, error) {
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
			err = tx.QueryRow(ctx, `UPDATE tracker_jobs SET status=CASE WHEN $8 AND reset_count + 1 > $9 THEN 'reset_exhausted' ELSE $6 END,attempt_id=NULL,worker_id=NULL,lease_expires_at=NULL,execution_error=$7,reset_count=reset_count+CASE WHEN $8 THEN 1 ELSE 0 END,updated_at=$5,completed_at=CASE WHEN $8 AND reset_count + 1 <= $9 THEN NULL ELSE $5 END WHERE project_id=$1 AND job_id=$2 AND status='wip' AND attempt_id=$3 AND worker_id=$4 AND lease_expires_at>$5 RETURNING status`, projectID, item.JobID, item.AttemptID, request.WorkerID, now, status, executionError, item.Retryable, project.maxResets).Scan(&actualStatus)
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
	return s.finishProjectJobs(ctx, userID, projectID, now, func(tx pgx.Tx, _ workerProject) ([]protocol.ItemResult, error) {
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

func (s *Store) finishProjectJobs(ctx context.Context, userID, projectID string, now int64, fn func(pgx.Tx, workerProject) ([]protocol.ItemResult, error)) (protocol.BatchResultResponse, error) {
	var response protocol.BatchResultResponse
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		project, err := authorizeProjectWorker(ctx, tx, userID, projectID)
		if err != nil {
			return err
		}
		results, err := fn(tx, project)
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
