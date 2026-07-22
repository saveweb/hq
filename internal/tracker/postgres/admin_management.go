package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/saveweb/hq/internal/queue"
	"github.com/saveweb/hq/internal/tracker"
	"github.com/saveweb/hq/pkg/protocol"
)

func (s *Store) ListUsers(ctx context.Context) ([]protocol.AdminUserSummary, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT u.id,COALESCE(u.github_login,''),u.status,u.roles,
			EXISTS(SELECT 1 FROM tracker_machine_tokens mt WHERE mt.user_id=u.id AND mt.revoked_at IS NULL),
			EXISTS(SELECT 1 FROM tracker_machine_tokens mt WHERE mt.user_id=u.id AND mt.revoked_at IS NULL AND mt.token IS NOT NULL),
			u.created_at,u.updated_at
		FROM tracker_users u ORDER BY u.id
	`)
	if err != nil {
		return nil, storeError("list users", err)
	}
	defer rows.Close()
	result := []protocol.AdminUserSummary{}
	for rows.Next() {
		var item protocol.AdminUserSummary
		if err := rows.Scan(&item.ID, &item.GitHubLogin, &item.Status, &item.Roles, &item.MachineTokenActive, &item.MachineTokenViewable, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, storeError("list users", err)
		}
		result = append(result, item)
	}
	return result, storeError("list users", rows.Err())
}

func (s *Store) MachineToken(ctx context.Context, userID string) (string, bool, error) {
	if !queue.ValidateIdentifier(userID) {
		return "", false, tracker.InvalidRequest("invalid user ID")
	}
	var token string
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(token,'') FROM tracker_machine_tokens WHERE user_id=$1 AND revoked_at IS NULL`, userID).Scan(&token)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return token, err == nil, storeError("get machine token", err)
}

func (s *Store) PutUser(ctx context.Context, userID, status string, roles []string, now int64) error {
	if !queue.ValidateIdentifier(userID) || (status != tracker.UserStatusPending && status != tracker.UserStatusActive && status != tracker.UserStatusSuspended) {
		return tracker.InvalidRequest("invalid user")
	}
	seen := map[string]bool{}
	for _, role := range roles {
		if (role != tracker.RoleAdmin && role != tracker.RoleWorker) || seen[role] {
			return tracker.InvalidRequest("invalid user roles")
		}
		seen[role] = true
	}
	roles = append([]string{}, roles...)
	sort.Strings(roles)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO tracker_users(id,status,roles,created_at,updated_at) VALUES($1,$2,$3,$4,$4)
		ON CONFLICT(id) DO UPDATE SET status=EXCLUDED.status,roles=EXCLUDED.roles,updated_at=EXCLUDED.updated_at
	`, userID, status, roles, now)
	return storeError("put user", err)
}

func (s *Store) DeleteUser(ctx context.Context, userID string) error {
	if !queue.ValidateIdentifier(userID) {
		return tracker.InvalidRequest("invalid user ID")
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM tracker_users WHERE id=$1`, userID)
	if err != nil {
		return storeError("delete user", err)
	}
	if tag.RowsAffected() == 0 {
		return &tracker.Error{Code: protocol.ErrorNotFound, Message: "user not found"}
	}
	return nil
}

func (s *Store) RotateMachineToken(ctx context.Context, userID, token string, now int64) error {
	if !queue.ValidateIdentifier(userID) || token == "" {
		return tracker.InvalidRequest("invalid user or token")
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO tracker_machine_tokens(user_id,token_hash,token,created_at,revoked_at)
		SELECT id,$2,$3,$4,NULL FROM tracker_users WHERE id=$1
		ON CONFLICT(user_id) DO UPDATE SET token_hash=EXCLUDED.token_hash,token=EXCLUDED.token,created_at=EXCLUDED.created_at,revoked_at=NULL
	`, userID, tokenDigest(token), token, now)
	if err != nil {
		return storeError("rotate machine token", err)
	}
	if tag.RowsAffected() == 0 {
		return &tracker.Error{Code: protocol.ErrorNotFound, Message: "user not found"}
	}
	return nil
}

func (s *Store) RevokeMachineToken(ctx context.Context, userID string, now int64) error {
	if !queue.ValidateIdentifier(userID) {
		return tracker.InvalidRequest("invalid user ID")
	}
	tag, err := s.pool.Exec(ctx, `UPDATE tracker_machine_tokens SET revoked_at=$2 WHERE user_id=$1 AND revoked_at IS NULL`, userID, now)
	if err != nil {
		return storeError("revoke machine token", err)
	}
	if tag.RowsAffected() == 0 {
		return &tracker.Error{Code: protocol.ErrorNotFound, Message: "active machine token not found"}
	}
	return nil
}

func (s *Store) DeleteProject(ctx context.Context, projectID string) error {
	if !queue.ValidateIdentifier(projectID) {
		return tracker.InvalidRequest("invalid project ID")
	}
	return storeError("delete project", pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var exists, hasWIP bool
		if err := tx.QueryRow(ctx, `SELECT true,EXISTS(SELECT 1 FROM tracker_jobs WHERE project_id=$1 AND status='wip') FROM tracker_projects WHERE id=$1 FOR UPDATE`, projectID).Scan(&exists, &hasWIP); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return &tracker.Error{Code: protocol.ErrorNotFound, Message: "project not found"}
			}
			return err
		}
		if hasWIP {
			return tracker.InvalidRequest("project has WIP jobs")
		}
		_, err := tx.Exec(ctx, `DELETE FROM tracker_projects WHERE id=$1`, projectID)
		return err
	}))
}

func (s *Store) ListProjectJobs(ctx context.Context, projectID, status string, afterJobID int64, limit int) (protocol.AdminJobListResponse, error) {
	if !queue.ValidateIdentifier(projectID) || afterJobID < 0 || limit < 1 || limit > 200 || (status != "" && status != protocol.JobStatusTodo && status != protocol.JobStatusWIP && status != protocol.JobStatusDone && status != protocol.JobStatusFailed && status != protocol.JobStatusResetExhausted) {
		return protocol.AdminJobListResponse{}, tracker.InvalidRequest("invalid job query")
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT true FROM tracker_projects WHERE id=$1`, projectID).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return protocol.AdminJobListResponse{}, &tracker.Error{Code: protocol.ErrorNotFound, Message: "project not found"}
		}
		return protocol.AdminJobListResponse{}, storeError("list project jobs", err)
	}
	rows, err := s.pool.Query(ctx, adminJobSelect+` WHERE project_id=$1 AND job_id>$2 AND ($3='' OR status=$3) ORDER BY job_id LIMIT $4`, projectID, afterJobID, status, limit+1)
	if err != nil {
		return protocol.AdminJobListResponse{}, storeError("list project jobs", err)
	}
	defer rows.Close()
	response := protocol.AdminJobListResponse{Jobs: []protocol.AdminJob{}}
	for rows.Next() {
		job, err := scanAdminJob(rows)
		if err != nil {
			return protocol.AdminJobListResponse{}, storeError("list project jobs", err)
		}
		response.Jobs = append(response.Jobs, job)
	}
	if err := rows.Err(); err != nil {
		return protocol.AdminJobListResponse{}, storeError("list project jobs", err)
	}
	if len(response.Jobs) > limit {
		response.Jobs = response.Jobs[:limit]
		next := response.Jobs[len(response.Jobs)-1].JobID
		response.NextAfterJobID = &next
	}
	return response, nil
}

func (s *Store) ProjectJob(ctx context.Context, projectID string, jobID int64) (protocol.AdminJob, error) {
	if !queue.ValidateIdentifier(projectID) || jobID < 1 {
		return protocol.AdminJob{}, tracker.InvalidRequest("invalid job")
	}
	job, err := scanAdminJob(s.pool.QueryRow(ctx, adminJobSelect+` WHERE project_id=$1 AND job_id=$2`, projectID, jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		return protocol.AdminJob{}, &tracker.Error{Code: protocol.ErrorNotFound, Message: "job not found"}
	}
	return job, storeError("get project job", err)
}

func (s *Store) RequeueProjectJob(ctx context.Context, projectID string, jobID, now int64) error {
	if !queue.ValidateIdentifier(projectID) || jobID < 1 {
		return tracker.InvalidRequest("invalid job")
	}
	tag, err := s.pool.Exec(ctx, `UPDATE tracker_jobs SET status='todo',reset_count=0,execution_error=NULL,outcome=NULL,warc_receipts=NULL,completed_at=NULL,updated_at=$3 WHERE project_id=$1 AND job_id=$2 AND status IN ('failed','reset_exhausted')`, projectID, jobID, now)
	if err != nil {
		return storeError("requeue project job", err)
	}
	if tag.RowsAffected() == 0 {
		return tracker.InvalidRequest("job is not failed or reset_exhausted")
	}
	return nil
}

func (s *Store) DeleteProjectJob(ctx context.Context, projectID string, jobID int64) error {
	if !queue.ValidateIdentifier(projectID) || jobID < 1 {
		return tracker.InvalidRequest("invalid job")
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM tracker_jobs WHERE project_id=$1 AND job_id=$2 AND status<>'wip'`, projectID, jobID)
	if err != nil {
		return storeError("delete project job", err)
	}
	if tag.RowsAffected() == 0 {
		return tracker.InvalidRequest("job not found or is WIP")
	}
	return nil
}

const adminJobSelect = `SELECT job_id,external_id,value,spec,random_key,status,attempt_id,worker_id,lease_expires_at,reset_count,outcome,warc_receipts,execution_error,created_at,updated_at,completed_at FROM tracker_jobs`

type adminJobScanner interface{ Scan(...any) error }

func scanAdminJob(row adminJobScanner) (protocol.AdminJob, error) {
	var job protocol.AdminJob
	var externalID *string
	var spec, outcome, receipts, executionError []byte
	if err := row.Scan(&job.JobID, &externalID, &job.Value, &spec, &job.RandomKey, &job.Status, &job.AttemptID, &job.WorkerID, &job.LeaseExpiresAt, &job.ResetCount, &outcome, &receipts, &executionError, &job.CreatedAt, &job.UpdatedAt, &job.CompletedAt); err != nil {
		return protocol.AdminJob{}, err
	}
	if externalID != nil {
		job.ID = *externalID
	}
	var stored storedJobSpec
	if err := json.Unmarshal(spec, &stored); err != nil {
		return protocol.AdminJob{}, err
	}
	job.Type, job.Via, job.Hops, job.Attrs = stored.Type, stored.Via, stored.Hops, stored.Attrs
	if job.Type == "" {
		job.Type = protocol.JobTypeSeed
	}
	if outcome != nil {
		job.Outcome = new(protocol.Outcome)
		if err := json.Unmarshal(outcome, job.Outcome); err != nil {
			return protocol.AdminJob{}, err
		}
	}
	job.WARCReceipts = []protocol.WARCReceipt{}
	if receipts != nil {
		if err := json.Unmarshal(receipts, &job.WARCReceipts); err != nil {
			return protocol.AdminJob{}, err
		}
	}
	if executionError != nil {
		job.ExecutionError = new(protocol.ExecutionError)
		if err := json.Unmarshal(executionError, job.ExecutionError); err != nil {
			return protocol.AdminJob{}, err
		}
	}
	return job, nil
}
