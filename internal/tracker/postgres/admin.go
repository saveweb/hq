package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/saveweb/hq/internal/queue"
	"github.com/saveweb/hq/internal/tracker"
	"github.com/saveweb/hq/pkg/protocol"
)

const projectSummaryQuery = `
		SELECT p.id,p.status,p.identity_mode,p.claim_order,p.dispatch_qps,p.worker_claim_qps,
			p.max_jobs_per_claim,p.max_resets,p.recommended_lease_seconds,p.client_versions,p.policy_version,p.created_at,p.updated_at,
		count(*) FILTER (WHERE j.status='todo'),
		count(*) FILTER (WHERE j.status='wip'),
		count(*) FILTER (WHERE j.status='done'),
		count(*) FILTER (WHERE j.status='failed'),
		count(*) FILTER (WHERE j.status='reset_exhausted')
	FROM tracker_projects p
	LEFT JOIN tracker_jobs j ON j.project_id=p.id
`

func (s *Store) ListProjectSummaries(ctx context.Context) ([]protocol.AdminProjectSummary, error) {
	rows, err := s.pool.Query(ctx, projectSummaryQuery+`
			GROUP BY p.id,p.status,p.identity_mode,p.claim_order,p.dispatch_qps,p.worker_claim_qps,
				p.max_jobs_per_claim,p.max_resets,p.recommended_lease_seconds,p.client_versions,p.policy_version,p.created_at,p.updated_at
		ORDER BY p.id
	`)
	if err != nil {
		return nil, storeError("list projects", err)
	}
	defer rows.Close()
	projects := []protocol.AdminProjectSummary{}
	for rows.Next() {
		project, err := scanProjectSummary(rows)
		if err != nil {
			return nil, storeError("list projects", err)
		}
		projects = append(projects, project)
	}
	if err := rows.Err(); err != nil {
		return nil, storeError("list projects", err)
	}
	return projects, nil
}

func (s *Store) ProjectSummary(ctx context.Context, projectID string) (protocol.AdminProjectSummary, error) {
	if !queue.ValidateIdentifier(projectID) {
		return protocol.AdminProjectSummary{}, tracker.InvalidRequest("invalid project ID")
	}
	row := s.pool.QueryRow(ctx, projectSummaryQuery+`
		WHERE p.id=$1
		GROUP BY p.id,p.status,p.identity_mode,p.claim_order,p.dispatch_qps,p.worker_claim_qps,
			p.max_jobs_per_claim,p.max_resets,p.recommended_lease_seconds,p.client_versions,p.policy_version,p.created_at,p.updated_at
	`, projectID)
	project, err := scanProjectSummary(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return protocol.AdminProjectSummary{}, &tracker.Error{Code: protocol.ErrorNotFound, Message: "project not found"}
	}
	if err != nil {
		return protocol.AdminProjectSummary{}, storeError("get project", err)
	}
	return project, nil
}

type projectSummaryScanner interface {
	Scan(...any) error
}

func scanProjectSummary(row projectSummaryScanner) (protocol.AdminProjectSummary, error) {
	var project protocol.AdminProjectSummary
	var todo, wip, done, failed, resetExhausted int64
	err := row.Scan(
		&project.ID, &project.Status, &project.IdentityMode, &project.ClaimOrder,
		&project.DispatchQPS, &project.WorkerClaimQPS, &project.MaxJobsPerClaim, &project.MaxResets, &project.RecommendedLeaseSeconds, &project.ClientVersions, &project.PolicyVersion,
		&project.CreatedAt, &project.UpdatedAt,
		&todo, &wip, &done, &failed, &resetExhausted,
	)
	if err != nil {
		return protocol.AdminProjectSummary{}, err
	}
	project.JobCounts = map[string]int64{
		protocol.JobStatusTodo:           todo,
		protocol.JobStatusWIP:            wip,
		protocol.JobStatusDone:           done,
		protocol.JobStatusFailed:         failed,
		protocol.JobStatusResetExhausted: resetExhausted,
	}
	return project, nil
}
