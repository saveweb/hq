package worker

import (
	"context"
	"fmt"

	"git.saveweb.org/saveweb/hq/internal/trackerclient"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

// ProjectQueue is the direct client for the single-site PostgreSQL queue. It
// has no route discovery, shard lease, or background heartbeat.
type ProjectQueue struct {
	projectID string
	workerID  string
	client    *trackerclient.Client
}

func OpenProjectQueue(config Config, projectID string) (*ProjectQueue, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	if projectID == "" {
		return nil, fmt.Errorf("worker: project ID is required")
	}
	client, err := trackerFor(config)
	if err != nil {
		return nil, err
	}
	return &ProjectQueue{projectID: projectID, workerID: config.WorkerID, client: client}, nil
}

func (q *ProjectQueue) Claim(ctx context.Context, maxJobs int, leaseSeconds int64, acceptTypes []string) (protocol.ProjectClaimResponse, error) {
	result, err := q.client.ClaimProjectJobs(ctx, q.projectID, protocol.ProjectClaimRequest{
		WorkerID: q.workerID, MaxJobs: maxJobs, LeaseSeconds: leaseSeconds,
		AcceptTypes: append([]string(nil), acceptTypes...),
	})
	if err != nil {
		return protocol.ProjectClaimResponse{}, convertTrackerError(err)
	}
	if result.ProjectID != q.projectID {
		return protocol.ProjectClaimResponse{}, fmt.Errorf("worker: tracker returned a mismatched project")
	}
	return result, nil
}

func (q *ProjectQueue) Complete(ctx context.Context, items []protocol.ProjectCompleteItem) (protocol.BatchResultResponse, error) {
	result, err := q.client.CompleteProjectJobs(ctx, q.projectID, protocol.ProjectCompleteRequest{WorkerID: q.workerID, Items: items})
	return result, convertTrackerError(err)
}

func (q *ProjectQueue) Fail(ctx context.Context, items []protocol.FailItem) (protocol.BatchResultResponse, error) {
	result, err := q.client.FailProjectJobs(ctx, q.projectID, protocol.ProjectFailRequest{WorkerID: q.workerID, Items: items})
	return result, convertTrackerError(err)
}

func (q *ProjectQueue) ExtendLease(ctx context.Context, extendSeconds int64, items []protocol.AttemptRef) (protocol.BatchResultResponse, error) {
	result, err := q.client.ExtendProjectJobLeases(ctx, q.projectID, protocol.ProjectExtendLeaseRequest{WorkerID: q.workerID, ExtendSeconds: extendSeconds, Items: items})
	return result, convertTrackerError(err)
}
