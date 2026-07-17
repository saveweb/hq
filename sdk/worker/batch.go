package worker

import (
	"context"
	"errors"
	"fmt"

	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

type Batch struct {
	session *Session
	route   routeIdentity
	jobs    []protocol.ClaimedJob
}

func (s *Session) Claim(
	ctx context.Context,
	maxJobs int,
	leaseSeconds int64,
	acceptTypes []string,
) (*Batch, error) {
	client, err := s.route(ctx, acceptTypes, nil, false)
	if err != nil {
		return nil, err
	}
	request := protocol.ClaimRequest{
		SessionRoute: protocol.SessionRoute{
			Route: protocol.Route{
				ProjectID: client.assignment.ProjectID, ShardID: client.assignment.ShardID,
				Generation: client.assignment.Generation,
			},
			SessionID: s.id,
		},
		MaxJobs: maxJobs, LeaseSeconds: leaseSeconds, AcceptTypes: append([]string(nil), acceptTypes...),
	}
	var response protocol.ClaimResponse
	if err := client.do(ctx, "/api/v1/queue/claim", request, &response); err != nil {
		return nil, err
	}
	if response.ProjectID != request.ProjectID || response.ShardID != request.ShardID || response.Generation != request.Generation {
		return nil, fmt.Errorf("worker: shard returned a mismatched claim route")
	}
	return &Batch{
		session: s, route: identityOf(client.assignment),
		jobs: append([]protocol.ClaimedJob(nil), response.Jobs...),
	}, nil
}

func (b *Batch) Jobs() []protocol.ClaimedJob {
	return append([]protocol.ClaimedJob(nil), b.jobs...)
}

func (b *Batch) Route() protocol.Route {
	return protocol.Route{ProjectID: b.route.projectID, ShardID: b.route.shardID, Generation: b.route.generation}
}

func (b *Batch) Refresh(ctx context.Context) error {
	_, err := b.session.route(ctx, nil, &b.route, true)
	return err
}

func (b *Batch) Complete(ctx context.Context, items []protocol.CompleteItem) (protocol.BatchResultResponse, error) {
	client, err := b.session.route(ctx, nil, &b.route, false)
	if err != nil {
		return protocol.BatchResultResponse{}, err
	}
	request := protocol.CompleteRequest{SessionRoute: b.sessionRoute(), Items: items}
	var response protocol.BatchResultResponse
	err = client.do(ctx, "/api/v1/queue/complete", request, &response)
	if err != nil {
		return protocol.BatchResultResponse{}, b.resolveMutationFailure(ctx, err)
	}
	return response, nil
}

func (b *Batch) Fail(ctx context.Context, items []protocol.FailItem) (protocol.BatchResultResponse, error) {
	client, err := b.session.route(ctx, nil, &b.route, false)
	if err != nil {
		return protocol.BatchResultResponse{}, err
	}
	request := protocol.FailRequest{SessionRoute: b.sessionRoute(), Items: items}
	var response protocol.BatchResultResponse
	err = client.do(ctx, "/api/v1/queue/fail", request, &response)
	if err != nil {
		return protocol.BatchResultResponse{}, b.resolveMutationFailure(ctx, err)
	}
	return response, nil
}

func (b *Batch) ExtendLease(ctx context.Context, extendSeconds int64, items []protocol.AttemptRef) (protocol.BatchResultResponse, error) {
	client, err := b.session.route(ctx, nil, &b.route, false)
	if err != nil {
		return protocol.BatchResultResponse{}, err
	}
	request := protocol.ExtendLeaseRequest{
		SessionRoute: b.sessionRoute(), ExtendSeconds: extendSeconds, Items: items,
	}
	var response protocol.BatchResultResponse
	err = client.do(ctx, "/api/v1/queue/extend-lease", request, &response)
	if err != nil {
		return protocol.BatchResultResponse{}, b.resolveMutationFailure(ctx, err)
	}
	return response, nil
}

func (b *Batch) sessionRoute() protocol.SessionRoute {
	return protocol.SessionRoute{
		Route: protocol.Route{
			ProjectID: b.route.projectID, ShardID: b.route.shardID, Generation: b.route.generation,
		},
		SessionID: b.session.id,
	}
}

func (b *Batch) resolveMutationFailure(ctx context.Context, operationError error) error {
	var apiError *APIError
	if errors.As(operationError, &apiError) {
		switch apiError.API.Code {
		case protocol.ErrorStaleGeneration, protocol.ErrorShardNotActive,
			protocol.ErrorOwnerLeaseExpired, protocol.ErrorShardUnavailable,
			protocol.ErrorInvalidAccessToken, protocol.ErrorSessionExpired:
		default:
			return operationError
		}
	}
	_, refreshError := b.session.route(ctx, nil, &b.route, true)
	if errors.Is(refreshError, ErrRouteRetired) {
		return fmt.Errorf("%w: original queue error: %v", ErrRouteRetired, operationError)
	}
	return operationError
}
