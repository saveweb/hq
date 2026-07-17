package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"git.saveweb.org/saveweb/hq/internal/objectstore"
	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func (s *Store) PutReceiver(ctx context.Context, receiver tracker.Receiver, now int64) error {
	return putReceiver(ctx, s.pool, receiver, now)
}

func putReceiver(ctx context.Context, db commandDB, receiver tracker.Receiver, now int64) error {
	if !queue.ValidateIdentifier(receiver.ProjectID) || !queue.ValidateIdentifier(receiver.ID) ||
		(receiver.Status != tracker.ReceiverStatusActive && receiver.Status != tracker.ReceiverStatusRemoved) ||
		receiver.SinkURI == "" || strings.HasSuffix(receiver.SinkURI, "/") ||
		receiver.Format != "jobs-jsonl-zstd-v1" {
		return fmt.Errorf("tracker postgres: invalid receiver")
	}
	if _, err := objectstore.ParseURI(receiver.SinkURI + "/probe"); err != nil {
		return fmt.Errorf("tracker postgres: invalid receiver sink URI: %w", err)
	}
	command, err := db.Exec(ctx, `
		INSERT INTO tracker_receivers(project_id, id, status, sink_uri, format, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$6)
		ON CONFLICT (project_id, id) DO UPDATE SET
			status=EXCLUDED.status, updated_at=EXCLUDED.updated_at
		WHERE tracker_receivers.sink_uri=EXCLUDED.sink_uri
			AND tracker_receivers.format=EXCLUDED.format
	`, receiver.ProjectID, receiver.ID, receiver.Status, receiver.SinkURI, receiver.Format, now)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("tracker postgres: receiver sink identity is immutable")
	}
	return nil
}

func (s *Store) GetReceiverTarget(
	ctx context.Context,
	userID, agentID, sessionID, receiverID string,
	now int64,
) (tracker.Receiver, error) {
	session, err := scanSession(s.pool.QueryRow(ctx, `
		SELECT `+sessionColumns+` FROM tracker_worker_sessions
		WHERE id=$1 AND user_id=$2 AND agent_id=$3
	`, sessionID, userID, agentID))
	if errors.Is(err, pgx.ErrNoRows) {
		return tracker.Receiver{}, &tracker.Error{Code: protocol.ErrorNotFound, Message: "session not found"}
	}
	if err != nil {
		return tracker.Receiver{}, fmt.Errorf("tracker postgres: get receiver session: %w", err)
	}
	if now >= session.LeaseExpiresAt {
		return tracker.Receiver{}, &tracker.Error{Code: protocol.ErrorSessionExpired, Message: "session lease expired"}
	}
	var result tracker.Receiver
	err = s.pool.QueryRow(ctx, `
		SELECT r.project_id, r.id, r.status, r.sink_uri, r.format
		FROM tracker_receivers r
		JOIN tracker_projects p ON p.id=r.project_id
		JOIN tracker_agents a ON a.id=$4
		JOIN tracker_users u ON u.id=a.user_id
		WHERE r.project_id=$1 AND r.id=$2 AND r.status='active'
			AND p.status IN ('active','draining')
			AND a.user_id=$3 AND a.kind='worker' AND a.status='online'
			AND u.status='active' AND 'worker'=ANY(u.roles)
	`, session.ProjectID, receiverID, userID, agentID).Scan(
		&result.ProjectID, &result.ID, &result.Status, &result.SinkURI, &result.Format,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return tracker.Receiver{}, &tracker.Error{
			Code: protocol.ErrorShardNotActive, Message: "receiver is not active for this session project",
		}
	}
	if err != nil {
		return tracker.Receiver{}, fmt.Errorf("tracker postgres: get receiver target: %w", err)
	}
	return result, nil
}
