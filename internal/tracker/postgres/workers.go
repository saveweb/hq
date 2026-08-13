package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/saveweb/hq/internal/queue"
	"github.com/saveweb/hq/internal/tracker"
)

const workerLastSeenWriteInterval = int64(60)

func associateWorker(ctx context.Context, tx pgx.Tx, workerID, userID string, now int64) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO tracker_workers(worker_id,user_id,last_seen_at) VALUES($1,$2,$3)
		ON CONFLICT(worker_id) DO UPDATE SET
			user_id=EXCLUDED.user_id,
			last_seen_at=GREATEST(tracker_workers.last_seen_at,EXCLUDED.last_seen_at)
		WHERE tracker_workers.user_id IS DISTINCT FROM EXCLUDED.user_id
			OR tracker_workers.last_seen_at <= EXCLUDED.last_seen_at-$4
	`, workerID, userID, now, workerLastSeenWriteInterval)
	return err
}

func (s *Store) WorkerUserID(ctx context.Context, workerID string) (string, bool, error) {
	var userID string
	err := s.pool.QueryRow(ctx, `SELECT user_id FROM tracker_workers WHERE worker_id=$1`, workerID).Scan(&userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return userID, err == nil, err
}

func (s *Store) DeleteWorker(ctx context.Context, workerID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM tracker_workers WHERE worker_id=$1`, workerID)
	return err
}

func (s *Store) ListWorkers(ctx context.Context, workerID, userID string, limit int) ([]tracker.WorkerUserMapping, error) {
	if (workerID != "" && !queue.ValidateIdentifier(workerID)) || (userID != "" && !queue.ValidateIdentifier(userID)) || (workerID != "" && userID != "") || limit < 1 || limit > 200 {
		return nil, tracker.InvalidRequest("invalid worker query")
	}
	query := `SELECT worker_id,user_id,last_seen_at FROM tracker_workers ORDER BY last_seen_at DESC,worker_id LIMIT $1`
	arguments := []any{limit}
	if workerID != "" {
		query = `SELECT worker_id,user_id,last_seen_at FROM tracker_workers WHERE worker_id=$1 LIMIT $2`
		arguments = []any{workerID, limit}
	} else if userID != "" {
		query = `SELECT worker_id,user_id,last_seen_at FROM tracker_workers WHERE user_id=$1 ORDER BY last_seen_at DESC,worker_id LIMIT $2`
		arguments = []any{userID, limit}
	}
	rows, err := s.pool.Query(ctx, query, arguments...)
	if err != nil {
		return nil, storeError("list workers", err)
	}
	defer rows.Close()
	result := []tracker.WorkerUserMapping{}
	for rows.Next() {
		var item tracker.WorkerUserMapping
		if err := rows.Scan(&item.WorkerID, &item.UserID, &item.LastSeenAt); err != nil {
			return nil, storeError("list workers", err)
		}
		result = append(result, item)
	}
	return result, storeError("list workers", rows.Err())
}
