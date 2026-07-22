package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/saveweb/hq/internal/queue"
	"github.com/saveweb/hq/internal/tracker"
)

func associateWorker(ctx context.Context, tx pgx.Tx, workerID, userID string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO tracker_workers(worker_id,user_id) VALUES($1,$2)
		ON CONFLICT(worker_id) DO UPDATE SET user_id=EXCLUDED.user_id
	`, workerID, userID)
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

func (s *Store) ListWorkers(ctx context.Context, workerID string, limit int) ([]tracker.WorkerUserMapping, error) {
	if (workerID != "" && !queue.ValidateIdentifier(workerID)) || limit < 1 || limit > 200 {
		return nil, tracker.InvalidRequest("invalid worker query")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT worker_id,user_id FROM tracker_workers
		WHERE $1='' OR worker_id=$1
		ORDER BY worker_id LIMIT $2
	`, workerID, limit)
	if err != nil {
		return nil, storeError("list workers", err)
	}
	defer rows.Close()
	result := []tracker.WorkerUserMapping{}
	for rows.Next() {
		var item tracker.WorkerUserMapping
		if err := rows.Scan(&item.WorkerID, &item.UserID); err != nil {
			return nil, storeError("list workers", err)
		}
		result = append(result, item)
	}
	return result, storeError("list workers", rows.Err())
}
