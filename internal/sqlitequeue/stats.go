package sqlitequeue

import (
	"context"
	"fmt"

	"git.saveweb.org/saveweb/hq/internal/queue"
)

func (s *Store) Stats(ctx context.Context) (queue.Stats, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT status, count(*) FROM jobs GROUP BY status")
	if err != nil {
		return queue.Stats{}, fmt.Errorf("sqlitequeue: query stats: %w", err)
	}
	defer rows.Close()
	var stats queue.Stats
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return queue.Stats{}, fmt.Errorf("sqlitequeue: scan stats: %w", err)
		}
		switch status {
		case queue.StatusTodo:
			stats.Todo = count
		case queue.StatusWIP:
			stats.WIP = count
		case queue.StatusDone:
			stats.Done = count
		case queue.StatusFailed:
			stats.Failed = count
		case queue.StatusResetExhausted:
			stats.ResetExhausted = count
		default:
			return queue.Stats{}, fmt.Errorf("sqlitequeue: unknown status %q", status)
		}
	}
	if err := rows.Err(); err != nil {
		return queue.Stats{}, fmt.Errorf("sqlitequeue: iterate stats: %w", err)
	}
	return stats, nil
}
