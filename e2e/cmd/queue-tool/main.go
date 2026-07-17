package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/sqlitequeue"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	mode := flag.String("mode", "", "seed, check, or check-source")
	dataDir := flag.String("data-dir", "", "shard data directory")
	projectID := flag.String("project-id", "", "project identifier")
	shardID := flag.String("shard-id", "", "shard identifier")
	generation := flag.Int64("generation", 1, "queue generation")
	flag.Parse()
	if flag.NArg() != 0 || *dataDir == "" || *projectID == "" || *shardID == "" || *generation < 1 {
		return fmt.Errorf("queue-tool: mode, data-dir, project-id, shard-id, and generation are required")
	}
	path := databasePath(*dataDir, *projectID, *shardID)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := sqlitequeue.Open(ctx, path, queue.Identity{
		ProjectID: *projectID, ShardID: *shardID, Generation: *generation,
	})
	if err != nil {
		return err
	}
	defer store.Close()
	switch *mode {
	case "seed":
		return seed(ctx, store, *generation)
	case "check":
		return check(ctx, store)
	case "check-source":
		return checkSource(ctx, store)
	default:
		return fmt.Errorf("queue-tool: --mode must be seed, check, or check-source")
	}
}

func checkSource(ctx context.Context, store *sqlitequeue.Store) error {
	stats, err := store.Stats(ctx)
	if err != nil {
		return err
	}
	if stats.Todo != 0 || stats.WIP != 0 || stats.Done != 2 || stats.Failed != 0 || stats.ResetExhausted != 0 {
		return fmt.Errorf("queue-tool: unexpected source final stats: %+v", stats)
	}
	fmt.Printf("source queue stats: done=%d\n", stats.Done)
	return nil
}

func databasePath(dataDir, projectID, shardID string) string {
	project := base64.RawURLEncoding.EncodeToString([]byte(projectID))
	shard := base64.RawURLEncoding.EncodeToString([]byte(shardID))
	return filepath.Join(dataDir, "queues", project, shard+".sqlite")
}

func seed(ctx context.Context, store *sqlitequeue.Store, generation int64) error {
	now := time.Now().Unix()
	if err := store.SetFence(ctx, generation, now, now+30); err != nil {
		return err
	}
	jobs := []queue.JobSpec{
		{ID: "a-go", URL: "https://example.test/go", Type: "seed", Attrs: map[string]any{"e2e": "go"}},
		{ID: "b-python", URL: "https://example.test/python", Type: "seed", Attrs: map[string]any{"e2e": "python"}},
		{ID: "e-takeover", URL: "https://example.test/takeover", Type: "asset", Attrs: map[string]any{"e2e": "takeover"}},
	}
	result, err := store.Enqueue(ctx, generation, now, jobs)
	if err != nil {
		return err
	}
	if result.Inserted != len(jobs) || result.Duplicate != 0 {
		return fmt.Errorf("queue-tool: unexpected seed result: %+v", result)
	}
	return nil
}

func check(ctx context.Context, store *sqlitequeue.Store) error {
	stats, err := store.Stats(ctx)
	if err != nil {
		return err
	}
	if stats.Todo != 0 || stats.WIP != 0 || stats.Done != 4 || stats.Failed != 1 || stats.ResetExhausted != 0 {
		return fmt.Errorf("queue-tool: unexpected final stats: %+v", stats)
	}
	fmt.Printf("final queue stats: done=%d failed=%d\n", stats.Done, stats.Failed)
	return nil
}
