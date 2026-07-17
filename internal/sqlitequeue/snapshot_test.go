package sqlitequeue

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"git.saveweb.org/saveweb/hq/internal/queue"
)

func TestSnapshotCreatesCompactIndependentSQLite(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	store, err := Open(ctx, filepath.Join(directory, "live.sqlite"), queue.Identity{
		ProjectID: "project-1", ShardID: "shard-1", Generation: 1,
	}, WithClock(func() int64 { return 100 }))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SetFence(ctx, 1, 100, 200); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Enqueue(ctx, 1, 100, []queue.JobSpec{{
		ID: "job-1", URL: "https://example.test/", Type: "seed", Attrs: map[string]any{},
	}}); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(directory, "snapshot with space.sqlite")
	if err := store.Snapshot(ctx, destination); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(destination)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("snapshot info = %+v, %v", info, err)
	}
	copyStore, err := Open(ctx, destination, queue.Identity{
		ProjectID: "project-1", ShardID: "shard-1", Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := copyStore.Stats(ctx)
	_ = copyStore.Close()
	if err != nil || stats.Todo != 1 {
		t.Fatalf("snapshot stats = %+v, %v", stats, err)
	}
	if err := store.Snapshot(ctx, destination); err == nil {
		t.Fatal("snapshot overwrote an existing file")
	}
}
