package checkpointrestore_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"git.saveweb.org/saveweb/hq/internal/checkpointformat"
	"git.saveweb.org/saveweb/hq/internal/checkpointrestore"
	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/sqlitequeue"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func TestRestoreVerifiesAndInstallsStandaloneQueue(t *testing.T) {
	ctx := context.Background()
	now := int64(1_780_100_000)
	source, err := sqlitequeue.Open(ctx, filepath.Join(t.TempDir(), "source.sqlite"), queue.Identity{
		ProjectID: "project-1", ShardID: "shard-1", Generation: 1,
	}, sqlitequeue.WithClock(func() int64 { return now }))
	if err != nil {
		t.Fatal(err)
	}
	if err := source.SetFence(ctx, 1, now, now+120); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Enqueue(ctx, 1, now, []queue.JobSpec{{
		ID: "job-1", URL: "https://example.test/", Type: "seed", Attrs: map[string]any{},
	}}); err != nil {
		t.Fatal(err)
	}
	artifact, err := checkpointformat.Create(ctx, source, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer artifact.Cleanup()
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(artifact.Path)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = response.Write(body)
	}))
	defer server.Close()
	config := checkpointrestore.DefaultConfig()
	config.AllowHTTP = true
	config.Clock = func() int64 { return now }
	restorer, err := checkpointrestore.New(config)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "queues", "restored.sqlite")
	assignment := protocol.OwnerAssignment{
		Route:  protocol.Route{ProjectID: "project-1", ShardID: "shard-1", Generation: 2},
		Status: "recovering", OwnerLeaseExpiresAt: now + 120,
		Checkpoint: &protocol.CheckpointRestore{
			Generation: 1, Sequence: 1, Format: checkpointformat.FormatSQLiteZstdV1,
			SHA256: artifact.SHA256, SizeBytes: artifact.SizeBytes, CreatedAt: now,
			DownloadURL: server.URL, URLExpiresAt: now + 60,
		},
	}
	if err := restorer.Restore(ctx, assignment, destination); err != nil {
		t.Fatal(err)
	}
	restored, err := sqlitequeue.Open(ctx, destination, queue.Identity{
		ProjectID: "project-1", ShardID: "shard-1", Generation: 2,
	}, sqlitequeue.WithClock(func() int64 { return now }))
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err := restored.SetFence(ctx, 2, now, now+120); err != nil {
		t.Fatal(err)
	}
	stats, err := restored.Stats(ctx)
	if err != nil || stats.Todo != 1 {
		t.Fatalf("restored stats = %+v, %v", stats, err)
	}

	bad := assignment
	copyCheckpoint := *assignment.Checkpoint
	copyCheckpoint.SHA256 = strings.Repeat("0", 64)
	bad.Checkpoint = &copyCheckpoint
	if err := restorer.Restore(ctx, bad, filepath.Join(t.TempDir(), "bad.sqlite")); err == nil ||
		!strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("checksum error = %v", err)
	}
}
