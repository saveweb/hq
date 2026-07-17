package sourceloader

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/sourceformat"
	"git.saveweb.org/saveweb/hq/internal/sqlitequeue"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const loaderNow = int64(1_780_000_000)

func TestLoaderVerifiesETagAndStreamsIntoFencedSQLite(t *testing.T) {
	jobs := make(chan protocol.JobSpecV1, 1)
	jobs <- protocol.JobSpecV1{
		ID: "job-1", URL: "https://example.test/", Type: "seed", Via: nil, Attrs: map[string]any{},
	}
	close(jobs)
	var source bytes.Buffer
	if err := sourceformat.Encode(context.Background(), &source, jobs); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("ETag", `"source-etag"`)
		response.Header().Set("Content-Length", strconv.Itoa(source.Len()))
		_, _ = response.Write(source.Bytes())
	}))
	defer server.Close()
	store, err := sqlitequeue.Open(context.Background(), filepath.Join(t.TempDir(), "queue.sqlite"), queue.Identity{
		ProjectID: "project-1", ShardID: "shard-1", Generation: 1,
	}, sqlitequeue.WithClock(func() int64 { return loaderNow }))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SetFence(context.Background(), 1, loaderNow, loaderNow+120); err != nil {
		t.Fatal(err)
	}
	config := DefaultConfig()
	config.AllowHTTP = true
	config.Clock = func() int64 { return loaderNow }
	loader, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	format, uri, etag, download := sourceformat.FormatJobsJSONLZstdV1, "s3://bucket/source", "source-etag", server.URL
	expires := loaderNow + 60
	stats, err := loader.Load(context.Background(), protocol.OwnerAssignment{
		Route:     protocol.Route{ProjectID: "project-1", ShardID: "shard-1", Generation: 1},
		SourceURI: &uri, SourceFormat: &format, SourceETag: &etag,
		SourceDownloadURL: &download, SourceURLExpiresAt: &expires,
	}, store)
	if err != nil || stats.Jobs != 1 {
		t.Fatalf("load stats = %+v, %v", stats, err)
	}
	queueStats, err := store.Stats(context.Background())
	if err != nil || queueStats.Todo != 1 {
		t.Fatalf("queue stats = %+v, %v", queueStats, err)
	}
	wrong := "wrong"
	assignment := protocol.OwnerAssignment{
		Route:     protocol.Route{ProjectID: "project-1", ShardID: "shard-1", Generation: 1},
		SourceURI: &uri, SourceFormat: &format, SourceETag: &wrong,
		SourceDownloadURL: &download, SourceURLExpiresAt: &expires,
	}
	if _, err := loader.Load(context.Background(), assignment, store); err == nil {
		t.Fatal("changed ETag accepted")
	}
}

func TestLoaderNeverLeaksPresignedURLInErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Close()
	secretURL := server.URL + "/source?X-Amz-Signature=do-not-leak"
	config := DefaultConfig()
	config.AllowHTTP = true
	config.Clock = func() int64 { return loaderNow }
	loader, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	format, uri, etag := sourceformat.FormatJobsJSONLZstdV1, "s3://bucket/source", "etag"
	expires := loaderNow + 60
	_, err = loader.Load(context.Background(), protocol.OwnerAssignment{
		Route:     protocol.Route{ProjectID: "project-1", ShardID: "shard-1", Generation: 1},
		SourceURI: &uri, SourceFormat: &format, SourceETag: &etag,
		SourceDownloadURL: &secretURL, SourceURLExpiresAt: &expires,
	}, nil)
	if err == nil || strings.Contains(err.Error(), "do-not-leak") || strings.Contains(err.Error(), server.URL) {
		t.Fatalf("unsafe download error = %v", err)
	}
}
