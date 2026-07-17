package checkpointupload

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"git.saveweb.org/saveweb/hq/internal/shard"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

type fakeSnapshotSource struct {
	target shard.CheckpointTarget
}

func (s fakeSnapshotSource) CheckpointTargets() []shard.CheckpointTarget {
	return []shard.CheckpointTarget{s.target}
}

func (fakeSnapshotSource) Snapshot(_ context.Context, _ shard.CheckpointTarget, destination string) error {
	return os.WriteFile(destination, []byte("compact sqlite snapshot"), 0o600)
}

type fakeControl struct {
	serverURL string
	begin     protocol.BeginCheckpointRequest
	part      protocol.CheckpointPartURLRequest
	complete  protocol.CompleteCheckpointRequest
}

func (c *fakeControl) BeginCheckpoint(
	_ context.Context, projectID, shardID string, request protocol.BeginCheckpointRequest,
) (protocol.BeginCheckpointResponse, error) {
	c.begin = request
	return protocol.BeginCheckpointResponse{
		ProjectID: projectID, ShardID: shardID, Generation: request.Generation,
		UploadID: "cp_test", PartSizeBytes: 5 << 20, CreatedAt: 100,
	}, nil
}

func (c *fakeControl) PresignCheckpointPart(
	_ context.Context, _, _, _ string, request protocol.CheckpointPartURLRequest,
) (protocol.CheckpointPartURLResponse, error) {
	c.part = request
	return protocol.CheckpointPartURLResponse{
		UploadID: "cp_test", PartNumber: request.PartNumber, URL: c.serverURL,
		Headers: map[string]string{"Content-Md5": request.ContentMD5}, ExpiresAt: 200,
	}, nil
}

func (c *fakeControl) CompleteCheckpoint(
	_ context.Context, projectID, shardID, _ string, request protocol.CompleteCheckpointRequest,
) (protocol.CheckpointResponse, error) {
	c.complete = request
	return protocol.CheckpointResponse{
		ProjectID: projectID, ShardID: shardID, Generation: request.Generation,
		Sequence: 1, URI: "s3://bucket/checkpoint", Format: "sqlite-zstd-v1",
		SHA256: c.begin.SHA256, SizeBytes: c.begin.SizeBytes, CreatedAt: 101,
	}, nil
}

func TestUploaderStreamsPresignedPartsAndPublishes(t *testing.T) {
	var uploaded []byte
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
		}
		digest := md5.Sum(body)
		if request.Method != http.MethodPut ||
			request.Header.Get("Content-Md5") != base64.StdEncoding.EncodeToString(digest[:]) {
			t.Errorf("invalid upload request: method=%s headers=%+v", request.Method, request.Header)
		}
		uploaded = append(uploaded, body...)
		response.Header().Set("ETag", `"part-1"`)
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	target := shard.CheckpointTarget{ProjectID: "project-1", ShardID: "shard-1", Generation: 3}
	control := &fakeControl{serverURL: server.URL}
	uploader, err := New(fakeSnapshotSource{target: target}, control, Config{
		WorkDir: t.TempDir(), AllowHTTP: true, HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	results := uploader.RunOnce(context.Background())
	if len(results) != 1 || results[0].Err != nil || results[0].Checkpoint.Sequence != 1 {
		t.Fatalf("results = %+v", results)
	}
	if len(uploaded) == 0 || control.begin.SizeBytes != int64(len(uploaded)) ||
		control.part.SizeBytes != int64(len(uploaded)) || len(control.complete.Parts) != 1 ||
		control.complete.Parts[0].ETag != `"part-1"` {
		t.Fatalf("begin=%+v part=%+v complete=%+v uploaded=%d", control.begin, control.part, control.complete, len(uploaded))
	}
}
