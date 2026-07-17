// Package checkpointupload creates and publishes SQLite checkpoints without
// proxying object bytes through the tracker.
package checkpointupload

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"git.saveweb.org/saveweb/hq/internal/checkpointformat"
	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/shard"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const maxPartMemory = int64(64 << 20)

type Control interface {
	BeginCheckpoint(context.Context, string, string, protocol.BeginCheckpointRequest) (protocol.BeginCheckpointResponse, error)
	PresignCheckpointPart(context.Context, string, string, string, protocol.CheckpointPartURLRequest) (protocol.CheckpointPartURLResponse, error)
	CompleteCheckpoint(context.Context, string, string, string, protocol.CompleteCheckpointRequest) (protocol.CheckpointResponse, error)
}

type SnapshotSource interface {
	CheckpointTargets() []shard.CheckpointTarget
	Snapshot(context.Context, shard.CheckpointTarget, string) error
}

type Config struct {
	WorkDir     string
	AllowHTTP   bool
	HTTPClient  *http.Client
	MaxAttempts int
}

type Uploader struct {
	source  SnapshotSource
	control Control
	config  Config
}

type Result struct {
	Target     shard.CheckpointTarget
	Checkpoint *protocol.CheckpointResponse
	Err        error
}

func New(source SnapshotSource, control Control, config Config) (*Uploader, error) {
	if source == nil || control == nil || config.WorkDir == "" {
		return nil, fmt.Errorf("checkpoint upload: invalid configuration")
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = 3
	}
	if config.MaxAttempts < 1 || config.MaxAttempts > 10 {
		return nil, fmt.Errorf("checkpoint upload: invalid retry limit")
	}
	if config.HTTPClient == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.ResponseHeaderTimeout = 30 * time.Second
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		} else {
			transport.TLSClientConfig = transport.TLSClientConfig.Clone()
			transport.TLSClientConfig.MinVersion = tls.VersionTLS12
		}
		config.HTTPClient = &http.Client{
			Transport:     transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	return &Uploader{source: source, control: control, config: config}, nil
}

func (u *Uploader) RunOnce(ctx context.Context) []Result {
	targets := u.source.CheckpointTargets()
	results := make([]Result, 0, len(targets))
	for _, target := range targets {
		checkpoint, err := u.Upload(ctx, target)
		results = append(results, Result{Target: target, Checkpoint: checkpoint, Err: err})
		if ctx.Err() != nil {
			break
		}
	}
	return results
}

func (u *Uploader) Upload(ctx context.Context, target shard.CheckpointTarget) (*protocol.CheckpointResponse, error) {
	snapshot := targetSnapshot{source: u.source, target: target}
	artifact, err := checkpointformat.Create(ctx, snapshot, u.config.WorkDir)
	if err != nil {
		return nil, err
	}
	defer artifact.Cleanup()
	begin, err := u.control.BeginCheckpoint(ctx, target.ProjectID, target.ShardID, protocol.BeginCheckpointRequest{
		Generation: target.Generation, SizeBytes: artifact.SizeBytes, SHA256: artifact.SHA256,
	})
	if err != nil {
		return nil, err
	}
	if begin.Generation != target.Generation || begin.UploadID == "" ||
		begin.PartSizeBytes < 5<<20 || begin.PartSizeBytes > maxPartMemory {
		return nil, fmt.Errorf("checkpoint upload: tracker returned invalid upload parameters")
	}
	parts, err := u.uploadParts(ctx, target, begin, artifact.Path, artifact.SizeBytes)
	if err != nil {
		return nil, err
	}
	checkpoint, err := u.control.CompleteCheckpoint(ctx, target.ProjectID, target.ShardID, begin.UploadID,
		protocol.CompleteCheckpointRequest{Generation: target.Generation, Parts: parts})
	if err != nil {
		return nil, err
	}
	return &checkpoint, nil
}

func (u *Uploader) uploadParts(
	ctx context.Context,
	target shard.CheckpointTarget,
	begin protocol.BeginCheckpointResponse,
	path string,
	totalSize int64,
) ([]protocol.CheckpointPart, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("checkpoint upload: open artifact: %w", err)
	}
	defer file.Close()
	parts := make([]protocol.CheckpointPart, 0, (totalSize+begin.PartSizeBytes-1)/begin.PartSizeBytes)
	buffer := make([]byte, int(begin.PartSizeBytes))
	for partNumber, offset := int32(1), int64(0); offset < totalSize; partNumber++ {
		if partNumber > 10_000 {
			return nil, fmt.Errorf("checkpoint upload: checkpoint requires too many parts")
		}
		size := min(begin.PartSizeBytes, totalSize-offset)
		part := buffer[:int(size)]
		if _, err := io.ReadFull(file, part); err != nil {
			return nil, fmt.Errorf("checkpoint upload: read part: %w", err)
		}
		etag, err := u.uploadPart(ctx, target, begin.UploadID, partNumber, part)
		if err != nil {
			return nil, err
		}
		parts = append(parts, protocol.CheckpointPart{PartNumber: partNumber, ETag: etag})
		offset += size
	}
	return parts, nil
}

func (u *Uploader) uploadPart(
	ctx context.Context,
	target shard.CheckpointTarget,
	uploadID string,
	partNumber int32,
	data []byte,
) (string, error) {
	digest := md5.Sum(data)
	contentMD5 := base64.StdEncoding.EncodeToString(digest[:])
	for attempt := 1; attempt <= u.config.MaxAttempts; attempt++ {
		part, err := u.control.PresignCheckpointPart(ctx, target.ProjectID, target.ShardID, uploadID,
			protocol.CheckpointPartURLRequest{
				Generation: target.Generation, PartNumber: partNumber,
				SizeBytes: int64(len(data)), ContentMD5: contentMD5,
			})
		if err != nil {
			return "", err
		}
		etag, err := u.putPart(ctx, part, data)
		if err == nil {
			return etag, nil
		}
		if ctx.Err() != nil || attempt == u.config.MaxAttempts {
			return "", err
		}
	}
	return "", fmt.Errorf("checkpoint upload: part retries exhausted")
}

func (u *Uploader) putPart(ctx context.Context, part protocol.CheckpointPartURLResponse, data []byte) (string, error) {
	parsed, err := url.Parse(part.URL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" ||
		(parsed.Scheme != "https" && !(u.config.AllowHTTP && parsed.Scheme == "http")) {
		return "", fmt.Errorf("checkpoint upload: invalid part URL")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, parsed.String(), bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("checkpoint upload: cannot create part request")
	}
	request.ContentLength = int64(len(data))
	for name, value := range part.Headers {
		if name == "" || value == "" || http.CanonicalHeaderKey(name) == "Host" ||
			containsNewline(name) || containsNewline(value) {
			return "", fmt.Errorf("checkpoint upload: invalid signed header")
		}
		request.Header.Set(name, value)
	}
	response, err := u.config.HTTPClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		// net/http includes the presigned URL in its error. Do not leak it.
		return "", fmt.Errorf("checkpoint upload: part request failed")
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("checkpoint upload: object server returned HTTP %d", response.StatusCode)
	}
	etag := response.Header.Get("ETag")
	if etag == "" || len(etag) > 512 || containsNewline(etag) {
		return "", fmt.Errorf("checkpoint upload: object server omitted part ETag")
	}
	return etag, nil
}

func containsNewline(value string) bool {
	for _, character := range value {
		if character == '\r' || character == '\n' {
			return true
		}
	}
	return false
}

type targetSnapshot struct {
	source SnapshotSource
	target shard.CheckpointTarget
}

func (s targetSnapshot) Snapshot(ctx context.Context, destination string) error {
	return s.source.Snapshot(ctx, s.target, destination)
}

var _ queue.Snapshotter = targetSnapshot{}
