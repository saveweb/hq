// Package receiveringest writes one immutable jobs-jsonl-zstd-v1 object per
// accepted receiver batch. It deliberately has no queue or claim behavior.
package receiveringest

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"git.saveweb.org/saveweb/hq/internal/objectstorage"
	"git.saveweb.org/saveweb/hq/internal/objectstore"
	"git.saveweb.org/saveweb/hq/internal/sourceformat"
	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

type Config struct {
	Store          objectstorage.ObjectWriter
	MaxObjectBytes int64
}

type Writer struct {
	store          objectstorage.ObjectWriter
	maxObjectBytes int64
}

func New(config Config) (*Writer, error) {
	if config.Store == nil || config.MaxObjectBytes < 1 || config.MaxObjectBytes > 1<<30 {
		return nil, fmt.Errorf("receiver ingest: invalid configuration")
	}
	return &Writer{store: config.Store, maxObjectBytes: config.MaxObjectBytes}, nil
}

func (w *Writer) Write(
	ctx context.Context,
	target tracker.Receiver,
	jobs []protocol.JobSpecV1,
	now int64,
) (tracker.ReceiverObject, error) {
	if target.ProjectID == "" || target.ID == "" || target.Status != tracker.ReceiverStatusActive ||
		target.Format != sourceformat.FormatJobsJSONLZstdV1 || now < 0 || strings.HasSuffix(target.SinkURI, "/") {
		return tracker.ReceiverObject{}, fmt.Errorf("receiver ingest: invalid target")
	}
	if _, err := objectstore.ParseURI(target.SinkURI + "/probe"); err != nil {
		return tracker.ReceiverObject{}, fmt.Errorf("receiver ingest: invalid sink URI")
	}
	jobChannel := make(chan protocol.JobSpecV1, len(jobs))
	for _, job := range jobs {
		jobChannel <- job
	}
	close(jobChannel)
	var body bytes.Buffer
	limited := &limitedBuffer{buffer: &body, remaining: w.maxObjectBytes}
	if err := sourceformat.Encode(ctx, limited, jobChannel); err != nil {
		return tracker.ReceiverObject{}, err
	}
	if body.Len() == 0 || int64(body.Len()) > w.maxObjectBytes {
		return tracker.ReceiverObject{}, fmt.Errorf("receiver ingest: encoded object exceeds size limit")
	}
	objectID, err := randomObjectID()
	if err != nil {
		return tracker.ReceiverObject{}, err
	}
	project := base64.RawURLEncoding.EncodeToString([]byte(target.ProjectID))
	receiver := base64.RawURLEncoding.EncodeToString([]byte(target.ID))
	uri := target.SinkURI + "/" + project + "/" + receiver + "/" + objectID + ".jobs.jsonl.zst"
	size := int64(body.Len())
	etag, err := w.store.Put(ctx, uri, bytes.NewReader(body.Bytes()), size, "application/zstd")
	if err != nil {
		return tracker.ReceiverObject{}, fmt.Errorf("receiver ingest: put object: %w", err)
	}
	headSize, headETag, err := w.store.Head(ctx, uri)
	if err != nil {
		return tracker.ReceiverObject{}, fmt.Errorf("receiver ingest: verify object: %w", err)
	}
	if headSize != size || etag == "" || headETag != etag {
		return tracker.ReceiverObject{}, fmt.Errorf("receiver ingest: object verification mismatch")
	}
	digest := sha256.Sum256(body.Bytes())
	return tracker.ReceiverObject{
		ProjectID: target.ProjectID, ReceiverID: target.ID,
		ObjectURI: uri, Format: target.Format, JobsCount: int64(len(jobs)),
		SizeBytes: size, SHA256: hex.EncodeToString(digest[:]), CreatedAt: now,
	}, nil
}

type limitedBuffer struct {
	buffer    *bytes.Buffer
	remaining int64
}

func (w *limitedBuffer) Write(value []byte) (int, error) {
	if int64(len(value)) > w.remaining {
		return 0, fmt.Errorf("receiver ingest: encoded object exceeds size limit")
	}
	written, err := w.buffer.Write(value)
	w.remaining -= int64(written)
	return written, err
}

func randomObjectID() (string, error) {
	var value [18]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("receiver ingest: random object ID: %w", err)
	}
	return "rb_" + base64.RawURLEncoding.EncodeToString(value[:]), nil
}
