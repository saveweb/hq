package receiveringest_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/receiveringest"
	"git.saveweb.org/saveweb/hq/internal/sourceformat"
	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

type memoryObjects struct {
	uri  string
	body []byte
}

func (m *memoryObjects) Put(_ context.Context, uri string, body *bytes.Reader, size int64, contentType string) (string, error) {
	if body.Size() != size || contentType != "application/zstd" {
		return "", io.ErrUnexpectedEOF
	}
	m.uri = uri
	m.body, _ = io.ReadAll(body)
	return "receiver-etag", nil
}

func (m *memoryObjects) Head(_ context.Context, uri string) (int64, string, error) {
	if uri != m.uri {
		return 0, "", io.ErrUnexpectedEOF
	}
	return int64(len(m.body)), "receiver-etag", nil
}

func TestWriterProducesReusableImmutableSourceObject(t *testing.T) {
	objects := &memoryObjects{}
	writer, err := receiveringest.New(receiveringest.Config{Store: objects, MaxObjectBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	result, err := writer.Write(context.Background(), tracker.Receiver{
		ProjectID: "project-1", ID: "receiver-1", Status: tracker.ReceiverStatusActive,
		SinkURI: "s3://receiver-output/stage-1", Format: sourceformat.FormatJobsJSONLZstdV1,
	}, []protocol.JobSpecV1{{
		ID: "job-1", URL: "https://example.test/", Type: protocol.JobTypeSeed, Via: nil,
		Attrs: map[string]any{"stage": 2},
	}}, 1_780_200_000)
	if err != nil {
		t.Fatal(err)
	}
	if result.JobsCount != 1 || result.SizeBytes != int64(len(objects.body)) ||
		!strings.HasSuffix(result.ObjectURI, ".jobs.jsonl.zst") || len(result.SHA256) != 64 {
		t.Fatalf("result = %+v", result)
	}
	var decoded int
	stats, err := sourceformat.Decode(context.Background(), bytes.NewReader(objects.body), sourceformat.Limits{
		MaxUncompressedBytes: 1 << 20, MaxJobs: 10,
	}, func(batch []queue.JobSpec) error {
		decoded += len(batch)
		return nil
	})
	if err != nil || stats.Jobs != 1 || decoded != 1 {
		t.Fatalf("decoded=%d stats=%+v error=%v", decoded, stats, err)
	}
}
