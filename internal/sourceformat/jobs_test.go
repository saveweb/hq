package sourceformat

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func TestJobsJSONLZstdRoundTrip(t *testing.T) {
	via := "https://example.test/"
	input := []protocol.JobSpecV1{
		{ID: "a", Value: "https://example.test/a", Type: "seed", Via: nil, Attrs: map[string]any{}},
		{ID: "b", Value: "https://example.test/b", Type: "asset", Via: &via, Hops: 1, Attrs: map[string]any{"depth": 1}},
	}
	jobs := make(chan protocol.JobSpecV1, len(input))
	for _, job := range input {
		jobs <- job
	}
	close(jobs)
	var compressed bytes.Buffer
	if err := Encode(context.Background(), &compressed, jobs); err != nil {
		t.Fatal(err)
	}
	var output []queue.JobSpec
	stats, err := Decode(context.Background(), bytes.NewReader(compressed.Bytes()), Limits{
		MaxUncompressedBytes: 1 << 20, MaxJobs: 100,
	}, func(batch []queue.JobSpec) error {
		output = append(output, batch...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Jobs != 2 || len(output) != 2 || output[1].ID != "b" || output[1].Via == nil {
		t.Fatalf("stats=%+v output=%+v", stats, output)
	}
}

func TestDecodeRejectsUnknownField(t *testing.T) {
	var compressed bytes.Buffer
	encoder, err := zstd.NewWriter(&compressed)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = encoder.Write([]byte(`{"id":"a","value":"https://example.test","unknown":true}` + "\n"))
	_ = encoder.Close()
	if _, err := Decode(context.Background(), &compressed, Limits{
		MaxUncompressedBytes: 1 << 20, MaxJobs: 100,
	}, func([]queue.JobSpec) error { return nil }); err == nil {
		t.Fatal("line with unknown field accepted")
	}
}

func TestEncodeOmitsUnusedJobFields(t *testing.T) {
	var compressed bytes.Buffer
	encoder, err := NewEncoder(&compressed)
	if err != nil {
		t.Fatal(err)
	}
	if err := encoder.Write(protocol.JobSpecV1{Value: "https://example.test"}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	decoder, err := zstd.NewReader(&compressed)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	line, err := io.ReadAll(decoder)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(line), "{\"value\":\"https://example.test\"}\n"; got != want {
		t.Fatalf("encoded job = %s, want %s", got, want)
	}
}
