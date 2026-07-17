package sourceformat

import (
	"bytes"
	"context"
	"testing"

	"github.com/klauspost/compress/zstd"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func TestJobsJSONLZstdRoundTrip(t *testing.T) {
	via := "https://example.test/"
	input := []protocol.JobSpecV1{
		{ID: "a", URL: "https://example.test/a", Type: "seed", Via: nil, Attrs: map[string]any{}},
		{ID: "b", URL: "https://example.test/b", Type: "asset", Via: &via, Hops: 1, Attrs: map[string]any{"depth": 1}},
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

func TestDecodeRejectsUnknownFieldAndLimit(t *testing.T) {
	for _, line := range []string{
		`{"id":"a","url":"https://example.test","via":null,"unknown":true}`,
		`{"id":"a","url":"https://example.test"}`,
	} {
		var compressed bytes.Buffer
		encoder, err := zstd.NewWriter(&compressed)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = encoder.Write([]byte(line + "\n"))
		_ = encoder.Close()
		if _, err := Decode(context.Background(), &compressed, Limits{
			MaxUncompressedBytes: 1 << 20, MaxJobs: 100,
		}, func([]queue.JobSpec) error { return nil }); err == nil {
			t.Fatalf("invalid line accepted: %s", line)
		}
	}
}
