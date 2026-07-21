package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/sourceformat"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func TestPackJobsTextProducesStableSource(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "jobs.txt")
	output := filepath.Join(directory, "jobs.jobs.jsonl.zst")
	if err := os.WriteFile(input, []byte("https://example.test/a\n\nhttps://example.test/b\r\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"pack", "--input", input, "--output", output}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var jobs []queue.JobSpec
	stats, err := sourceformat.Decode(context.Background(), bytes.NewReader(raw), sourceformat.Limits{
		MaxUncompressedBytes: 1 << 20, MaxJobs: 10,
	}, func(batch []queue.JobSpec) error {
		jobs = append(jobs, batch...)
		return nil
	})
	if err != nil || stats.Jobs != 2 || len(jobs) != 2 {
		t.Fatalf("stats=%+v jobs=%+v error=%v", stats, jobs, err)
	}
	if jobs[0].ID != protocol.DefaultJobID(protocol.JobTypeSeed, jobs[0].Value) {
		t.Fatalf("job ID = %q", jobs[0].ID)
	}
	if err := run([]string{"pack", "--input", input, "--output", output}); err == nil {
		t.Fatal("pack overwrote an existing source")
	}
}

func TestPackUniqueValueOmitsExternalID(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "jobs.txt")
	output := filepath.Join(directory, "jobs.jobs.jsonl.zst")
	if err := os.WriteFile(input, []byte("simple-value\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"pack", "--identity-mode", "unique_value", "--input", input, "--output", output}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var jobs []queue.JobSpec
	_, err = sourceformat.Decode(context.Background(), bytes.NewReader(raw), sourceformat.Limits{MaxUncompressedBytes: 1 << 20, MaxJobs: 10}, func(batch []queue.JobSpec) error {
		jobs = append(jobs, batch...)
		return nil
	})
	if err != nil || len(jobs) != 1 || jobs[0].ID != "" || jobs[0].Value != "simple-value" {
		t.Fatalf("jobs=%+v error=%v", jobs, err)
	}
}
