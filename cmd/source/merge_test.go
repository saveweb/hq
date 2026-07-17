package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/sourceformat"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func TestMergeDeduplicatesInInputOrderAndSplits(t *testing.T) {
	directory := t.TempDir()
	via := "https://different-discovery.test/"
	first := filepath.Join(directory, "receiver-1.jobs.jsonl.zst")
	second := filepath.Join(directory, "receiver-2.jobs.jsonl.zst")
	writeSource(t, first, []protocol.JobSpecV1{
		{ID: "job-a", URL: "https://example.test/a", Type: protocol.JobTypeSeed, Via: nil, Attrs: map[string]any{"stage": 2}},
		{ID: "job-b", URL: "https://example.test/b", Type: protocol.JobTypeAsset, Via: nil, Attrs: map[string]any{}},
	})
	writeSource(t, second, []protocol.JobSpecV1{
		{ID: "job-a", URL: "https://example.test/a", Type: protocol.JobTypeSeed, Via: &via, Hops: 9, Attrs: map[string]any{"stage": 2}},
		{ID: "job-c", URL: "https://example.test/c", Type: protocol.JobTypeSeed, Via: &via, Hops: 1, Attrs: map[string]any{}},
	})
	prefix := filepath.Join(directory, "stage-2")
	stats, err := mergeSources(context.Background(), mergeConfig{
		Inputs: []string{first, second}, OutputPrefix: prefix,
		JobsPerFile: 2, MaxJobs: 100, MaxUncompressedBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.InputJobs != 4 || stats.UniqueJobs != 3 || stats.DuplicateJobs != 1 || stats.OutputFiles != 2 {
		t.Fatalf("stats = %+v", stats)
	}
	var merged []queue.JobSpec
	for index := 1; index <= 2; index++ {
		merged = append(merged, readSource(t, fmtOutput(prefix, index))...)
	}
	if len(merged) != 3 || merged[0].ID != "job-a" || merged[1].ID != "job-b" || merged[2].ID != "job-c" {
		t.Fatalf("merged jobs = %+v", merged)
	}
	if merged[0].Via != nil || merged[0].Hops != 0 {
		t.Fatalf("first occurrence was not retained: %+v", merged[0])
	}
}

func TestMergeRejectsIdentityConflictAndCleansOutputs(t *testing.T) {
	directory := t.TempDir()
	first := filepath.Join(directory, "first.zst")
	second := filepath.Join(directory, "second.zst")
	writeSource(t, first, []protocol.JobSpecV1{{
		ID: "same-id", URL: "https://example.test/a", Type: protocol.JobTypeSeed, Via: nil, Attrs: map[string]any{},
	}})
	writeSource(t, second, []protocol.JobSpecV1{{
		ID: "same-id", URL: "https://example.test/different", Type: protocol.JobTypeSeed, Via: nil, Attrs: map[string]any{},
	}})
	prefix := filepath.Join(directory, "conflict")
	_, err := mergeSources(context.Background(), mergeConfig{
		Inputs: []string{first, second}, OutputPrefix: prefix,
		JobsPerFile: 10, MaxJobs: 100, MaxUncompressedBytes: 1 << 20,
	})
	var conflict *queue.Error
	if !errors.As(err, &conflict) || conflict.Code != protocol.ErrorIdentityConflict {
		t.Fatalf("merge error = %v", err)
	}
	outputs, globErr := filepath.Glob(prefix + "-*.jobs.jsonl.zst")
	if globErr != nil || len(outputs) != 0 {
		t.Fatalf("partial outputs = %v, error = %v", outputs, globErr)
	}
}

func TestMergeNeverOverwritesAndRemovesEarlierSplit(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "input.zst")
	writeSource(t, input, []protocol.JobSpecV1{
		{ID: "job-a", URL: "https://example.test/a", Type: protocol.JobTypeSeed, Via: nil, Attrs: map[string]any{}},
		{ID: "job-b", URL: "https://example.test/b", Type: protocol.JobTypeSeed, Via: nil, Attrs: map[string]any{}},
	})
	prefix := filepath.Join(directory, "reserved")
	reserved := fmtOutput(prefix, 2)
	if err := os.WriteFile(reserved, []byte("do not overwrite"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := mergeSources(context.Background(), mergeConfig{
		Inputs: []string{input}, OutputPrefix: prefix,
		JobsPerFile: 1, MaxJobs: 100, MaxUncompressedBytes: 1 << 20,
	})
	if err == nil {
		t.Fatal("merge overwrote a reserved output")
	}
	if _, statErr := os.Stat(fmtOutput(prefix, 1)); !os.IsNotExist(statErr) {
		t.Fatalf("first partial split still exists: %v", statErr)
	}
	content, readErr := os.ReadFile(reserved)
	if readErr != nil || string(content) != "do not overwrite" {
		t.Fatalf("reserved output changed: %q, %v", content, readErr)
	}
}

func writeSource(t *testing.T, path string, jobs []protocol.JobSpecV1) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := sourceformat.NewEncoder(file)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	for _, job := range jobs {
		if err := encoder.Write(job); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
	}
	if err := encoder.Close(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func readSource(t *testing.T, path string) []queue.JobSpec {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var result []queue.JobSpec
	_, err = sourceformat.Decode(context.Background(), bytes.NewReader(raw), sourceformat.Limits{
		MaxUncompressedBytes: 1 << 20, MaxJobs: 100,
	}, func(batch []queue.JobSpec) error {
		result = append(result, batch...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func fmtOutput(prefix string, index int) string {
	return prefix + "-" + fmt.Sprintf("%06d", index) + ".jobs.jsonl.zst"
}
