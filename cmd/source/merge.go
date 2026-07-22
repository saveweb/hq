package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/saveweb/hq/internal/queue"
	"github.com/saveweb/hq/internal/sourceformat"
	"github.com/saveweb/hq/pkg/protocol"
)

const (
	defaultMergeJobsPerFile = int64(100_000)
	defaultMergeMaxJobs     = int64(10_000_000)
	defaultMergeMaxBytes    = int64(16 << 30)
	maxMergeJobs            = int64(100_000_000)
	maxMergeBytes           = int64(1 << 40)
)

type inputPaths []string

func (p *inputPaths) String() string { return strings.Join(*p, ",") }

func (p *inputPaths) Set(value string) error {
	if value == "" {
		return fmt.Errorf("input path is empty")
	}
	*p = append(*p, value)
	return nil
}

type mergeConfig struct {
	Inputs               []string
	OutputPrefix         string
	JobsPerFile          int64
	MaxJobs              int64
	MaxUncompressedBytes int64
}

type mergeStats struct {
	InputJobs         int64
	UniqueJobs        int64
	DuplicateJobs     int64
	UncompressedBytes int64
	OutputFiles       int
}

func runMerge(args []string) error {
	flags := flag.NewFlagSet("merge", flag.ContinueOnError)
	var inputs inputPaths
	flags.Var(&inputs, "input", "jobs-jsonl-zstd-v1 input file; repeat in deterministic merge order, or use - once")
	outputPrefix := flags.String("output-prefix", "", "new output file prefix")
	jobsPerFile := flags.Int64("jobs-per-file", defaultMergeJobsPerFile, "maximum unique jobs per output source")
	maxJobs := flags.Int64("max-jobs", defaultMergeMaxJobs, "maximum input jobs including duplicates")
	maxBytes := flags.Int64("max-uncompressed-bytes", defaultMergeMaxBytes, "maximum total decoded bytes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	config := mergeConfig{
		Inputs: append([]string(nil), inputs...), OutputPrefix: *outputPrefix,
		JobsPerFile: *jobsPerFile, MaxJobs: *maxJobs, MaxUncompressedBytes: *maxBytes,
	}
	if flags.NArg() != 0 || len(config.Inputs) == 0 || len(config.Inputs) > 100_000 ||
		config.OutputPrefix == "" || config.OutputPrefix == "-" ||
		config.JobsPerFile < 1 || config.JobsPerFile > config.MaxJobs ||
		config.MaxJobs < 1 || config.MaxJobs > maxMergeJobs ||
		config.MaxUncompressedBytes < 1 || config.MaxUncompressedBytes > maxMergeBytes {
		return fmt.Errorf("source merge: invalid inputs, output prefix, or limits")
	}
	stdinCount := 0
	for _, path := range config.Inputs {
		if path == "-" {
			stdinCount++
		}
	}
	if stdinCount > 1 {
		return fmt.Errorf("source merge: stdin can be used only once")
	}
	stats, err := mergeSources(context.Background(), config)
	if err != nil {
		return err
	}
	fmt.Printf("inputs=%d jobs=%d unique=%d duplicates=%d outputs=%d\n",
		len(config.Inputs), stats.InputJobs, stats.UniqueJobs, stats.DuplicateJobs, stats.OutputFiles)
	return nil
}

func mergeSources(ctx context.Context, config mergeConfig) (mergeStats, error) {
	outputs := newSplitOutputs(config.OutputPrefix, config.JobsPerFile)
	committed := false
	defer func() {
		if !committed {
			outputs.cleanup()
		}
	}()
	seen := make(map[string][sha256.Size]byte)
	var stats mergeStats
	for _, path := range config.Inputs {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		remainingJobs := config.MaxJobs - stats.InputJobs
		remainingBytes := config.MaxUncompressedBytes - stats.UncompressedBytes
		if remainingJobs < 1 || remainingBytes < 1 {
			return stats, fmt.Errorf("source merge: aggregate input exceeds configured limits")
		}
		input, closeInput, err := openMergeInput(path)
		if err != nil {
			return stats, err
		}
		decoded, decodeErr := sourceformat.Decode(ctx, input, sourceformat.Limits{
			MaxUncompressedBytes: remainingBytes, MaxJobs: remainingJobs,
		}, func(batch []queue.JobSpec) error {
			for _, job := range batch {
				stats.InputJobs++
				if job.ID == "" {
					return &queue.Error{Code: protocol.ErrorInvalidJob, Message: "source merge requires job IDs"}
				}
				normalized, validation := queue.NormalizeJob(job)
				if validation != nil {
					return validation
				}
				identity := mergeIdentity(normalized)
				if existing, ok := seen[job.ID]; ok {
					if existing != identity {
						return &queue.Error{
							Code:    protocol.ErrorIdentityConflict,
							Message: "source merge: job ID has different type, value, or attributes",
							Details: map[string]any{"job_id": job.ID},
						}
					}
					stats.DuplicateJobs++
					continue
				}
				seen[job.ID] = identity
				if err := outputs.write(protocol.JobSpecV1{
					ID: normalized.ID, Value: normalized.Value, Type: normalized.Type,
					Via: normalized.Via, Hops: normalized.Hops, Attrs: normalized.Attrs,
				}); err != nil {
					return err
				}
				stats.UniqueJobs++
			}
			return nil
		})
		closeErr := closeInput()
		if decodeErr != nil {
			return stats, fmt.Errorf("source merge: decode %q: %w", path, decodeErr)
		}
		if closeErr != nil {
			return stats, fmt.Errorf("source merge: close %q: %w", path, closeErr)
		}
		stats.UncompressedBytes += decoded.UncompressedBytes
	}
	if stats.UniqueJobs == 0 {
		return stats, fmt.Errorf("source merge: inputs contain no jobs")
	}
	if err := outputs.finish(); err != nil {
		return stats, err
	}
	stats.OutputFiles = len(outputs.paths)
	committed = true
	return stats, nil
}

func mergeIdentity(job queue.NormalizedJob) [sha256.Size]byte {
	encoded, _ := json.Marshal([3]string{job.Type, job.Value, job.AttrsJSON})
	return sha256.Sum256(encoded)
}

func openMergeInput(path string) (io.Reader, func() error, error) {
	if path == "-" {
		return os.Stdin, func() error { return nil }, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("source merge: open input %q: %w", path, err)
	}
	return file, file.Close, nil
}

type splitOutputs struct {
	prefix      string
	jobsPerFile int64
	paths       []string
	current     *splitOutput
}

type splitOutput struct {
	file    *os.File
	encoder *sourceformat.Encoder
	jobs    int64
}

func newSplitOutputs(prefix string, jobsPerFile int64) *splitOutputs {
	return &splitOutputs{prefix: prefix, jobsPerFile: jobsPerFile}
}

func (o *splitOutputs) write(job protocol.JobSpecV1) error {
	if o.current == nil {
		path := fmt.Sprintf("%s-%06d.jobs.jsonl.zst", o.prefix, len(o.paths)+1)
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return fmt.Errorf("source merge: create output %q: %w", path, err)
		}
		o.paths = append(o.paths, path)
		encoder, err := sourceformat.NewEncoder(file)
		if err != nil {
			_ = file.Close()
			return err
		}
		o.current = &splitOutput{file: file, encoder: encoder}
	}
	if err := o.current.encoder.Write(job); err != nil {
		return err
	}
	o.current.jobs++
	if o.current.jobs == o.jobsPerFile {
		return o.closeCurrent()
	}
	return nil
}

func (o *splitOutputs) finish() error { return o.closeCurrent() }

func (o *splitOutputs) closeCurrent() error {
	if o.current == nil {
		return nil
	}
	current := o.current
	o.current = nil
	if err := current.encoder.Close(); err != nil {
		_ = current.file.Close()
		return err
	}
	if err := current.file.Sync(); err != nil {
		_ = current.file.Close()
		return fmt.Errorf("source merge: sync output: %w", err)
	}
	if err := current.file.Close(); err != nil {
		return fmt.Errorf("source merge: close output: %w", err)
	}
	return nil
}

func (o *splitOutputs) cleanup() {
	if o.current != nil {
		_ = o.current.encoder.Close()
		_ = o.current.file.Close()
		o.current = nil
	}
	for _, path := range o.paths {
		_ = os.Remove(path)
	}
}
