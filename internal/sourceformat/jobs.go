// Package sourceformat implements the immutable jobs-jsonl-zstd-v1 source
// format shared by the source packer and HQ importer.
package sourceformat

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const (
	FormatJobsJSONLZstdV1 = "jobs-jsonl-zstd-v1"
	maxLineBytes          = 64 << 10
)

type Limits struct {
	MaxUncompressedBytes int64
	MaxJobs              int64
}

type DecodeStats struct {
	Jobs              int64
	UncompressedBytes int64
}

type Encoder struct {
	zstd   *zstd.Encoder
	json   *json.Encoder
	closed bool
}

func NewEncoder(output io.Writer) (*Encoder, error) {
	if output == nil {
		return nil, fmt.Errorf("source format: output is required")
	}
	encoder, err := zstd.NewWriter(output,
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(6)),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		return nil, fmt.Errorf("source format: create zstd encoder: %w", err)
	}
	jsonEncoder := json.NewEncoder(encoder)
	jsonEncoder.SetEscapeHTML(false)
	return &Encoder{zstd: encoder, json: jsonEncoder}, nil
}

func (e *Encoder) Write(job protocol.JobSpecV1) error {
	if e == nil || e.closed {
		return fmt.Errorf("source format: encoder is closed")
	}
	if _, err := normalizeProtocolJob(job); err != nil {
		return err
	}
	if job.Type == "" {
		job.Type = protocol.JobTypeSeed
	}
	if job.Attrs == nil {
		job.Attrs = map[string]any{}
	}
	if err := e.json.Encode(job); err != nil {
		return fmt.Errorf("source format: encode job: %w", err)
	}
	return nil
}

func (e *Encoder) Close() error {
	if e == nil || e.closed {
		return nil
	}
	e.closed = true
	if err := e.zstd.Close(); err != nil {
		return fmt.Errorf("source format: close zstd encoder: %w", err)
	}
	return nil
}

func Encode(ctx context.Context, output io.Writer, jobs <-chan protocol.JobSpecV1) error {
	encoder, err := NewEncoder(output)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			_ = encoder.Close()
			return ctx.Err()
		case job, ok := <-jobs:
			if !ok {
				return encoder.Close()
			}
			if err := encoder.Write(job); err != nil {
				_ = encoder.Close()
				return err
			}
		}
	}
}

func Decode(
	ctx context.Context,
	input io.Reader,
	limits Limits,
	consume func([]queue.JobSpec) error,
) (DecodeStats, error) {
	if limits.MaxUncompressedBytes < 1 || limits.MaxJobs < 1 || consume == nil {
		return DecodeStats{}, fmt.Errorf("source format: invalid decode limits")
	}
	decoder, err := zstd.NewReader(input,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(128<<20),
		zstd.WithDecoderMaxWindow(64<<20),
	)
	if err != nil {
		return DecodeStats{}, fmt.Errorf("source format: create zstd decoder: %w", err)
	}
	defer decoder.Close()
	limited := &io.LimitedReader{R: decoder, N: limits.MaxUncompressedBytes + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 64<<10), maxLineBytes)
	batch := make([]queue.JobSpec, 0, 1000)
	var stats DecodeStats
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := consume(batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if !utf8.Valid(line) {
			return stats, fmt.Errorf("source format: line %d is not UTF-8", stats.Jobs+1)
		}
		job, err := decodeJob(line)
		if err != nil {
			return stats, fmt.Errorf("source format: line %d: %w", stats.Jobs+1, err)
		}
		batch = append(batch, job)
		stats.Jobs++
		if stats.Jobs > limits.MaxJobs {
			return stats, fmt.Errorf("source format: source exceeds job limit")
		}
		if len(batch) == cap(batch) {
			if err := flush(); err != nil {
				return stats, err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return stats, fmt.Errorf("source format: decode JSON Lines: %w", err)
	}
	stats.UncompressedBytes = limits.MaxUncompressedBytes + 1 - limited.N
	if stats.UncompressedBytes > limits.MaxUncompressedBytes {
		return stats, fmt.Errorf("source format: source exceeds uncompressed size limit")
	}
	if err := flush(); err != nil {
		return stats, err
	}
	return stats, nil
}

func decodeJob(line []byte) (queue.JobSpec, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(line, &fields); err != nil {
		return queue.JobSpec{}, err
	}
	for _, required := range []string{"id", "url", "via"} {
		if _, ok := fields[required]; !ok {
			return queue.JobSpec{}, fmt.Errorf("required field %q is missing", required)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var wire protocol.JobSpecV1
	if err := decoder.Decode(&wire); err != nil {
		return queue.JobSpec{}, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return queue.JobSpec{}, fmt.Errorf("line must contain one JSON object")
	}
	return normalizeProtocolJob(wire)
}

func normalizeProtocolJob(job protocol.JobSpecV1) (queue.JobSpec, error) {
	value, validation := queue.NormalizeJob(queue.JobSpec{
		ID: job.ID, URL: job.URL, Type: job.Type, Via: job.Via,
		Hops: job.Hops, Attrs: job.Attrs,
	})
	if validation != nil {
		return queue.JobSpec{}, validation
	}
	return value.JobSpec, nil
}
