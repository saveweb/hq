// Package checkpointformat creates compact sqlite-zstd-v1 checkpoint objects.
package checkpointformat

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"

	"git.saveweb.org/saveweb/hq/internal/queue"
)

const FormatSQLiteZstdV1 = "sqlite-zstd-v1"

type Artifact struct {
	Path      string
	SizeBytes int64
	SHA256    string
	workDir   string
}

func Create(ctx context.Context, snapshotter queue.Snapshotter, parentDir string) (Artifact, error) {
	if snapshotter == nil || parentDir == "" {
		return Artifact{}, fmt.Errorf("checkpoint format: invalid create request")
	}
	if err := os.MkdirAll(parentDir, 0o700); err != nil {
		return Artifact{}, fmt.Errorf("checkpoint format: create work parent: %w", err)
	}
	workDir, err := os.MkdirTemp(parentDir, "checkpoint-")
	if err != nil {
		return Artifact{}, fmt.Errorf("checkpoint format: create work directory: %w", err)
	}
	remove := true
	defer func() {
		if remove {
			_ = os.RemoveAll(workDir)
		}
	}()
	snapshotPath := filepath.Join(workDir, "queue.sqlite")
	if err := snapshotter.Snapshot(ctx, snapshotPath); err != nil {
		return Artifact{}, err
	}
	compressedPath := filepath.Join(workDir, "queue.sqlite.zst")
	size, checksum, err := compress(ctx, snapshotPath, compressedPath)
	if err != nil {
		return Artifact{}, err
	}
	if err := os.Remove(snapshotPath); err != nil {
		return Artifact{}, fmt.Errorf("checkpoint format: remove uncompressed snapshot: %w", err)
	}
	remove = false
	return Artifact{Path: compressedPath, SizeBytes: size, SHA256: checksum, workDir: workDir}, nil
}

func (a Artifact) Cleanup() error {
	if a.workDir == "" {
		return nil
	}
	return os.RemoveAll(a.workDir)
}

func compress(ctx context.Context, sourcePath, destinationPath string) (int64, string, error) {
	source, err := os.Open(sourcePath)
	if err != nil {
		return 0, "", fmt.Errorf("checkpoint format: open snapshot: %w", err)
	}
	defer source.Close()
	destination, err := os.OpenFile(destinationPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return 0, "", fmt.Errorf("checkpoint format: create compressed snapshot: %w", err)
	}
	remove := true
	defer func() {
		_ = destination.Close()
		if remove {
			_ = os.Remove(destinationPath)
		}
	}()
	hash := sha256.New()
	counting := &countWriter{writer: io.MultiWriter(destination, hash)}
	encoder, err := zstd.NewWriter(counting,
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(6)),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		return 0, "", fmt.Errorf("checkpoint format: create zstd encoder: %w", err)
	}
	if _, err := io.Copy(encoder, contextReader{ctx: ctx, reader: source}); err != nil {
		_ = encoder.Close()
		return 0, "", fmt.Errorf("checkpoint format: compress snapshot: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return 0, "", fmt.Errorf("checkpoint format: close zstd encoder: %w", err)
	}
	if err := destination.Sync(); err != nil {
		return 0, "", fmt.Errorf("checkpoint format: sync compressed snapshot: %w", err)
	}
	if err := destination.Close(); err != nil {
		return 0, "", fmt.Errorf("checkpoint format: close compressed snapshot: %w", err)
	}
	remove = false
	return counting.count, hex.EncodeToString(hash.Sum(nil)), nil
}

type countWriter struct {
	writer io.Writer
	count  int64
}

func (w *countWriter) Write(value []byte) (int, error) {
	written, err := w.writer.Write(value)
	w.count += int64(written)
	return written, err
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(value []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(value)
}
