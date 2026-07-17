// Package checkpointrestore downloads, verifies, and atomically installs a
// tracker-selected sqlite-zstd-v1 checkpoint. It never receives S3 credentials.
package checkpointrestore

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/zstd"

	"git.saveweb.org/saveweb/hq/internal/checkpointformat"
	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/sqlitequeue"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

type Config struct {
	AllowHTTP            bool
	MaxCompressedBytes   int64
	MaxUncompressedBytes int64
	HTTPClient           *http.Client
	Clock                func() int64
}

type Restorer struct {
	config Config
}

func DefaultConfig() Config {
	return Config{
		MaxCompressedBytes: 64 << 30, MaxUncompressedBytes: 512 << 30,
		Clock: func() int64 { return time.Now().Unix() },
	}
}

func New(config Config) (*Restorer, error) {
	if config.MaxCompressedBytes < 1 || config.MaxUncompressedBytes < 1 {
		return nil, fmt.Errorf("checkpoint restore: invalid limits")
	}
	if config.Clock == nil {
		config.Clock = func() int64 { return time.Now().Unix() }
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
	return &Restorer{config: config}, nil
}

func (r *Restorer) Restore(ctx context.Context, assignment protocol.OwnerAssignment, destination string) error {
	checkpoint := assignment.Checkpoint
	if checkpoint == nil || checkpoint.Format != checkpointformat.FormatSQLiteZstdV1 ||
		checkpoint.Generation < 1 || checkpoint.Generation > assignment.Generation || checkpoint.Sequence < 1 ||
		checkpoint.SizeBytes < 1 || checkpoint.SizeBytes > r.config.MaxCompressedBytes ||
		len(checkpoint.SHA256) != sha256.Size*2 || checkpoint.DownloadURL == "" ||
		checkpoint.URLExpiresAt <= r.config.Clock() {
		return fmt.Errorf("checkpoint restore: incomplete or expired assignment")
	}
	expectedChecksum, err := hex.DecodeString(checkpoint.SHA256)
	if err != nil || len(expectedChecksum) != sha256.Size {
		return fmt.Errorf("checkpoint restore: invalid checksum")
	}
	parsed, err := url.Parse(checkpoint.DownloadURL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" ||
		(parsed.Scheme != "https" && !(r.config.AllowHTTP && parsed.Scheme == "http")) ||
		len(checkpoint.DownloadURL) > 8192 {
		return fmt.Errorf("checkpoint restore: invalid download URL")
	}
	absPath, err := filepath.Abs(destination)
	if err != nil {
		return fmt.Errorf("checkpoint restore: destination path: %w", err)
	}
	if _, err := os.Lstat(absPath); err == nil || !os.IsNotExist(err) {
		return fmt.Errorf("checkpoint restore: destination already exists or cannot be inspected")
	}
	if err := os.MkdirAll(filepath.Dir(absPath), 0o700); err != nil {
		return fmt.Errorf("checkpoint restore: create queue directory: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(absPath), ".checkpoint-restore-*.sqlite")
	if err != nil {
		return fmt.Errorf("checkpoint restore: create temporary queue: %w", err)
	}
	temporaryPath := temporary.Name()
	remove := true
	defer func() {
		_ = temporary.Close()
		if remove {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("checkpoint restore: protect temporary queue: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return fmt.Errorf("checkpoint restore: cannot create download request")
	}
	request.Header.Set("Accept-Encoding", "identity")
	response, err := r.config.HTTPClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// net/http errors include the presigned query. Never return it.
		return fmt.Errorf("checkpoint restore: download request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return fmt.Errorf("checkpoint restore: object server returned HTTP %d", response.StatusCode)
	}
	if response.ContentLength >= 0 && response.ContentLength != checkpoint.SizeBytes {
		return fmt.Errorf("checkpoint restore: compressed size mismatch")
	}
	hash := sha256.New()
	compressed := &countingReader{reader: io.TeeReader(
		&io.LimitedReader{R: response.Body, N: checkpoint.SizeBytes + 1}, hash,
	)}
	decoder, err := zstd.NewReader(compressed,
		zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20), zstd.WithDecoderMaxWindow(64<<20),
	)
	if err != nil {
		return fmt.Errorf("checkpoint restore: create zstd decoder: %w", err)
	}
	written, copyError := io.CopyN(temporary, decoder, r.config.MaxUncompressedBytes+1)
	decoder.Close()
	if copyError != nil && copyError != io.EOF {
		return fmt.Errorf("checkpoint restore: decompress: %w", copyError)
	}
	if written > r.config.MaxUncompressedBytes {
		return fmt.Errorf("checkpoint restore: uncompressed checkpoint exceeds size limit")
	}
	if compressed.count != checkpoint.SizeBytes {
		return fmt.Errorf("checkpoint restore: compressed size mismatch")
	}
	if !equalBytes(hash.Sum(nil), expectedChecksum) {
		return fmt.Errorf("checkpoint restore: checksum mismatch")
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("checkpoint restore: sync queue: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("checkpoint restore: close queue: %w", err)
	}
	if err := sqlitequeue.VerifyCheckpoint(ctx, temporaryPath, queue.Identity{
		ProjectID: assignment.ProjectID, ShardID: assignment.ShardID, Generation: checkpoint.Generation,
	}); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, absPath); err != nil {
		return fmt.Errorf("checkpoint restore: install queue: %w", err)
	}
	remove = false
	if err := syncDirectory(filepath.Dir(absPath)); err != nil {
		return err
	}
	return nil
}

type countingReader struct {
	reader io.Reader
	count  int64
}

func (r *countingReader) Read(value []byte) (int, error) {
	read, err := r.reader.Read(value)
	r.count += int64(read)
	return read, err
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var different byte
	for index := range left {
		different |= left[index] ^ right[index]
	}
	return different == 0
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("checkpoint restore: open queue directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("checkpoint restore: sync queue directory: %w", err)
	}
	return nil
}
