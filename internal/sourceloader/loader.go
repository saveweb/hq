// Package sourceloader streams one immutable jobs-jsonl-zstd-v1 object into a
// fenced queue store. It accepts only a tracker-issued exact-object URL and
// never holds S3 credentials.
package sourceloader

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"git.saveweb.org/saveweb/hq/internal/objectstore"
	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/sourceformat"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

type Config struct {
	AllowHTTP            bool
	MaxCompressedBytes   int64
	MaxUncompressedBytes int64
	MaxJobs              int64
	HTTPClient           *http.Client
	Clock                func() int64
}

type Loader struct {
	config Config
}

func DefaultConfig() Config {
	return Config{
		MaxCompressedBytes: 16 << 30, MaxUncompressedBytes: 64 << 30,
		MaxJobs: 100_000_000, Clock: func() int64 { return time.Now().Unix() },
	}
}

func New(config Config) (*Loader, error) {
	if config.MaxCompressedBytes < 1 || config.MaxUncompressedBytes < 1 || config.MaxJobs < 1 {
		return nil, fmt.Errorf("source loader: invalid limits")
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
			Transport: transport,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	return &Loader{config: config}, nil
}

func (l *Loader) Load(
	ctx context.Context,
	assignment protocol.OwnerAssignment,
	store queue.Store,
) (sourceformat.DecodeStats, error) {
	if assignment.SourceURI == nil || assignment.SourceFormat == nil ||
		*assignment.SourceFormat != sourceformat.FormatJobsJSONLZstdV1 ||
		assignment.SourceETag == nil || *assignment.SourceETag == "" ||
		assignment.SourceDownloadURL == nil || assignment.SourceURLExpiresAt == nil ||
		*assignment.SourceURLExpiresAt <= l.config.Clock() {
		return sourceformat.DecodeStats{}, fmt.Errorf("source loader: incomplete or expired source assignment")
	}
	parsed, err := url.Parse(*assignment.SourceDownloadURL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" ||
		(parsed.Scheme != "https" && !(l.config.AllowHTTP && parsed.Scheme == "http")) ||
		len(*assignment.SourceDownloadURL) > 8192 {
		return sourceformat.DecodeStats{}, fmt.Errorf("source loader: invalid download URL")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return sourceformat.DecodeStats{}, fmt.Errorf("source loader: cannot create download request")
	}
	request.Header.Set("Accept-Encoding", "identity")
	response, err := l.config.HTTPClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return sourceformat.DecodeStats{}, ctx.Err()
		}
		// net/http errors include the request URL, whose query contains the
		// presigned credential. Never propagate it into local status or logs.
		return sourceformat.DecodeStats{}, fmt.Errorf("source loader: download request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return sourceformat.DecodeStats{}, fmt.Errorf("source loader: object server returned HTTP %d", response.StatusCode)
	}
	if response.ContentLength > l.config.MaxCompressedBytes {
		return sourceformat.DecodeStats{}, fmt.Errorf("source loader: compressed source exceeds size limit")
	}
	actualETag := objectstore.NormalizeETag(response.Header.Get("ETag"))
	if actualETag == "" || actualETag != objectstore.NormalizeETag(*assignment.SourceETag) {
		return sourceformat.DecodeStats{}, fmt.Errorf("source loader: source ETag changed")
	}
	compressed := &io.LimitedReader{R: response.Body, N: l.config.MaxCompressedBytes + 1}
	stats, err := sourceformat.Decode(ctx, compressed, sourceformat.Limits{
		MaxUncompressedBytes: l.config.MaxUncompressedBytes, MaxJobs: l.config.MaxJobs,
	}, func(jobs []queue.JobSpec) error {
		_, err := store.Enqueue(ctx, assignment.Generation, l.config.Clock(), jobs)
		return err
	})
	if err != nil {
		return stats, err
	}
	if compressed.N <= 0 {
		return stats, fmt.Errorf("source loader: compressed source exceeds size limit")
	}
	return stats, nil
}
