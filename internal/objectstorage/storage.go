// Package objectstorage defines the narrow trusted object-storage boundary.
// Implementations may use R2, S3, or another compatible service.
package objectstorage

import (
	"context"
	"time"
)

type PartURL struct {
	URL       string
	Headers   map[string]string
	ExpiresAt int64
}

type CompletedPart struct {
	PartNumber int32
	ETag       string
}

type CheckpointStore interface {
	CreateMultipart(ctx context.Context, uri string) (string, error)
	PresignUploadPart(ctx context.Context, uri, uploadID string, partNumber int32,
		sizeBytes int64, contentMD5 string, now int64, ttl time.Duration) (PartURL, error)
	CompleteMultipart(ctx context.Context, uri, uploadID string, parts []CompletedPart) error
	AbortMultipart(ctx context.Context, uri, uploadID string) error
	Head(ctx context.Context, uri string) (sizeBytes int64, etag string, err error)
}
