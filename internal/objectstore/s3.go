// Package objectstore is the narrow trusted S3-compatible adapter used by the
// tracker. Queue and shard domain packages never receive AWS credentials or
// SDK types.
package objectstore

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"git.saveweb.org/saveweb/hq/internal/objectstorage"
)

type Config struct {
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	UsePathStyle    bool
	AllowHTTP       bool
	HTTPClient      *http.Client
}

type Client struct {
	s3      *s3.Client
	presign *s3.PresignClient
}

type Object struct {
	Bucket string
	Key    string
}

func New(config Config) (*Client, error) {
	endpoint, err := url.Parse(config.Endpoint)
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" ||
		endpoint.Fragment != "" || (endpoint.Path != "" && endpoint.Path != "/") ||
		(endpoint.Scheme != "https" && !(config.AllowHTTP && endpoint.Scheme == "http")) ||
		config.Region == "" || config.AccessKeyID == "" || config.SecretAccessKey == "" {
		return nil, fmt.Errorf("objectstore: invalid S3-compatible configuration")
	}
	if config.HTTPClient == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		} else {
			transport.TLSClientConfig = transport.TLSClientConfig.Clone()
			transport.TLSClientConfig.MinVersion = tls.VersionTLS12
		}
		config.HTTPClient = &http.Client{
			Transport: transport, Timeout: 30 * time.Second,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	awsConfig := aws.Config{
		Region: config.Region,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
			config.AccessKeyID, config.SecretAccessKey, "",
		)),
		HTTPClient: config.HTTPClient,
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(options *retry.StandardOptions) {
				options.MaxAttempts = 4
			})
		},
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(strings.TrimSuffix(config.Endpoint, "/"))
		options.UsePathStyle = config.UsePathStyle
	})
	return &Client{s3: client, presign: s3.NewPresignClient(client)}, nil
}

func ParseURI(value string) (Object, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "s3" || parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawPath != "" {
		return Object{}, fmt.Errorf("objectstore: invalid s3 URI")
	}
	key := strings.TrimPrefix(parsed.Path, "/")
	if key == "" || strings.Contains(key, "\x00") || len(parsed.Host) > 255 || len(key) > 1024 {
		return Object{}, fmt.Errorf("objectstore: invalid s3 bucket or key")
	}
	return Object{Bucket: parsed.Host, Key: key}, nil
}

func URI(object Object) string {
	return (&url.URL{Scheme: "s3", Host: object.Bucket, Path: "/" + object.Key}).String()
}

func (c *Client) Head(ctx context.Context, uri string) (int64, string, error) {
	object, err := ParseURI(uri)
	if err != nil {
		return 0, "", err
	}
	result, err := c.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(object.Bucket), Key: aws.String(object.Key),
	})
	if err != nil {
		return 0, "", fmt.Errorf("objectstore: head %s: %w", URI(object), err)
	}
	if result.ContentLength == nil || *result.ContentLength < 0 || result.ETag == nil || *result.ETag == "" {
		return 0, "", fmt.Errorf("objectstore: incomplete object metadata")
	}
	return *result.ContentLength, NormalizeETag(*result.ETag), nil
}

func (c *Client) Put(
	ctx context.Context,
	uri string,
	body *bytes.Reader,
	sizeBytes int64,
	contentType string,
) (string, error) {
	object, err := ParseURI(uri)
	if err != nil {
		return "", err
	}
	if body == nil || sizeBytes < 1 || body.Size() != sizeBytes || contentType == "" || len(contentType) > 256 {
		return "", fmt.Errorf("objectstore: invalid put request")
	}
	result, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(object.Bucket), Key: aws.String(object.Key), Body: body,
		ContentLength: aws.Int64(sizeBytes), ContentType: aws.String(contentType),
	})
	if err != nil {
		return "", fmt.Errorf("objectstore: put %s: %w", URI(object), err)
	}
	if result.ETag == nil || *result.ETag == "" {
		return "", fmt.Errorf("objectstore: put response omitted ETag")
	}
	return NormalizeETag(*result.ETag), nil
}

func (c *Client) PresignGet(
	ctx context.Context,
	uri string,
	now int64,
	ttl time.Duration,
) (string, int64, error) {
	object, err := ParseURI(uri)
	if err != nil {
		return "", 0, err
	}
	if now < 0 || ttl < time.Minute || ttl > 24*time.Hour {
		return "", 0, fmt.Errorf("objectstore: invalid presign lifetime")
	}
	request, err := c.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(object.Bucket), Key: aws.String(object.Key),
	}, func(options *s3.PresignOptions) {
		options.Expires = ttl
	})
	if err != nil {
		return "", 0, fmt.Errorf("objectstore: presign get %s: %w", URI(object), err)
	}
	if request.Method != http.MethodGet || request.URL == "" {
		return "", 0, fmt.Errorf("objectstore: invalid presigned download")
	}
	return request.URL, now + int64(ttl/time.Second), nil
}

func (c *Client) CreateMultipart(ctx context.Context, uri string) (string, error) {
	object, err := ParseURI(uri)
	if err != nil {
		return "", err
	}
	result, err := c.s3.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(object.Bucket), Key: aws.String(object.Key),
		ContentType: aws.String("application/zstd"),
	})
	if err != nil {
		return "", fmt.Errorf("objectstore: create multipart %s: %w", URI(object), err)
	}
	if result.UploadId == nil || *result.UploadId == "" || len(*result.UploadId) > 2048 {
		return "", fmt.Errorf("objectstore: invalid multipart upload ID")
	}
	return *result.UploadId, nil
}

func (c *Client) PresignUploadPart(
	ctx context.Context,
	uri, uploadID string,
	partNumber int32,
	sizeBytes int64,
	contentMD5 string,
	now int64,
	ttl time.Duration,
) (objectstorage.PartURL, error) {
	object, err := ParseURI(uri)
	if err != nil {
		return objectstorage.PartURL{}, err
	}
	if uploadID == "" || len(uploadID) > 2048 || partNumber < 1 || partNumber > 10_000 ||
		sizeBytes < 1 || sizeBytes > 5<<30 || now < 0 || ttl < time.Minute || ttl > 24*time.Hour {
		return objectstorage.PartURL{}, fmt.Errorf("objectstore: invalid upload part request")
	}
	request, err := c.presign.PresignUploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(object.Bucket), Key: aws.String(object.Key), UploadId: aws.String(uploadID),
		PartNumber: aws.Int32(partNumber), ContentLength: aws.Int64(sizeBytes), ContentMD5: aws.String(contentMD5),
	}, func(options *s3.PresignOptions) {
		options.Expires = ttl
	})
	if err != nil {
		return objectstorage.PartURL{}, fmt.Errorf("objectstore: presign upload part: %w", err)
	}
	if request.Method != http.MethodPut || request.URL == "" {
		return objectstorage.PartURL{}, fmt.Errorf("objectstore: invalid presigned upload part")
	}
	headers := make(map[string]string, len(request.SignedHeader)+1)
	for name, values := range request.SignedHeader {
		if strings.EqualFold(name, "host") {
			continue
		}
		if len(values) != 1 {
			return objectstorage.PartURL{}, fmt.Errorf("objectstore: invalid signed upload header")
		}
		headers[http.CanonicalHeaderKey(name)] = values[0]
	}
	// Content-MD5 is deliberately signed and returned explicitly. The shard
	// computes it from the exact part bytes and cannot substitute another body.
	headers["Content-Md5"] = contentMD5
	return objectstorage.PartURL{URL: request.URL, Headers: headers, ExpiresAt: now + int64(ttl/time.Second)}, nil
}

func (c *Client) CompleteMultipart(ctx context.Context, uri, uploadID string, parts []objectstorage.CompletedPart) error {
	object, err := ParseURI(uri)
	if err != nil {
		return err
	}
	if uploadID == "" || len(parts) < 1 || len(parts) > 10_000 {
		return fmt.Errorf("objectstore: invalid complete multipart request")
	}
	copyParts := append([]objectstorage.CompletedPart(nil), parts...)
	sort.Slice(copyParts, func(i, j int) bool { return copyParts[i].PartNumber < copyParts[j].PartNumber })
	completed := make([]types.CompletedPart, len(copyParts))
	for index, part := range copyParts {
		if part.PartNumber != int32(index+1) || part.ETag == "" || len(part.ETag) > 512 {
			return fmt.Errorf("objectstore: invalid completed part list")
		}
		completed[index] = types.CompletedPart{PartNumber: aws.Int32(part.PartNumber), ETag: aws.String(part.ETag)}
	}
	_, err = c.s3.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(object.Bucket), Key: aws.String(object.Key), UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	})
	if err != nil {
		return fmt.Errorf("objectstore: complete multipart: %w", err)
	}
	return nil
}

func (c *Client) AbortMultipart(ctx context.Context, uri, uploadID string) error {
	object, err := ParseURI(uri)
	if err != nil {
		return err
	}
	if uploadID == "" || len(uploadID) > 2048 {
		return fmt.Errorf("objectstore: invalid abort multipart request")
	}
	_, err = c.s3.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket: aws.String(object.Bucket), Key: aws.String(object.Key), UploadId: aws.String(uploadID),
	})
	if err != nil {
		return fmt.Errorf("objectstore: abort multipart: %w", err)
	}
	return nil
}

func NormalizeETag(value string) string {
	return strings.Trim(strings.TrimSpace(value), `"`)
}
