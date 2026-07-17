// Package objectstore is the narrow trusted S3-compatible adapter used by the
// tracker. Queue and shard domain packages never receive AWS credentials or
// SDK types.
package objectstore

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

type Metadata struct {
	Size int64
	ETag string
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

func (c *Client) Head(ctx context.Context, uri string) (Metadata, error) {
	object, err := ParseURI(uri)
	if err != nil {
		return Metadata{}, err
	}
	result, err := c.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(object.Bucket), Key: aws.String(object.Key),
	})
	if err != nil {
		return Metadata{}, fmt.Errorf("objectstore: head %s: %w", URI(object), err)
	}
	if result.ContentLength == nil || *result.ContentLength < 0 || result.ETag == nil || *result.ETag == "" {
		return Metadata{}, fmt.Errorf("objectstore: incomplete object metadata")
	}
	return Metadata{Size: *result.ContentLength, ETag: NormalizeETag(*result.ETag)}, nil
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

func NormalizeETag(value string) string {
	return strings.Trim(strings.TrimSpace(value), `"`)
}
