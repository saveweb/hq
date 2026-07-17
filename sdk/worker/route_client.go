package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const (
	maxQueueRequestBytes  = int64(1 << 20)
	maxQueueResponseBytes = int64(16 << 20)
)

type routeIdentity struct {
	projectID    string
	shardID      string
	generation   int64
	ownerAgentID string
}

func identityOf(value protocol.Assignment) routeIdentity {
	return routeIdentity{
		projectID: value.ProjectID, shardID: value.ShardID,
		generation: value.Generation, ownerAgentID: value.OwnerAgentID,
	}
}

func (r routeIdentity) matches(value protocol.Assignment) bool {
	return r.projectID == value.ProjectID && r.shardID == value.ShardID &&
		r.generation == value.Generation && r.ownerAgentID == value.OwnerAgentID
}

type routeClient struct {
	assignment protocol.Assignment
	baseURL    string
	client     *http.Client
	transport  *http.Transport
}

func newRouteClient(assignment protocol.Assignment, config Config) (*routeClient, error) {
	parsed, err := url.Parse(assignment.Endpoint)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Scheme != "https" && !(config.AllowHTTPShard && parsed.Scheme == "http")) {
		return nil, fmt.Errorf("worker: invalid or disallowed shard endpoint")
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: config.RequestTimeout,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       60 * time.Second,
	}
	if parsed.Scheme == "http" && assignment.TLSSPKISHA256 != nil {
		return nil, fmt.Errorf("worker: HTTP shard endpoint cannot have a TLS pin")
	}
	if assignment.TLSSPKISHA256 != nil {
		pin, err := base64.RawURLEncoding.DecodeString(*assignment.TLSSPKISHA256)
		if err != nil || len(pin) != sha256.Size {
			return nil, fmt.Errorf("worker: invalid shard SPKI pin")
		}
		transport.TLSClientConfig = pinnedTLSConfig(parsed.Hostname(), pin)
	} else if parsed.Scheme == "https" {
		transport.TLSClientConfig = &tls.Config{ServerName: parsed.Hostname(), MinVersion: tls.VersionTLS12}
	}
	client := &http.Client{
		Transport: transport, Timeout: config.RequestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return &routeClient{
		assignment: assignment, baseURL: strings.TrimSuffix(assignment.Endpoint, "/"),
		client: client, transport: transport,
	}, nil
}

func (c *routeClient) Close() { c.transport.CloseIdleConnections() }

func (c *routeClient) do(ctx context.Context, endpoint string, input, output any) error {
	encoded, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("worker: encode queue request: %w", err)
	}
	if int64(len(encoded)) > maxQueueRequestBytes {
		return &APIError{Status: 0, API: protocol.APIError{
			Code: protocol.ErrorInvalidRequest, Message: "queue request exceeds 1 MiB", Details: protocol.Attrs{},
		}}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+endpoint, bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("worker: create queue request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.assignment.AccessToken)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Cache-Control", "no-store, no-cache, max-age=0")
	request.Header.Set("Pragma", "no-cache")
	response, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("worker: queue request: %w", err)
	}
	defer response.Body.Close()
	if err := validateQueueCacheHeaders(response.Header); err != nil {
		return err
	}
	mediaType, _, mediaError := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if mediaError != nil || mediaType != "application/json" {
		return fmt.Errorf("worker: queue response content type is not application/json")
	}
	reader := io.LimitReader(response.Body, maxQueueResponseBytes+1)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var envelope protocol.ErrorEnvelope
		if err := decodeQueueJSON(reader, &envelope); err != nil {
			return fmt.Errorf("worker: queue HTTP %d with invalid error response: %w", response.StatusCode, err)
		}
		return &APIError{Status: response.StatusCode, API: envelope.Error}
	}
	if err := decodeQueueJSON(reader, output); err != nil {
		return fmt.Errorf("worker: decode queue response: %w", err)
	}
	return nil
}

func pinnedTLSConfig(serverName string, pin []byte) *tls.Config {
	return &tls.Config{
		ServerName: serverName, MinVersion: tls.VersionTLS12,
		// The explicit SPKI digest is the trust root for a pinned endpoint.
		InsecureSkipVerify: true, //nolint:gosec
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return fmt.Errorf("worker: shard TLS peer has no certificate")
			}
			certificate := state.PeerCertificates[0]
			now := time.Now()
			if now.Before(certificate.NotBefore) || now.After(certificate.NotAfter) {
				return x509.CertificateInvalidError{Cert: certificate, Reason: x509.Expired}
			}
			digest := sha256.Sum256(certificate.RawSubjectPublicKeyInfo)
			if subtle.ConstantTimeCompare(digest[:], pin) != 1 {
				return fmt.Errorf("worker: shard TLS SPKI pin mismatch")
			}
			return nil
		},
	}
}

func validateQueueCacheHeaders(headers http.Header) error {
	if !strings.Contains(strings.ToLower(headers.Get("Cache-Control")), "no-store") {
		return &APIError{Status: 0, API: protocol.APIError{
			Code: protocol.ErrorCacheMisconfigured, Message: "queue response is missing Cache-Control: no-store",
			Details: protocol.Attrs{},
		}}
	}
	switch strings.ToUpper(strings.TrimSpace(headers.Get("CF-Cache-Status"))) {
	case "", "DYNAMIC", "BYPASS":
		return nil
	default:
		return &APIError{Status: 0, API: protocol.APIError{
			Code: protocol.ErrorCacheMisconfigured, Message: "queue response may have been served from cache",
			Details: protocol.Attrs{},
		}}
	}
}

func decodeQueueJSON(reader io.Reader, output any) error {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("expected exactly one JSON object")
	}
	return nil
}
