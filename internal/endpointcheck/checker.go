// Package endpointcheck verifies a volunteer shard's public HTTP(S) endpoint.
package endpointcheck

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
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

	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const (
	challengePath     = "/api/v1/shard/endpoint-challenge"
	maxChallengeBytes = int64(4096)
	defaultTimeout    = 8 * time.Second
)

var (
	errForbiddenAddress = errors.New("endpoint resolves to a non-public address")
	errPinMismatch      = errors.New("TLS SPKI pin mismatch")
)

type resolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

type options struct {
	resolver     resolver
	dialer       *net.Dialer
	timeout      time.Duration
	allowPrivate bool
}

type Checker struct {
	options options
}

type Options struct {
	AllowPrivate bool
	Timeout      time.Duration
}

func New() *Checker {
	return NewWithOptions(Options{})
}

func NewWithOptions(value Options) *Checker {
	return newWithOptions(options{
		resolver: net.DefaultResolver,
		dialer:   &net.Dialer{Timeout: defaultTimeout, KeepAlive: 30 * time.Second},
		timeout:  value.Timeout, allowPrivate: value.AllowPrivate,
	})
}

func newWithOptions(value options) *Checker {
	if value.resolver == nil {
		value.resolver = net.DefaultResolver
	}
	if value.dialer == nil {
		value.dialer = &net.Dialer{Timeout: defaultTimeout, KeepAlive: 30 * time.Second}
	}
	if value.timeout <= 0 {
		value.timeout = defaultTimeout
	}
	return &Checker{options: value}
}

func (c *Checker) Check(ctx context.Context, agentID, endpoint string, tlsSPKISHA256 *string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", invalidEndpoint("invalid endpoint URI")
	}
	pin, err := decodePin(parsed.Scheme, tlsSPKISHA256)
	if err != nil {
		return "", err
	}
	challenge, err := randomChallenge()
	if err != nil {
		return "", fmt.Errorf("endpointcheck: create challenge: %w", err)
	}
	body, err := json.Marshal(protocol.EndpointChallenge{AgentID: agentID, Challenge: challenge})
	if err != nil {
		return "", fmt.Errorf("endpointcheck: encode challenge: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimSuffix(endpoint, "/")+challengePath, strings.NewReader(string(body)))
	if err != nil {
		return "", invalidEndpoint("invalid endpoint URI")
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Cache-Control", "no-store, no-cache, max-age=0")
	request.Header.Set("Pragma", "no-cache")
	request.Header.Set("Cloudflare-CDN-Cache-Control", "no-store")
	request.Header.Set("CDN-Cache-Control", "no-store")

	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           c.dialContext,
		TLSHandshakeTimeout:   c.options.timeout,
		ResponseHeaderTimeout: c.options.timeout,
		ForceAttemptHTTP2:     true,
	}
	if pin != nil {
		transport.TLSClientConfig = pinnedTLSConfig(parsed.Hostname(), pin)
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   c.options.timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	defer transport.CloseIdleConnections()
	response, err := client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if errors.Is(err, errForbiddenAddress) {
			return "", invalidEndpoint(errForbiddenAddress.Error())
		}
		if isTLSError(err) {
			return tracker.EndpointTLSFailed, nil
		}
		return tracker.EndpointUnreachable, nil
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return tracker.EndpointUnreachable, nil
	}
	if !cacheHeadersSafe(response.Header) {
		return tracker.EndpointCacheFailed, nil
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return tracker.EndpointUnreachable, nil
	}
	limited := io.LimitReader(response.Body, maxChallengeBytes+1)
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	var answer protocol.EndpointChallenge
	if err := decoder.Decode(&answer); err != nil || answer.AgentID != agentID || answer.Challenge != challenge {
		return tracker.EndpointUnreachable, nil
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return tracker.EndpointUnreachable, nil
	}
	if parsed.Scheme == "http" {
		return tracker.EndpointInsecure, nil
	}
	return tracker.EndpointHealthy, nil
}

func (c *Checker) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	addresses := []net.IPAddr{}
	if literal := net.ParseIP(host); literal != nil {
		addresses = append(addresses, net.IPAddr{IP: literal})
	} else {
		addresses, err = c.options.resolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
	}
	var lastError error
	for _, value := range addresses {
		if !c.options.allowPrivate && !publicIP(value.IP) {
			lastError = errForbiddenAddress
			continue
		}
		connection, dialError := c.options.dialer.DialContext(ctx, network, net.JoinHostPort(value.IP.String(), port))
		if dialError == nil {
			return connection, nil
		}
		lastError = dialError
	}
	if lastError == nil {
		lastError = fmt.Errorf("endpoint has no addresses")
	}
	return nil, lastError
}

func publicIP(value net.IP) bool {
	return value != nil && value.IsGlobalUnicast() && !value.IsPrivate() && !value.IsLoopback() &&
		!value.IsLinkLocalUnicast() && !value.IsLinkLocalMulticast() && !value.IsUnspecified()
}

func decodePin(scheme string, encoded *string) ([]byte, error) {
	if encoded == nil {
		return nil, nil
	}
	if scheme != "https" {
		return nil, invalidEndpoint("TLS pin is valid only for HTTPS")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(*encoded)
	if err != nil || len(decoded) != sha256.Size {
		return nil, invalidEndpoint("TLS SPKI pin must be an unpadded base64url SHA-256 digest")
	}
	return decoded, nil
}

func randomChallenge() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value[:]), nil
}

func pinnedTLSConfig(serverName string, expected []byte) *tls.Config {
	return &tls.Config{
		ServerName: serverName,
		MinVersion: tls.VersionTLS12,
		// The explicit SPKI digest is the trust root, including for self-signed certificates.
		InsecureSkipVerify: true, //nolint:gosec
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return errPinMismatch
			}
			certificate := state.PeerCertificates[0]
			now := time.Now()
			if now.Before(certificate.NotBefore) || now.After(certificate.NotAfter) {
				return x509.CertificateInvalidError{Cert: certificate, Reason: x509.Expired}
			}
			digest := sha256.Sum256(certificate.RawSubjectPublicKeyInfo)
			if !equalBytes(digest[:], expected) {
				return errPinMismatch
			}
			return nil
		},
	}
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

func cacheHeadersSafe(headers http.Header) bool {
	cacheControl := strings.ToLower(headers.Get("Cache-Control"))
	if !strings.Contains(cacheControl, "no-store") {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(headers.Get("CF-Cache-Status"))) {
	case "", "DYNAMIC", "BYPASS":
		return true
	default:
		return false
	}
}

func isTLSError(err error) bool {
	if errors.Is(err, errPinMismatch) {
		return true
	}
	var certificateInvalid x509.CertificateInvalidError
	var unknownAuthority x509.UnknownAuthorityError
	var hostnameError x509.HostnameError
	var recordHeader tls.RecordHeaderError
	return errors.As(err, &certificateInvalid) || errors.As(err, &unknownAuthority) ||
		errors.As(err, &hostnameError) || errors.As(err, &recordHeader)
}

func invalidEndpoint(message string) *tracker.Error {
	return &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: message}
}
