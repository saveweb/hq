package endpointcheck

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

func testChecker() *Checker {
	return newWithOptions(options{
		resolver: net.DefaultResolver, dialer: &net.Dialer{Timeout: time.Second},
		timeout: time.Second, allowPrivate: true,
	})
}

func challengeHandler(t *testing.T, cfCacheStatus string, mutate bool) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, challengePath) || request.Method != http.MethodPost {
			http.NotFound(response, request)
			return
		}
		if request.Header.Get("Cache-Control") == "" || request.Header.Get("Cloudflare-CDN-Cache-Control") != "no-store" {
			t.Errorf("missing anti-cache request headers: %+v", request.Header)
		}
		var challenge protocol.EndpointChallenge
		if err := json.NewDecoder(request.Body).Decode(&challenge); err != nil {
			t.Error(err)
			return
		}
		if mutate {
			challenge.Challenge += "wrong"
		}
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Cache-Control", "no-store, no-cache, max-age=0")
		if cfCacheStatus != "" {
			response.Header().Set("CF-Cache-Status", cfCacheStatus)
		}
		_ = json.NewEncoder(response).Encode(challenge)
	})
}

func TestHTTPSChallengeAcceptsMatchingSelfSignedSPKIPin(t *testing.T) {
	server := httptest.NewTLSServer(challengeHandler(t, "DYNAMIC", false))
	defer server.Close()
	digest := sha256.Sum256(server.Certificate().RawSubjectPublicKeyInfo)
	pin := base64.RawURLEncoding.EncodeToString(digest[:])

	status, err := testChecker().Check(context.Background(), "shard-1", server.URL, &pin)
	if err != nil || status != tracker.EndpointHealthy {
		t.Fatalf("check = %q, %v", status, err)
	}
}

func TestHTTPSChallengeRejectsWrongPin(t *testing.T) {
	server := httptest.NewTLSServer(challengeHandler(t, "", false))
	defer server.Close()
	wrongDigest := sha256.Sum256([]byte("wrong key"))
	pin := base64.RawURLEncoding.EncodeToString(wrongDigest[:])

	status, err := testChecker().Check(context.Background(), "shard-1", server.URL, &pin)
	if err != nil || status != tracker.EndpointTLSFailed {
		t.Fatalf("check = %q, %v", status, err)
	}
}

func TestHTTPIsExplicitlyInsecure(t *testing.T) {
	server := httptest.NewServer(challengeHandler(t, "BYPASS", false))
	defer server.Close()
	for _, endpoint := range []string{server.URL, server.URL + "/proxy-prefix"} {
		status, err := testChecker().Check(context.Background(), "shard-1", endpoint, nil)
		if err != nil || status != tracker.EndpointInsecure {
			t.Fatalf("check %s = %q, %v", endpoint, status, err)
		}
	}
}

func TestChallengeDetectsCacheAndIdentityFailures(t *testing.T) {
	for name, test := range map[string]struct {
		cfStatus string
		mutate   bool
		expected string
	}{
		"cache hit":  {cfStatus: "HIT", expected: tracker.EndpointCacheFailed},
		"wrong echo": {mutate: true, expected: tracker.EndpointUnreachable},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(challengeHandler(t, test.cfStatus, test.mutate))
			defer server.Close()
			status, err := testChecker().Check(context.Background(), "shard-1", server.URL, nil)
			if err != nil || status != test.expected {
				t.Fatalf("check = %q, %v", status, err)
			}
		})
	}
}

func TestProductionCheckerBlocksPrivateAndLoopbackDestinations(t *testing.T) {
	for _, endpoint := range []string{"http://127.0.0.1:1234", "http://[::1]:1234", "http://10.1.2.3:1234"} {
		status, err := New().Check(context.Background(), "shard-1", endpoint, nil)
		if status != "" || !tracker.IsCode(err, protocol.ErrorInvalidRequest) {
			t.Fatalf("check %s = %q, %v", endpoint, status, err)
		}
	}
}

func TestPublicIPClassification(t *testing.T) {
	for value, expected := range map[string]bool{
		"8.8.8.8": true, "2606:4700:4700::1111": true,
		"127.0.0.1": false, "10.0.0.1": false, "169.254.1.1": false,
		"::1": false, "fc00::1": false, "fe80::1": false,
	} {
		if actual := publicIP(net.ParseIP(value)); actual != expected {
			t.Errorf("publicIP(%s) = %v, want %v", value, actual, expected)
		}
	}
}
