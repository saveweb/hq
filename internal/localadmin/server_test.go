package localadmin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"git.saveweb.org/saveweb/hq/internal/shard"
)

type fakeProvider struct{ paused bool }

func (p *fakeProvider) RuntimeStatus(context.Context) (shard.RuntimeStatus, error) {
	return shard.RuntimeStatus{
		AgentID: "shard-1", ServerTime: 1_780_000_000, ClaimsPaused: p.paused,
		Shards: []shard.ShardStatus{{ProjectID: "project-1", ShardID: "shard-a", Generation: 2}},
	}, nil
}

func (p *fakeProvider) SetClaimsPaused(value bool) { p.paused = value }

func TestLocalLoginStatusAndClaimPause(t *testing.T) {
	provider := &fakeProvider{}
	token := strings.Repeat("t", 43)
	server, err := NewServer(provider, token, "http://127.0.0.1:9081", func() int64 { return 1_780_000_000 })
	if err != nil {
		t.Fatal(err)
	}
	wrong := localRequest(server, http.MethodPost, "/login", url.Values{"token": {token}}, nil, "https://evil.test", "")
	if wrong.Code != http.StatusForbidden {
		t.Fatalf("wrong-origin login = %d", wrong.Code)
	}
	login := localRequest(server, http.MethodPost, "/login", url.Values{"token": {token}}, nil, "http://127.0.0.1:9081", "")
	if login.Code != http.StatusSeeOther {
		t.Fatalf("login = %d %q", login.Code, login.Body.String())
	}
	cookie := findLocalCookie(t, login)
	admin := localRequest(server, http.MethodGet, "/admin", nil, cookie, "", "")
	if admin.Code != http.StatusOK || !strings.Contains(admin.Body.String(), "shard-a") ||
		admin.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("admin = %d %q", admin.Code, admin.Body.String())
	}
	match := regexp.MustCompile(`name="csrf" value="([^"]+)"`).FindStringSubmatch(admin.Body.String())
	if len(match) != 2 {
		t.Fatal("CSRF token missing")
	}
	pause := localRequest(server, http.MethodPost, "/admin/claims/pause", url.Values{"csrf": {match[1]}}, cookie, "http://127.0.0.1:9081", "")
	if pause.Code != http.StatusSeeOther || !provider.paused {
		t.Fatalf("pause = %d paused=%v", pause.Code, provider.paused)
	}
	api := localRequest(server, http.MethodGet, "/api/v1/status", nil, nil, "", "Bearer "+token)
	if api.Code != http.StatusOK {
		t.Fatalf("API status = %d %q", api.Code, api.Body.String())
	}
	var status shard.RuntimeStatus
	if err := json.Unmarshal(api.Body.Bytes(), &status); err != nil || !status.ClaimsPaused {
		t.Fatalf("API status = %+v, %v", status, err)
	}
}

func localRequest(
	server http.Handler,
	method, target string,
	form url.Values,
	cookie *http.Cookie,
	origin, authorization string,
) *httptest.ResponseRecorder {
	body := ""
	if form != nil {
		body = form.Encode()
	}
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	request.Host = "127.0.0.1:9081"
	if form != nil {
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if cookie != nil {
		request.AddCookie(cookie)
	}
	if origin != "" {
		request.Header.Set("Origin", origin)
	}
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	return response
}

func findLocalCookie(t *testing.T, response *httptest.ResponseRecorder) *http.Cookie {
	t.Helper()
	for _, cookie := range response.Result().Cookies() {
		if cookie.Name == localSessionCookie && cookie.Value != "" {
			return cookie
		}
	}
	t.Fatal("local session cookie not found")
	return nil
}
