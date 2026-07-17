package trackerweb

import (
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"

	"git.saveweb.org/saveweb/hq/internal/tracker"
)

const webTestNow = int64(1_780_000_000)

type fakeOAuth struct {
	state     string
	challenge string
	verifier  string
}

func (f *fakeOAuth) AuthorizationURL(state, challenge string) (string, error) {
	f.state, f.challenge = state, challenge
	return "https://github.test/authorize?state=" + url.QueryEscape(state), nil
}

func (f *fakeOAuth) Exchange(_ context.Context, code, verifier string) (string, error) {
	if code != "test-code" {
		return "", context.Canceled
	}
	f.verifier = verifier
	return "github-access-token", nil
}

func (f *fakeOAuth) User(_ context.Context, token string) (tracker.GitHubIdentity, error) {
	if token != "github-access-token" {
		return tracker.GitHubIdentity{}, context.Canceled
	}
	avatar := "https://avatars.test/u"
	return tracker.GitHubIdentity{UserID: 42, Login: "alice", AvatarURL: &avatar}, nil
}

type fakeStore struct {
	user         tracker.User
	sessions     map[string]string
	machineToken string
	updatedUser  string
}

func newFakeStore() *fakeStore {
	githubID := int64(42)
	return &fakeStore{
		user: tracker.User{
			ID: "admin", GitHubUserID: &githubID, GitHubLogin: "alice",
			Status: tracker.UserStatusActive,
			Roles:  map[string]bool{tracker.RoleAdmin: true, tracker.RoleWorker: true},
		},
		sessions: map[string]string{},
	}
}

func (s *fakeStore) UpsertGitHubUser(_ context.Context, _ tracker.GitHubIdentity, _ bool, _ int64) (tracker.User, error) {
	return s.user, nil
}

func (s *fakeStore) CreateWebSession(_ context.Context, userID string, hash []byte, _, _ int64) error {
	s.sessions[base64.RawURLEncoding.EncodeToString(hash)] = userID
	return nil
}

func (s *fakeStore) AuthenticateWebSession(_ context.Context, hash []byte, _ int64) (tracker.User, error) {
	if s.sessions[base64.RawURLEncoding.EncodeToString(hash)] != s.user.ID {
		return tracker.User{}, context.Canceled
	}
	return s.user, nil
}

func (s *fakeStore) DeleteWebSession(_ context.Context, hash []byte) error {
	delete(s.sessions, base64.RawURLEncoding.EncodeToString(hash))
	return nil
}

func (s *fakeStore) MachineToken(_ context.Context, _ string) (string, error) {
	return s.machineToken, nil
}

func (s *fakeStore) ResetMachineToken(_ context.Context, _ string, _ int64) (string, error) {
	s.machineToken = "mt_generated"
	return s.machineToken, nil
}

func (s *fakeStore) ListUserAgents(context.Context, string) ([]tracker.Agent, error) {
	return []tracker.Agent{{ID: "worker-1", Kind: "worker", Name: "Worker", Status: "online"}}, nil
}

func (s *fakeStore) ListUsers(context.Context) ([]tracker.User, error) {
	return []tracker.User{s.user}, nil
}

func (s *fakeStore) UpdateUserAccess(_ context.Context, _, target, _ string, _ map[string]bool, _ string, _ int64) error {
	s.updatedUser = target
	return nil
}

func TestGitHubOAuthPortalCSRFAndAdminFlow(t *testing.T) {
	store, oauth := newFakeStore(), &fakeOAuth{}
	handler, err := New(store, oauth, Config{
		PublicURL: "http://tracker.test", Secret: []byte("0123456789abcdef0123456789abcdef"),
		Clock: func() int64 { return webTestNow },
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server := echo.New()
	handler.Register(server)

	start := perform(server, http.MethodGet, "/auth/github/start", "", nil, "")
	if start.Code != http.StatusFound || oauth.state == "" || len(oauth.challenge) != 43 {
		t.Fatalf("start = %d %q", start.Code, start.Body.String())
	}
	oauthCookie := responseCookie(t, start, oauthCookieName)
	callback := perform(
		server, http.MethodGet,
		"/auth/github/callback?code=test-code&state="+url.QueryEscape(oauth.state),
		"", oauthCookie, "",
	)
	if callback.Code != http.StatusSeeOther || len(oauth.verifier) != 43 {
		t.Fatalf("callback = %d %q", callback.Code, callback.Body.String())
	}
	sessionCookie := responseCookie(t, callback, sessionCookieName)
	portal := perform(server, http.MethodGet, "/portal", "", sessionCookie, "")
	if portal.Code != http.StatusOK || !strings.Contains(portal.Body.String(), "alice") ||
		portal.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("portal = %d %q", portal.Code, portal.Body.String())
	}
	csrfMatch := regexp.MustCompile(`name="csrf" value="([^"]+)"`).FindStringSubmatch(portal.Body.String())
	if len(csrfMatch) != 2 {
		t.Fatalf("CSRF token missing from %q", portal.Body.String())
	}
	form := url.Values{"csrf": {csrfMatch[1]}}.Encode()
	wrongOrigin := perform(server, http.MethodPost, "/portal/machine-token/reset", form, sessionCookie, "https://evil.test")
	if wrongOrigin.Code != http.StatusForbidden || store.machineToken != "" {
		t.Fatalf("wrong-origin reset = %d token=%q", wrongOrigin.Code, store.machineToken)
	}
	reset := perform(server, http.MethodPost, "/portal/machine-token/reset", form, sessionCookie, "http://tracker.test")
	if reset.Code != http.StatusSeeOther || store.machineToken == "" {
		t.Fatalf("reset = %d token=%q", reset.Code, store.machineToken)
	}
	admin := perform(server, http.MethodGet, "/admin/users", "", sessionCookie, "")
	if admin.Code != http.StatusOK || !strings.Contains(admin.Body.String(), "User administration") {
		t.Fatalf("admin = %d %q", admin.Code, admin.Body.String())
	}
	updateForm := url.Values{
		"csrf": {csrfMatch[1]}, "status": {"active"}, "role_worker": {"on"}, "reason": {"approved"},
	}.Encode()
	update := perform(server, http.MethodPost, "/admin/users/target/access", updateForm, sessionCookie, "http://tracker.test")
	if update.Code != http.StatusSeeOther || store.updatedUser != "target" {
		t.Fatalf("update = %d target=%q", update.Code, store.updatedUser)
	}
}

func perform(
	server http.Handler,
	method, target, form string,
	cookie *http.Cookie,
	origin string,
) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, strings.NewReader(form))
	request.Host = "tracker.test"
	if form != "" {
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if cookie != nil {
		request.AddCookie(cookie)
	}
	if origin != "" {
		request.Header.Set("Origin", origin)
	}
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	return response
}

func responseCookie(t *testing.T, response *httptest.ResponseRecorder, name string) *http.Cookie {
	t.Helper()
	for _, cookie := range response.Result().Cookies() {
		if cookie.Name == name && cookie.Value != "" {
			return cookie
		}
	}
	t.Fatalf("cookie %q not found", name)
	return nil
}
