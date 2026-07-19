package trackerweb

import (
	"context"
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
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

type fakeStore struct {
	user       tracker.User
	sessions   map[string]bool
	projects   map[string]protocol.AdminProjectSummary
	enqueued   []protocol.JobSpecV1
	deleted    bool
	upsertedID int64
}

func newFakeStore() *fakeStore {
	return &fakeStore{sessions: map[string]bool{}, projects: map[string]protocol.AdminProjectSummary{}}
}

func (s *fakeStore) UpsertGitHubAdmin(_ context.Context, identity tracker.GitHubIdentity, now int64) (tracker.User, error) {
	s.upsertedID = identity.UserID
	s.user = tracker.User{ID: "gh_42", GitHubUserID: &identity.UserID, GitHubLogin: identity.Login, Status: tracker.UserStatusActive, Roles: map[string]bool{tracker.RoleAdmin: true}, LastLoginAt: &now}
	return s.user, nil
}
func (s *fakeStore) CreateWebSession(_ context.Context, _ string, hash []byte, _, _ int64) error {
	s.sessions[string(hash)] = true
	return nil
}
func (s *fakeStore) AuthenticateWebSession(_ context.Context, hash []byte, _ int64) (tracker.User, error) {
	if !s.sessions[string(hash)] {
		return tracker.User{}, tracker.ErrWebSessionNotFound
	}
	return s.user, nil
}
func (s *fakeStore) DeleteWebSession(_ context.Context, hash []byte) error {
	delete(s.sessions, string(hash))
	s.deleted = true
	return nil
}
func (s *fakeStore) ListProjectSummaries(context.Context) ([]protocol.AdminProjectSummary, error) {
	result := make([]protocol.AdminProjectSummary, 0, len(s.projects))
	for _, project := range s.projects {
		result = append(result, project)
	}
	return result, nil
}
func (s *fakeStore) ProjectSummary(_ context.Context, projectID string) (protocol.AdminProjectSummary, error) {
	project, ok := s.projects[projectID]
	if !ok {
		return protocol.AdminProjectSummary{}, &tracker.Error{Code: protocol.ErrorNotFound, Message: "missing"}
	}
	return project, nil
}
func (s *fakeStore) PutProject(_ context.Context, project tracker.Project, now int64) error {
	existing := s.projects[project.ID]
	if existing.CreatedAt == 0 {
		existing.CreatedAt = now
	}
	existing.ID, existing.Status, existing.UpdatedAt = project.ID, project.Status, now
	if existing.JobCounts == nil {
		existing.JobCounts = map[string]int64{}
	}
	s.projects[project.ID] = existing
	return nil
}
func (s *fakeStore) EnqueueProjectJobs(_ context.Context, projectID string, jobs []protocol.JobSpecV1, _ int64) (int64, error) {
	s.enqueued = append(s.enqueued, jobs...)
	project := s.projects[projectID]
	project.JobCounts[protocol.JobStatusTodo] += int64(len(jobs))
	s.projects[projectID] = project
	return int64(len(jobs)), nil
}

type fakeOAuth struct {
	member   bool
	verifier string
}

func (o *fakeOAuth) AuthorizationURL(state, challenge string) (string, error) {
	return "https://github.example/authorize?state=" + url.QueryEscape(state) + "&code_challenge=" + url.QueryEscape(challenge), nil
}
func (o *fakeOAuth) Exchange(_ context.Context, code, verifier string) (string, error) {
	if code != "accepted-code" {
		return "", io.EOF
	}
	o.verifier = verifier
	return "github-token", nil
}
func (o *fakeOAuth) User(context.Context, string) (tracker.GitHubIdentity, error) {
	return tracker.GitHubIdentity{UserID: 42, Login: "octocat"}, nil
}
func (o *fakeOAuth) TeamMembership(_ context.Context, token, organization, team, username string) (bool, error) {
	if token != "github-token" || organization != "saveweb" || team != "core" || username != "octocat" {
		return false, io.ErrUnexpectedEOF
	}
	return o.member, nil
}

func TestGitHubLoginAndAdminWorkflow(t *testing.T) {
	store := newFakeStore()
	oauth := &fakeOAuth{member: true}
	server := newTestServer(t, store, oauth)

	landing := request(t, server, http.MethodGet, "/", "", nil)
	if landing.Code != http.StatusOK || !strings.Contains(landing.Body.String(), "Sign in with GitHub") {
		t.Fatalf("landing = %d %q", landing.Code, landing.Body.String())
	}
	start := request(t, server, http.MethodGet, "/auth/github/start", "", nil)
	if start.Code != http.StatusFound {
		t.Fatalf("start status = %d", start.Code)
	}
	authorize, _ := url.Parse(start.Header().Get("Location"))
	if len(authorize.Query().Get("state")) != 43 || len(authorize.Query().Get("code_challenge")) != 43 {
		t.Fatalf("authorize URL = %s", authorize)
	}
	oauthCookie := responseCookie(t, start, oauthCookieName)
	callbackPath := "/auth/github/callback?code=accepted-code&state=" + url.QueryEscape(authorize.Query().Get("state"))
	callback := request(t, server, http.MethodGet, callbackPath, "", oauthCookie)
	if callback.Code != http.StatusSeeOther || callback.Header().Get("Location") != "/admin" || len(oauth.verifier) != 43 || store.upsertedID != 42 {
		t.Fatalf("callback = %d location=%q verifier=%q upsert=%d", callback.Code, callback.Header().Get("Location"), oauth.verifier, store.upsertedID)
	}
	sessionCookie := responseCookie(t, callback, sessionCookieName)
	if !sessionCookie.HttpOnly || sessionCookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("session cookie flags = %+v", sessionCookie)
	}

	dashboard := request(t, server, http.MethodGet, "/admin", "", sessionCookie)
	if dashboard.Code != http.StatusOK || !strings.Contains(dashboard.Body.String(), "octocat") {
		t.Fatalf("dashboard = %d %q", dashboard.Code, dashboard.Body.String())
	}
	csrf := extractCSRF(t, dashboard.Body.String())
	create := postForm(t, server, "/admin/projects", url.Values{"csrf": {csrf}, "project_id": {"demo"}, "status": {tracker.ProjectStatusActive}}, sessionCookie)
	if create.Code != http.StatusSeeOther || create.Header().Get("Location") != "/admin/projects/demo" {
		t.Fatalf("create = %d %q", create.Code, create.Header().Get("Location"))
	}
	detail := request(t, server, http.MethodGet, "/admin/projects/demo", "", sessionCookie)
	if detail.Code != http.StatusOK || !strings.Contains(detail.Body.String(), "Enqueue jobs") {
		t.Fatalf("detail = %d %q", detail.Code, detail.Body.String())
	}
	jobs := `[ {"id":"job-1","url":"https://example.com/","type":"archive"} ]`
	enqueue := postForm(t, server, "/admin/projects/demo/jobs", url.Values{"csrf": {csrf}, "jobs_json": {jobs}}, sessionCookie)
	if enqueue.Code != http.StatusSeeOther || len(store.enqueued) != 1 || store.enqueued[0].ID != "job-1" {
		t.Fatalf("enqueue = %d jobs=%+v", enqueue.Code, store.enqueued)
	}
	rejected := postForm(t, server, "/admin/projects/demo/status", url.Values{"csrf": {"wrong"}, "status": {tracker.ProjectStatusArchived}}, sessionCookie)
	if rejected.Code != http.StatusForbidden || store.projects["demo"].Status != tracker.ProjectStatusActive {
		t.Fatalf("invalid CSRF = %d project=%+v", rejected.Code, store.projects["demo"])
	}
	logout := postForm(t, server, "/logout", url.Values{"csrf": {csrf}}, sessionCookie)
	if logout.Code != http.StatusSeeOther || !store.deleted {
		t.Fatalf("logout = %d deleted=%v", logout.Code, store.deleted)
	}
}

func TestOAuthRejectsNonTeamMember(t *testing.T) {
	store := newFakeStore()
	server := newTestServer(t, store, &fakeOAuth{member: false})
	start := request(t, server, http.MethodGet, "/auth/github/start", "", nil)
	authorize, _ := url.Parse(start.Header().Get("Location"))
	callback := request(t, server, http.MethodGet, "/auth/github/callback?code=accepted-code&state="+url.QueryEscape(authorize.Query().Get("state")), "", responseCookie(t, start, oauthCookieName))
	if callback.Code != http.StatusForbidden || store.upsertedID != 0 {
		t.Fatalf("callback = %d upsert=%d", callback.Code, store.upsertedID)
	}
	for _, cookie := range callback.Result().Cookies() {
		if cookie.Name == sessionCookieName {
			t.Fatal("unauthorized user received a session cookie")
		}
	}
}

func newTestServer(t *testing.T, store *fakeStore, oauth *fakeOAuth) *echo.Echo {
	t.Helper()
	handler, err := New(store, oauth, Config{PublicURL: "https://hq.example", AdminOrganization: "saveweb", AdminTeam: "core", Secret: []byte("0123456789abcdef0123456789abcdef"), Clock: func() int64 { return 1_700_000_000 }}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server := echo.New()
	handler.Register(server)
	return server
}

func request(t *testing.T, server http.Handler, method, target, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if cookie != nil {
		req.AddCookie(cookie)
	}
	response := httptest.NewRecorder()
	server.ServeHTTP(response, req)
	return response
}

func postForm(t *testing.T, server http.Handler, target string, form url.Values, cookie *http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, req)
	return response
}

func responseCookie(t *testing.T, response *httptest.ResponseRecorder, name string) *http.Cookie {
	t.Helper()
	for _, cookie := range response.Result().Cookies() {
		if cookie.Name == name && cookie.MaxAge >= 0 {
			return cookie
		}
	}
	t.Fatalf("response has no %s cookie", name)
	return nil
}

func extractCSRF(t *testing.T, body string) string {
	t.Helper()
	match := regexp.MustCompile(`name="csrf" value="([^"]+)"`).FindStringSubmatch(body)
	if len(match) != 2 {
		t.Fatal("CSRF input not found")
	}
	return match[1]
}
