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
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const webTestNow = int64(1_780_000_000)

type fakeOAuth struct {
	state      string
	challenge  string
	verifier   string
	teamMember bool
	teamQuery  string
	teamErr    error
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

func (f *fakeOAuth) TeamMembership(
	_ context.Context, token, organization, team, username string,
) (bool, error) {
	if token != "github-access-token" {
		return false, context.Canceled
	}
	f.teamQuery = organization + "/" + team + "/" + username
	return f.teamMember, f.teamErr
}

type fakeStore struct {
	user         tracker.User
	sessions     map[string]string
	machineToken string
	updatedUser  string
	loginAdmin   bool
	putProject   tracker.Project
	putShard     tracker.Shard
	putReceiver  tracker.Receiver
	transition   struct {
		projectID  string
		shardID    string
		generation int64
		status     string
	}
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

func (s *fakeStore) UpsertGitHubUser(_ context.Context, _ tracker.GitHubIdentity, isAdmin bool, _ int64) (tracker.User, error) {
	s.loginAdmin = isAdmin
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

func (s *fakeStore) ListProjects(context.Context) ([]tracker.Project, error) {
	return []tracker.Project{{ID: "project-1", Status: tracker.ProjectStatusActive}}, nil
}

func (s *fakeStore) ListAdminShards(context.Context) ([]tracker.AdminShard, error) {
	return []tracker.AdminShard{{Shard: tracker.Shard{
		ProjectID: "project-1", ID: "shard-1", Status: tracker.ShardStatusActive,
		OwnerAgentID: "shard-agent-1", Generation: 3, OwnerLeaseExpiresAt: webTestNow + 120,
	}}}, nil
}

func (s *fakeStore) ListReceivers(context.Context) ([]tracker.Receiver, error) {
	return []tracker.Receiver{{
		ProjectID: "project-1", ID: "receiver-1", Status: tracker.ReceiverStatusActive,
		SinkURI: "s3://receiver/stage-1", Format: "jobs-jsonl-zstd-v1",
	}}, nil
}

func (s *fakeStore) ListShardAgents(context.Context) ([]tracker.Agent, error) {
	return []tracker.Agent{{
		ID: "shard-agent-1", Kind: protocol.AgentKindShard, Name: "Shard",
		Status: "online", EndpointStatus: tracker.EndpointHealthy,
	}}, nil
}

func (s *fakeStore) ListAuditEvents(context.Context, int) ([]tracker.AuditEvent, error) {
	return []tracker.AuditEvent{{
		ID: 1, ActorID: "admin", Action: "project.put", TargetID: "project-1",
		Reason: "created", CreatedAt: webTestNow,
	}}, nil
}

func (s *fakeStore) AdminPutProject(_ context.Context, _ string, project tracker.Project, _ string, _ int64) error {
	s.putProject = project
	return nil
}

func (s *fakeStore) AdminPutShard(_ context.Context, _ string, shard tracker.Shard, _ string, _ int64) error {
	s.putShard = shard
	return nil
}

func (s *fakeStore) AdminTransitionShard(
	_ context.Context, _, projectID, shardID string, generation int64, status, _ string, _ int64,
) error {
	s.transition.projectID = projectID
	s.transition.shardID = shardID
	s.transition.generation = generation
	s.transition.status = status
	return nil
}

func (s *fakeStore) AdminPutReceiver(_ context.Context, _ string, receiver tracker.Receiver, _ string, _ int64) error {
	s.putReceiver = receiver
	return nil
}

func TestGitHubOAuthPortalCSRFAndAdminFlow(t *testing.T) {
	store, oauth := newFakeStore(), &fakeOAuth{teamMember: true}
	handler, err := New(store, oauth, Config{
		PublicURL: "http://tracker.test", Secret: []byte("0123456789abcdef0123456789abcdef"),
		AdminOrganization: "saveweb", AdminTeam: "core",
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
	if oauth.teamQuery != "saveweb/core/alice" || !store.loginAdmin {
		t.Fatalf("team policy query = %q admin=%v", oauth.teamQuery, store.loginAdmin)
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

	projects := perform(server, http.MethodGet, "/admin/projects", "", sessionCookie, "")
	if projects.Code != http.StatusOK || !strings.Contains(projects.Body.String(), "Project administration") ||
		!strings.Contains(projects.Body.String(), "project-1/shard-1") ||
		!strings.Contains(projects.Body.String(), "s3://receiver/stage-1") {
		t.Fatalf("projects = %d %q", projects.Code, projects.Body.String())
	}
	projectForm := url.Values{
		"csrf": {csrfMatch[1]}, "project_id": {"project-2"}, "status": {"active"},
		"reason": {"new archive project"},
	}.Encode()
	projectUpdate := perform(server, http.MethodPost, "/admin/projects", projectForm, sessionCookie, "http://tracker.test")
	if projectUpdate.Code != http.StatusSeeOther || store.putProject.ID != "project-2" {
		t.Fatalf("project update = %d %+v", projectUpdate.Code, store.putProject)
	}
	shardForm := url.Values{
		"csrf": {csrfMatch[1]}, "project_id": {"project-2"}, "shard_id": {"shard-2"},
		"owner_agent_id": {"shard-agent-1"}, "status": {"loading"}, "generation": {"1"},
		"source_uri":    {"s3://sources/shard-2.jobs.jsonl.zst"},
		"source_format": {"jobs-jsonl-zstd-v1"}, "source_etag": {"etag-2"},
		"reason": {"attach immutable source"},
	}.Encode()
	shardUpdate := perform(server, http.MethodPost, "/admin/shards", shardForm, sessionCookie, "http://tracker.test")
	if shardUpdate.Code != http.StatusSeeOther || store.putShard.ID != "shard-2" ||
		store.putShard.SourceURI == nil || *store.putShard.SourceURI != "s3://sources/shard-2.jobs.jsonl.zst" {
		t.Fatalf("shard update = %d %+v", shardUpdate.Code, store.putShard)
	}
	unsafeShardForm := url.Values{
		"csrf": {csrfMatch[1]}, "project_id": {"project-2"}, "shard_id": {"shard-2"},
		"owner_agent_id": {"shard-agent-1"}, "status": {"active"}, "generation": {"2"},
		"reason": {"unsafe state edit"},
	}.Encode()
	unsafeShard := perform(server, http.MethodPost, "/admin/shards", unsafeShardForm, sessionCookie, "http://tracker.test")
	if unsafeShard.Code != http.StatusBadRequest || store.putShard.Generation != 1 {
		t.Fatalf("unsafe shard update = %d %+v", unsafeShard.Code, store.putShard)
	}
	transitionForm := url.Values{
		"csrf": {csrfMatch[1]}, "project_id": {"project-1"}, "shard_id": {"shard-1"},
		"expected_generation": {"3"}, "target_status": {"draining"}, "reason": {"planned pause"},
	}.Encode()
	transition := perform(server, http.MethodPost, "/admin/shards/transition", transitionForm, sessionCookie, "http://tracker.test")
	if transition.Code != http.StatusSeeOther || store.transition.projectID != "project-1" ||
		store.transition.shardID != "shard-1" || store.transition.generation != 3 ||
		store.transition.status != tracker.ShardStatusDraining {
		t.Fatalf("shard transition = %d %+v", transition.Code, store.transition)
	}
	receiverForm := url.Values{
		"csrf": {csrfMatch[1]}, "project_id": {"project-2"}, "receiver_id": {"stage-output"},
		"status": {"active"}, "sink_uri": {"s3://receiver/project-2/stage-output/"},
		"reason": {"collect next-stage jobs"},
	}.Encode()
	receiverUpdate := perform(server, http.MethodPost, "/admin/receivers", receiverForm, sessionCookie, "http://tracker.test")
	if receiverUpdate.Code != http.StatusSeeOther || store.putReceiver.ID != "stage-output" ||
		store.putReceiver.SinkURI != "s3://receiver/project-2/stage-output" {
		t.Fatalf("receiver update = %d %+v", receiverUpdate.Code, store.putReceiver)
	}
	badLogout := perform(
		server, http.MethodPost, "/logout", url.Values{"csrf": {"invalid"}}.Encode(), sessionCookie, "",
	)
	if badLogout.Code != http.StatusForbidden || len(store.sessions) != 1 {
		t.Fatalf("no-origin invalid-CSRF logout = %d sessions=%+v", badLogout.Code, store.sessions)
	}
	logout := perform(server, http.MethodPost, "/logout", form, sessionCookie, "")
	if logout.Code != http.StatusSeeOther || len(store.sessions) != 0 {
		t.Fatalf("no-origin logout = %d sessions=%+v", logout.Code, store.sessions)
	}
}

func TestGitHubOAuthNonmemberAndLookupFailure(t *testing.T) {
	for _, test := range []struct {
		name       string
		membership bool
		lookupErr  error
		wantStatus int
	}{
		{name: "ordinary user", wantStatus: http.StatusSeeOther},
		{name: "lookup failure", lookupErr: context.DeadlineExceeded, wantStatus: http.StatusBadGateway},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, oauth := newFakeStore(), &fakeOAuth{teamMember: test.membership, teamErr: test.lookupErr}
			handler, err := New(store, oauth, Config{
				PublicURL: "http://tracker.test", Secret: []byte("0123456789abcdef0123456789abcdef"),
				AdminOrganization: "test-org", AdminTeam: "test-team",
				Clock: func() int64 { return webTestNow },
			}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil {
				t.Fatal(err)
			}
			server := echo.New()
			handler.Register(server)
			start := perform(server, http.MethodGet, "/auth/github/start", "", nil, "")
			cookie := responseCookie(t, start, oauthCookieName)
			callback := perform(
				server, http.MethodGet,
				"/auth/github/callback?code=test-code&state="+url.QueryEscape(oauth.state),
				"", cookie, "",
			)
			if callback.Code != test.wantStatus || store.loginAdmin ||
				oauth.teamQuery != "test-org/test-team/alice" {
				t.Fatalf("callback = %d admin=%v query=%q", callback.Code, store.loginAdmin, oauth.teamQuery)
			}
			if test.lookupErr != nil && len(store.sessions) != 0 {
				t.Fatalf("lookup failure created sessions: %+v", store.sessions)
			}
		})
	}
}

func TestPostRequestOriginPolicy(t *testing.T) {
	handler := &Handler{publicOrigin: "https://tracker.test"}
	for _, test := range []struct {
		name    string
		origin  string
		referer string
		allowed bool
	}{
		{name: "same origin", origin: "https://tracker.test", allowed: true},
		{name: "cross origin", origin: "https://evil.test", allowed: false},
		{name: "origin takes precedence", origin: "https://evil.test", referer: "https://tracker.test/portal", allowed: false},
		{name: "same origin referer", referer: "https://tracker.test/portal", allowed: true},
		{name: "cross origin referer", referer: "https://evil.test/", allowed: false},
		{name: "userinfo referer", referer: "https://evil.test@tracker.test/portal", allowed: false},
		{name: "source headers omitted", allowed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "https://tracker.test/logout", nil)
			if test.origin != "" {
				request.Header.Set("Origin", test.origin)
			}
			if test.referer != "" {
				request.Header.Set("Referer", test.referer)
			}
			if got := handler.requestOriginAllowed(request); got != test.allowed {
				t.Fatalf("allowed = %v, want %v", got, test.allowed)
			}
		})
	}
}

func TestProjectAdministrationRequiresActiveAdmin(t *testing.T) {
	store := newFakeStore()
	store.user.Roles = map[string]bool{tracker.RoleWorker: true}
	handler, err := New(store, nil, Config{
		PublicURL: "http://tracker.test", Secret: []byte("0123456789abcdef0123456789abcdef"),
		Clock: func() int64 { return webTestNow },
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server := echo.New()
	handler.Register(server)
	sessionToken := strings.Repeat("a", 43)
	store.sessions[base64.RawURLEncoding.EncodeToString(sessionHash(sessionToken))] = store.user.ID
	cookie := &http.Cookie{Name: sessionCookieName, Value: sessionToken}

	response := perform(server, http.MethodGet, "/admin/projects", "", cookie, "")
	if response.Code != http.StatusForbidden || !strings.Contains(response.Body.String(), "Administrator role required") {
		t.Fatalf("non-admin project page = %d %q", response.Code, response.Body.String())
	}
	csrf := handler.csrfToken(sessionToken)
	form := url.Values{
		"csrf": {csrf}, "project_id": {"forbidden"}, "status": {"active"}, "reason": {"no access"},
	}.Encode()
	response = perform(server, http.MethodPost, "/admin/projects", form, cookie, "http://tracker.test")
	if response.Code != http.StatusForbidden || store.putProject.ID != "" {
		t.Fatalf("non-admin project command = %d %+v", response.Code, store.putProject)
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
