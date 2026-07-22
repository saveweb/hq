package trackerweb

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/saveweb/hq/internal/sourceformat"
	"github.com/saveweb/hq/internal/tracker"
	"github.com/saveweb/hq/pkg/protocol"
)

type fakeStore struct {
	user         tracker.User
	sessions     map[string]bool
	projects     map[string]protocol.AdminProjectSummary
	enqueued     []protocol.AdminEnqueueJob
	jobs         map[string][]protocol.AdminJob
	users        map[string]protocol.AdminUserSummary
	tokens       map[string]string
	deleted      bool
	upsertedID   int64
	registeredID int64
}

func newFakeStore() *fakeStore {
	return &fakeStore{sessions: map[string]bool{}, projects: map[string]protocol.AdminProjectSummary{}, jobs: map[string][]protocol.AdminJob{}, users: map[string]protocol.AdminUserSummary{}, tokens: map[string]string{}}
}

func (s *fakeStore) UpsertGitHubAdmin(_ context.Context, identity tracker.GitHubIdentity, now int64) (tracker.User, error) {
	s.upsertedID = identity.UserID
	s.user = tracker.User{ID: "gh_42", GitHubUserID: &identity.UserID, GitHubLogin: identity.Login, Status: tracker.UserStatusActive, Roles: map[string]bool{tracker.RoleAdmin: true}, LastLoginAt: &now}
	return s.user, nil
}
func (s *fakeStore) UpsertGitHubPendingWorker(_ context.Context, identity tracker.GitHubIdentity, now int64) (tracker.User, error) {
	s.registeredID = identity.UserID
	id := "gh_42"
	summary, exists := s.users[id]
	if !exists {
		summary = protocol.AdminUserSummary{ID: id, Status: tracker.UserStatusPending, Roles: []string{tracker.RoleWorker}}
	}
	summary.GitHubLogin = identity.Login
	s.users[id] = summary
	roles := map[string]bool{}
	for _, role := range summary.Roles {
		roles[role] = true
	}
	s.user = tracker.User{ID: id, GitHubUserID: &identity.UserID, GitHubLogin: identity.Login, Status: summary.Status, Roles: roles, LastLoginAt: &now}
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
	existing.ClientVersions = append([]string(nil), project.ClientVersions...)
	existing.DispatchQPS = project.DispatchQPS
	existing.WorkerClaimQPS = project.WorkerClaimQPS
	existing.MaxJobsPerClaim = project.MaxJobsPerClaim
	if project.MaxResets != nil {
		existing.MaxResets = *project.MaxResets
	}
	if project.RecommendedLeaseSeconds != nil {
		existing.RecommendedLeaseSeconds = *project.RecommendedLeaseSeconds
	}
	if existing.IdentityMode == "" {
		existing.IdentityMode = project.IdentityMode
	}
	if project.ClaimOrder != "" {
		existing.ClaimOrder = project.ClaimOrder
	} else if existing.ClaimOrder == "" {
		existing.ClaimOrder = tracker.ClaimOrderFIFO
	}
	if existing.JobCounts == nil {
		existing.JobCounts = map[string]int64{}
	}
	s.projects[project.ID] = existing
	return nil
}
func (s *fakeStore) EnqueueProjectJobs(_ context.Context, projectID string, jobs []protocol.AdminEnqueueJob, now int64) (int64, error) {
	s.enqueued = append(s.enqueued, jobs...)
	for _, spec := range jobs {
		var randomKey int32
		if spec.RandomKey != nil {
			randomKey = *spec.RandomKey
		}
		s.jobs[projectID] = append(s.jobs[projectID], protocol.AdminJob{JobSpecV1: protocol.JobSpecV1{ID: spec.ID, Value: spec.Value, Type: spec.Type, Via: spec.Via, Hops: spec.Hops, Attrs: spec.Attrs}, JobID: int64(len(s.jobs[projectID]) + 1), RandomKey: randomKey, Status: protocol.JobStatusTodo, CreatedAt: now, UpdatedAt: now})
	}
	project := s.projects[projectID]
	project.JobCounts[protocol.JobStatusTodo] += int64(len(jobs))
	s.projects[projectID] = project
	return int64(len(jobs)), nil
}
func (s *fakeStore) ListUsers(context.Context) ([]protocol.AdminUserSummary, error) {
	result := []protocol.AdminUserSummary{}
	for _, user := range s.users {
		result = append(result, user)
	}
	return result, nil
}
func (s *fakeStore) MachineToken(_ context.Context, id string) (string, bool, error) {
	active := s.users[id].MachineTokenActive
	if !active {
		return "", false, nil
	}
	return s.tokens[id], true, nil
}
func (s *fakeStore) PutUser(_ context.Context, id, status string, roles []string, _ int64) error {
	s.users[id] = protocol.AdminUserSummary{ID: id, Status: status, Roles: roles}
	return nil
}
func (s *fakeStore) DeleteUser(_ context.Context, id string) error {
	delete(s.users, id)
	return nil
}
func (s *fakeStore) RotateMachineToken(_ context.Context, id, token string, _ int64) error {
	user := s.users[id]
	user.ID, user.MachineTokenActive, user.MachineTokenViewable = id, true, true
	s.users[id] = user
	s.tokens[id] = token
	return nil
}
func (s *fakeStore) RevokeMachineToken(_ context.Context, id string, _ int64) error {
	user := s.users[id]
	user.MachineTokenActive, user.MachineTokenViewable = false, false
	s.users[id] = user
	return nil
}
func (s *fakeStore) DeleteProject(_ context.Context, id string) error {
	delete(s.projects, id)
	delete(s.jobs, id)
	return nil
}
func (s *fakeStore) ListProjectJobs(_ context.Context, id, _ string, _ int64, _ int) (protocol.AdminJobListResponse, error) {
	return protocol.AdminJobListResponse{Jobs: s.jobs[id]}, nil
}
func (s *fakeStore) ProjectJob(_ context.Context, id string, jobID int64) (protocol.AdminJob, error) {
	for _, job := range s.jobs[id] {
		if job.JobID == jobID {
			return job, nil
		}
	}
	return protocol.AdminJob{}, &tracker.Error{Code: protocol.ErrorNotFound, Message: "missing"}
}
func (s *fakeStore) RequeueProjectJob(_ context.Context, id string, jobID, _ int64) error {
	for index := range s.jobs[id] {
		if s.jobs[id][index].JobID == jobID {
			s.jobs[id][index].Status = protocol.JobStatusTodo
		}
	}
	return nil
}
func (s *fakeStore) DeleteProjectJob(_ context.Context, id string, jobID int64) error {
	jobs := s.jobs[id]
	for index := range jobs {
		if jobs[index].JobID == jobID {
			s.jobs[id] = append(jobs[:index], jobs[index+1:]...)
			break
		}
	}
	return nil
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
	if landing.Code != http.StatusOK || !strings.Contains(landing.Body.String(), "Continue with GitHub") {
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
	create := postForm(t, server, "/admin/projects", url.Values{"csrf": {csrf}, "project_id": {"demo"}, "status": {tracker.ProjectStatusActive}, "identity_mode": {tracker.IdentityModeUniqueValue}, "claim_order": {tracker.ClaimOrderRandom}, "client_versions": {"worker-v2\nworker-v1"}}, sessionCookie)
	if create.Code != http.StatusSeeOther || create.Header().Get("Location") != "/admin/projects/demo" {
		t.Fatalf("create = %d %q", create.Code, create.Header().Get("Location"))
	}
	detail := request(t, server, http.MethodGet, "/admin/projects/demo", "", sessionCookie)
	if detail.Code != http.StatusOK || !strings.Contains(detail.Body.String(), "Enqueue jobs") || !strings.Contains(detail.Body.String(), tracker.IdentityModeUniqueValue) || !strings.Contains(detail.Body.String(), "Claim order random") || !strings.Contains(detail.Body.String(), "Recommended lease (seconds)") ||
		!strings.Contains(detail.Body.String(), "1700000000 (2023-11-14 22:13:20 UTC)") {
		t.Fatalf("detail = %d %q", detail.Code, detail.Body.String())
	}
	settings := postForm(t, server, "/admin/projects/demo/status", url.Values{"csrf": {csrf}, "status": {tracker.ProjectStatusActive}, "claim_order": {tracker.ClaimOrderFIFO}, "recommended_lease_seconds": {"120"}, "client_versions": {"worker-v2\nworker-v1"}}, sessionCookie)
	if settings.Code != http.StatusSeeOther || store.projects["demo"].ClaimOrder != tracker.ClaimOrderFIFO || store.projects["demo"].RecommendedLeaseSeconds != 120 || len(store.projects["demo"].ClientVersions) != 2 {
		t.Fatalf("settings = %d project=%+v", settings.Code, store.projects["demo"])
	}
	jobs := `[ {"value":"https://example.com/","type":"archive","random_key":-9} ]`
	enqueue := postForm(t, server, "/admin/projects/demo/jobs", url.Values{"csrf": {csrf}, "jobs_json": {jobs}}, sessionCookie)
	if enqueue.Code != http.StatusSeeOther || len(store.enqueued) != 1 || store.enqueued[0].Value != "https://example.com/" || store.enqueued[0].RandomKey == nil || *store.enqueued[0].RandomKey != -9 {
		t.Fatalf("enqueue = %d jobs=%+v", enqueue.Code, store.enqueued)
	}
	jobDetail := request(t, server, http.MethodGet, "/admin/projects/demo/jobs/1", "", sessionCookie)
	if jobDetail.Code != http.StatusOK || !strings.Contains(jobDetail.Body.String(), "https://example.com/") ||
		!strings.Contains(jobDetail.Body.String(), "1700000000 (2023-11-14 22:13:20 UTC)") {
		t.Fatalf("job detail = %d %q", jobDetail.Code, jobDetail.Body.String())
	}
	users := request(t, server, http.MethodGet, "/admin/users", "", sessionCookie)
	if users.Code != http.StatusOK || !strings.Contains(users.Body.String(), "Create user") {
		t.Fatalf("users = %d %q", users.Code, users.Body.String())
	}
	putUser := postForm(t, server, "/admin/users", url.Values{"csrf": {csrf}, "user_id": {"worker-web"}, "status": {tracker.UserStatusActive}, "roles": {tracker.RoleWorker}}, sessionCookie)
	if putUser.Code != http.StatusSeeOther || store.users["worker-web"].Status != tracker.UserStatusActive {
		t.Fatalf("put user = %d user=%+v", putUser.Code, store.users["worker-web"])
	}
	editUser := request(t, server, http.MethodGet, "/admin/users?user_id=worker-web", "", sessionCookie)
	editBody := editUser.Body.String()
	if editUser.Code != http.StatusOK || !strings.Contains(editBody, "Update user") ||
		!strings.Contains(editBody, `value="worker-web" readonly`) ||
		!strings.Contains(editBody, `value="active" selected`) ||
		!strings.Contains(editBody, `value="worker" checked`) {
		t.Fatalf("edit user = %d %q", editUser.Code, editBody)
	}
	token := postForm(t, server, "/admin/users/worker-web/token", url.Values{"csrf": {csrf}}, sessionCookie)
	if token.Code != http.StatusOK || !strings.Contains(token.Body.String(), "hq_") || !store.users["worker-web"].MachineTokenActive {
		t.Fatalf("rotate token = %d %q", token.Code, token.Body.String())
	}
	viewToken := request(t, server, http.MethodGet, "/admin/users/worker-web/token", "", sessionCookie)
	if viewToken.Code != http.StatusOK || !strings.Contains(viewToken.Body.String(), store.tokens["worker-web"]) {
		t.Fatalf("view token = %d %q", viewToken.Code, viewToken.Body.String())
	}
	deletedUser := postForm(t, server, "/admin/users/worker-web/delete", url.Values{"csrf": {csrf}}, sessionCookie)
	if deletedUser.Code != http.StatusSeeOther {
		t.Fatalf("delete user = %d", deletedUser.Code)
	}
	if _, exists := store.users["worker-web"]; exists {
		t.Fatal("deleted user remains in store")
	}
	deleteJob := postForm(t, server, "/admin/projects/demo/jobs/1/delete", url.Values{"csrf": {csrf}}, sessionCookie)
	if deleteJob.Code != http.StatusSeeOther || len(store.jobs["demo"]) != 0 {
		t.Fatalf("delete job = %d jobs=%+v", deleteJob.Code, store.jobs["demo"])
	}
	rejected := postForm(t, server, "/admin/projects/demo/status", url.Values{"csrf": {"wrong"}, "status": {tracker.ProjectStatusArchived}}, sessionCookie)
	if rejected.Code != http.StatusForbidden || store.projects["demo"].Status != tracker.ProjectStatusActive {
		t.Fatalf("invalid CSRF = %d project=%+v", rejected.Code, store.projects["demo"])
	}
	deletedProject := postForm(t, server, "/admin/projects/demo/delete", url.Values{"csrf": {csrf}}, sessionCookie)
	if deletedProject.Code != http.StatusSeeOther {
		t.Fatalf("delete project = %d", deletedProject.Code)
	}
	logout := postForm(t, server, "/logout", url.Values{"csrf": {csrf}}, sessionCookie)
	if logout.Code != http.StatusSeeOther || !store.deleted {
		t.Fatalf("logout = %d deleted=%v", logout.Code, store.deleted)
	}
}

func TestOAuthRegistersNonTeamMemberAsPendingWorker(t *testing.T) {
	store := newFakeStore()
	server := newTestServer(t, store, &fakeOAuth{member: false})
	start := request(t, server, http.MethodGet, "/auth/github/start", "", nil)
	authorize, _ := url.Parse(start.Header().Get("Location"))
	callback := request(t, server, http.MethodGet, "/auth/github/callback?code=accepted-code&state="+url.QueryEscape(authorize.Query().Get("state")), "", responseCookie(t, start, oauthCookieName))
	if callback.Code != http.StatusOK || store.upsertedID != 0 || store.registeredID != 42 ||
		!strings.Contains(callback.Body.String(), "Worker registration") ||
		store.users["gh_42"].Status != tracker.UserStatusPending {
		t.Fatalf("callback = %d admin-upsert=%d registered=%d body=%q", callback.Code, store.upsertedID, store.registeredID, callback.Body.String())
	}
	for _, cookie := range callback.Result().Cookies() {
		if cookie.Name == sessionCookieName {
			t.Fatal("unauthorized user received a session cookie")
		}
	}
}

func TestActiveWorkerManagesOwnMachineToken(t *testing.T) {
	store := newFakeStore()
	store.users["gh_42"] = protocol.AdminUserSummary{
		ID: "gh_42", Status: tracker.UserStatusActive, Roles: []string{tracker.RoleWorker},
	}
	server := newTestServer(t, store, &fakeOAuth{member: false})
	start := request(t, server, http.MethodGet, "/auth/github/start", "", nil)
	authorize, _ := url.Parse(start.Header().Get("Location"))
	callback := request(t, server, http.MethodGet, "/auth/github/callback?code=accepted-code&state="+url.QueryEscape(authorize.Query().Get("state")), "", responseCookie(t, start, oauthCookieName))
	if callback.Code != http.StatusSeeOther || callback.Header().Get("Location") != "/worker" {
		t.Fatalf("worker callback = %d location=%q", callback.Code, callback.Header().Get("Location"))
	}
	sessionCookie := responseCookie(t, callback, sessionCookieName)
	portal := request(t, server, http.MethodGet, "/worker", "", sessionCookie)
	if portal.Code != http.StatusOK || !strings.Contains(portal.Body.String(), "Generate token") {
		t.Fatalf("worker portal = %d %q", portal.Code, portal.Body.String())
	}
	csrf := extractCSRF(t, portal.Body.String())
	token := postForm(t, server, "/worker/token", url.Values{"csrf": {csrf}}, sessionCookie)
	if token.Code != http.StatusOK || !strings.Contains(token.Body.String(), "hq_") || !store.users["gh_42"].MachineTokenActive {
		t.Fatalf("worker token = %d %q", token.Code, token.Body.String())
	}
	portal = request(t, server, http.MethodGet, "/worker", "", sessionCookie)
	if portal.Code != http.StatusOK || !strings.Contains(portal.Body.String(), "View token") ||
		!strings.Contains(portal.Body.String(), store.tokens["gh_42"]) {
		t.Fatalf("worker token view = %d %q", portal.Code, portal.Body.String())
	}
	admin := request(t, server, http.MethodGet, "/admin", "", sessionCookie)
	if admin.Code != http.StatusForbidden {
		t.Fatalf("worker admin access = %d", admin.Code)
	}
	revoke := postForm(t, server, "/worker/token/revoke", url.Values{"csrf": {csrf}}, sessionCookie)
	if revoke.Code != http.StatusSeeOther || store.users["gh_42"].MachineTokenActive {
		t.Fatalf("worker revoke = %d active=%v", revoke.Code, store.users["gh_42"].MachineTokenActive)
	}
	logout := postForm(t, server, "/logout", url.Values{"csrf": {csrf}}, sessionCookie)
	if logout.Code != http.StatusSeeOther {
		t.Fatalf("worker logout = %d", logout.Code)
	}
}

func TestSourceUploadAcceptsMultipartCSRF(t *testing.T) {
	store := newFakeStore()
	store.user = tracker.User{ID: "admin", Status: tracker.UserStatusActive, Roles: map[string]bool{tracker.RoleAdmin: true}}
	store.projects["demo"] = protocol.AdminProjectSummary{
		ID: "demo", Status: tracker.ProjectStatusActive, IdentityMode: tracker.IdentityModeUniqueValue,
		JobCounts: map[string]int64{},
	}
	sessionToken := strings.Repeat("s", 43)
	store.sessions[string(sessionHash(sessionToken))] = true
	sessionCookie := &http.Cookie{Name: sessionCookieName, Value: sessionToken}
	server := newTestServer(t, store, &fakeOAuth{member: true})

	detail := request(t, server, http.MethodGet, "/admin/projects/demo", "", sessionCookie)
	csrf := extractCSRF(t, detail.Body.String())

	var source bytes.Buffer
	encoder, err := sourceformat.NewEncoder(&source)
	if err != nil {
		t.Fatal(err)
	}
	if err := encoder.Write(protocol.JobSpecV1{Value: "123"}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}

	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	if err := form.WriteField("csrf", csrf); err != nil {
		t.Fatal(err)
	}
	file, err := form.CreateFormFile("source_file", "jobs.jsonl.zst")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(source.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/projects/demo/source", &body)
	req.Header.Set("Content-Type", form.FormDataContentType())
	req.AddCookie(sessionCookie)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, req)
	if response.Code != http.StatusSeeOther || len(store.enqueued) != 1 || store.enqueued[0].Value != "123" {
		t.Fatalf("source upload = %d jobs=%+v body=%q", response.Code, store.enqueued, response.Body.String())
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
