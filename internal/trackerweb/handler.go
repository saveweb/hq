// Package trackerweb implements the GitHub-authenticated HQ administration UI.
package trackerweb

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/sourceformat"
	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const (
	sessionCookieName  = "saveweb_hq_session"
	oauthCookieName    = "saveweb_hq_oauth"
	oauthTTL           = 10 * time.Minute
	defaultSessionTTL  = 12 * time.Hour
	maxFormBytes       = int64(1 << 20)
	maxSourceFormBytes = int64(257 << 20)
	maxSourceBytes     = int64(256 << 20)
	maxExpandedBytes   = int64(4 << 30)
	maxSourceJobs      = int64(10_000_000)
)

type Store interface {
	UpsertGitHubAdmin(context.Context, tracker.GitHubIdentity, int64) (tracker.User, error)
	UpsertGitHubPendingWorker(context.Context, tracker.GitHubIdentity, int64) (tracker.User, error)
	CreateWebSession(context.Context, string, []byte, int64, int64) error
	AuthenticateWebSession(context.Context, []byte, int64) (tracker.User, error)
	DeleteWebSession(context.Context, []byte) error
	ListProjectSummaries(context.Context) ([]protocol.AdminProjectSummary, error)
	ProjectSummary(context.Context, string) (protocol.AdminProjectSummary, error)
	PutProject(context.Context, tracker.Project, int64) error
	EnqueueProjectJobs(context.Context, string, []protocol.JobSpecV1, int64) (int64, error)
	ListUsers(context.Context) ([]protocol.AdminUserSummary, error)
	PutUser(context.Context, string, string, []string, int64) error
	DeleteUser(context.Context, string) error
	RotateMachineToken(context.Context, string, string, int64) error
	RevokeMachineToken(context.Context, string, int64) error
	DeleteProject(context.Context, string) error
	ListProjectJobs(context.Context, string, string, int64, int) (protocol.AdminJobListResponse, error)
	ProjectJob(context.Context, string, int64) (protocol.AdminJob, error)
	RequeueProjectJob(context.Context, string, int64, int64) error
	DeleteProjectJob(context.Context, string, int64) error
}

type OAuth interface {
	AuthorizationURL(state, codeChallenge string) (string, error)
	Exchange(context.Context, string, string) (string, error)
	User(context.Context, string) (tracker.GitHubIdentity, error)
	TeamMembership(context.Context, string, string, string, string) (bool, error)
}

type Config struct {
	PublicURL, AdminOrganization, AdminTeam string
	Secret                                  []byte
	SessionTTL                              time.Duration
	Clock                                   func() int64
}

type Handler struct {
	store                   Store
	oauth                   OAuth
	logger                  *slog.Logger
	secret                  []byte
	secureCookies           bool
	adminOrganization, team string
	sessionTTL              time.Duration
	clock                   func() int64
}

func New(store Store, oauth OAuth, config Config, logger *slog.Logger) (*Handler, error) {
	publicURL, err := url.Parse(config.PublicURL)
	if err != nil || publicURL.Host == "" || (publicURL.Scheme != "https" && publicURL.Scheme != "http") ||
		(publicURL.Path != "" && publicURL.Path != "/") || publicURL.RawQuery != "" || publicURL.Fragment != "" ||
		len(config.Secret) < 32 || oauth == nil {
		return nil, fmt.Errorf("tracker web: invalid public URL, secret, or OAuth client")
	}
	for _, value := range []string{config.AdminOrganization, config.AdminTeam} {
		if value == "" || len(value) > 255 || strings.TrimSpace(value) != value {
			return nil, fmt.Errorf("tracker web: invalid admin organization or team")
		}
	}
	if config.SessionTTL == 0 {
		config.SessionTTL = defaultSessionTTL
	}
	if config.SessionTTL < time.Minute || config.SessionTTL > 7*24*time.Hour {
		return nil, fmt.Errorf("tracker web: session TTL is outside 1m-7d")
	}
	if config.Clock == nil {
		config.Clock = func() int64 { return time.Now().Unix() }
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		store: store, oauth: oauth, logger: logger, secret: append([]byte(nil), config.Secret...),
		secureCookies: publicURL.Scheme == "https", adminOrganization: config.AdminOrganization,
		team: config.AdminTeam, sessionTTL: config.SessionTTL, clock: config.Clock,
	}, nil
}

func (h *Handler) Register(server *echo.Echo) {
	server.GET("/", h.landing)
	server.GET("/assets/admin.css", h.stylesheet)
	server.GET("/auth/github/start", h.oauthStart)
	server.GET("/auth/github/callback", h.oauthCallback)
	server.POST("/logout", h.logout)
	server.GET("/admin", h.dashboard)
	server.GET("/admin/users", h.users)
	server.POST("/admin/users", h.putUser)
	server.POST("/admin/users/:user_id/delete", h.deleteUser)
	server.POST("/admin/users/:user_id/token", h.rotateUserToken)
	server.POST("/admin/users/:user_id/token/revoke", h.revokeUserToken)
	server.POST("/admin/projects", h.createProject)
	server.GET("/admin/projects/:project_id", h.project)
	server.POST("/admin/projects/:project_id/status", h.updateProjectStatus)
	server.POST("/admin/projects/:project_id/delete", h.deleteProject)
	server.POST("/admin/projects/:project_id/jobs", h.enqueueJobs)
	server.POST("/admin/projects/:project_id/source", h.enqueueSource)
	server.GET("/admin/projects/:project_id/jobs/:job_id", h.job)
	server.POST("/admin/projects/:project_id/jobs/:job_id/requeue", h.requeueJob)
	server.POST("/admin/projects/:project_id/jobs/:job_id/delete", h.deleteJob)
}

func (h *Handler) landing(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	if _, _, err := h.currentUser(ctx); err == nil {
		return ctx.Redirect(http.StatusSeeOther, "/admin")
	} else if !errors.Is(err, tracker.ErrWebSessionNotFound) {
		return h.internal(ctx, err)
	}
	return render(ctx, http.StatusOK, "login", nil)
}

func (h *Handler) stylesheet(ctx *echo.Context) error {
	ctx.Response().Header().Set("Content-Type", "text/css; charset=utf-8")
	ctx.Response().Header().Set("Cache-Control", "public, max-age=3600")
	return ctx.String(http.StatusOK, adminCSS)
}

func (h *Handler) oauthStart(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	state, err := randomValue()
	if err != nil {
		return h.internal(ctx, err)
	}
	verifier, err := randomValue()
	if err != nil {
		return h.internal(ctx, err)
	}
	digest := sha256.Sum256([]byte(verifier))
	authorizeURL, err := h.oauth.AuthorizationURL(state, base64.RawURLEncoding.EncodeToString(digest[:]))
	if err != nil {
		return h.internal(ctx, err)
	}
	expiresAt := h.clock() + int64(oauthTTL/time.Second)
	ctx.SetCookie(&http.Cookie{
		Name: oauthCookieName, Value: h.signOAuthTransaction(state, verifier, expiresAt),
		Path: "/auth/github/callback", MaxAge: int(oauthTTL / time.Second), HttpOnly: true,
		Secure: h.secureCookies, SameSite: http.SameSiteLaxMode,
	})
	return ctx.Redirect(http.StatusFound, authorizeURL)
}

func (h *Handler) oauthCallback(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	cookie, err := ctx.Cookie(oauthCookieName)
	h.clearCookie(ctx, oauthCookieName, "/auth/github/callback")
	if err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Login transaction is missing or expired")
	}
	state, verifier, err := h.verifyOAuthTransaction(cookie.Value)
	if err != nil || !hmac.Equal([]byte(state), []byte(ctx.QueryParam("state"))) || ctx.QueryParam("error") != "" || ctx.QueryParam("code") == "" {
		return h.pageError(ctx, http.StatusBadRequest, "GitHub authorization was not completed")
	}
	requestContext, cancel := context.WithTimeout(ctx.Request().Context(), 20*time.Second)
	defer cancel()
	accessToken, err := h.oauth.Exchange(requestContext, ctx.QueryParam("code"), verifier)
	if err != nil {
		h.logger.Warn("GitHub OAuth exchange failed", "error", err)
		return h.pageError(ctx, http.StatusBadGateway, "GitHub login failed")
	}
	identity, err := h.oauth.User(requestContext, accessToken)
	if err != nil {
		return h.pageError(ctx, http.StatusBadGateway, "GitHub identity lookup failed")
	}
	member, err := h.oauth.TeamMembership(requestContext, accessToken, h.adminOrganization, h.team, identity.Login)
	accessToken = ""
	if err != nil {
		h.logger.Warn("GitHub team membership lookup failed", "error", err)
		return h.pageError(ctx, http.StatusBadGateway, "GitHub team membership lookup failed")
	}
	now := h.clock()
	if !member {
		user, err := h.store.UpsertGitHubPendingWorker(requestContext, identity, now)
		if err != nil {
			return h.internal(ctx, err)
		}
		return render(ctx, http.StatusOK, "worker-registration", map[string]any{"User": user})
	}
	user, err := h.store.UpsertGitHubAdmin(requestContext, identity, now)
	if err != nil {
		return h.internal(ctx, err)
	}
	sessionToken, err := randomValue()
	if err != nil {
		return h.internal(ctx, err)
	}
	if err := h.store.CreateWebSession(requestContext, user.ID, sessionHash(sessionToken), now, now+int64(h.sessionTTL/time.Second)); err != nil {
		return h.internal(ctx, err)
	}
	ctx.SetCookie(&http.Cookie{
		Name: sessionCookieName, Value: sessionToken, Path: "/", MaxAge: int(h.sessionTTL / time.Second),
		HttpOnly: true, Secure: h.secureCookies, SameSite: http.SameSiteLaxMode,
	})
	return ctx.Redirect(http.StatusSeeOther, "/admin")
}

func (h *Handler) dashboard(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	user, sessionToken, ok := h.requireAdmin(ctx)
	if !ok {
		return nil
	}
	projects, err := h.store.ListProjectSummaries(ctx.Request().Context())
	if err != nil {
		return h.internal(ctx, err)
	}
	return render(ctx, http.StatusOK, "dashboard", map[string]any{
		"User": user, "Projects": projects, "CSRF": h.csrfToken(sessionToken),
	})
}

func (h *Handler) users(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	user, sessionToken, ok := h.requireAdmin(ctx)
	if !ok {
		return nil
	}
	users, err := h.store.ListUsers(ctx.Request().Context())
	if err != nil {
		return h.internal(ctx, err)
	}
	selectedID := ctx.QueryParam("user_id")
	var editUser protocol.AdminUserSummary
	editing, editWorker, editAdmin := false, false, false
	if selectedID != "" {
		for _, candidate := range users {
			if candidate.ID != selectedID {
				continue
			}
			editUser, editing = candidate, true
			for _, role := range candidate.Roles {
				editWorker = editWorker || role == tracker.RoleWorker
				editAdmin = editAdmin || role == tracker.RoleAdmin
			}
			break
		}
		if !editing {
			return h.pageError(ctx, http.StatusNotFound, "User not found")
		}
	}
	return render(ctx, http.StatusOK, "users", map[string]any{
		"User": user, "Users": users, "CSRF": h.csrfToken(sessionToken),
		"Editing": editing, "EditUser": editUser, "EditWorker": editWorker, "EditAdmin": editAdmin,
	})
}

func (h *Handler) putUser(ctx *echo.Context) error {
	_, _, ok := h.authorizePost(ctx)
	if !ok {
		return nil
	}
	if err := ctx.Request().ParseForm(); err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Invalid form submission")
	}
	if err := h.store.PutUser(ctx.Request().Context(), ctx.FormValue("user_id"), ctx.FormValue("status"), ctx.Request().Form["roles"], h.clock()); err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "User update was rejected")
	}
	return ctx.Redirect(http.StatusSeeOther, "/admin/users")
}

func (h *Handler) deleteUser(ctx *echo.Context) error {
	_, _, ok := h.authorizePost(ctx)
	if !ok {
		return nil
	}
	if err := h.store.DeleteUser(ctx.Request().Context(), ctx.Param("user_id")); err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "User deletion was rejected")
	}
	return ctx.Redirect(http.StatusSeeOther, "/admin/users")
}

func (h *Handler) rotateUserToken(ctx *echo.Context) error {
	user, sessionToken, ok := h.authorizePost(ctx)
	if !ok {
		return nil
	}
	random, err := randomValue()
	if err != nil {
		return h.internal(ctx, err)
	}
	token := "hq_" + random
	userID := ctx.Param("user_id")
	if err := h.store.RotateMachineToken(ctx.Request().Context(), userID, token, h.clock()); err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Token rotation was rejected")
	}
	h.webHeaders(ctx.Response().Header())
	return render(ctx, http.StatusOK, "token", map[string]any{"User": user, "UserID": userID, "Token": token, "CSRF": h.csrfToken(sessionToken)})
}

func (h *Handler) revokeUserToken(ctx *echo.Context) error {
	_, _, ok := h.authorizePost(ctx)
	if !ok {
		return nil
	}
	if err := h.store.RevokeMachineToken(ctx.Request().Context(), ctx.Param("user_id"), h.clock()); err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Token revocation was rejected")
	}
	return ctx.Redirect(http.StatusSeeOther, "/admin/users")
}

func (h *Handler) project(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	user, sessionToken, ok := h.requireAdmin(ctx)
	if !ok {
		return nil
	}
	project, err := h.store.ProjectSummary(ctx.Request().Context(), ctx.Param("project_id"))
	if err != nil {
		if tracker.IsCode(err, protocol.ErrorNotFound) {
			return h.pageError(ctx, http.StatusNotFound, "Project not found")
		}
		return h.internal(ctx, err)
	}
	jobs, err := h.store.ListProjectJobs(ctx.Request().Context(), project.ID, "", 0, 100)
	if err != nil {
		return h.internal(ctx, err)
	}
	return render(ctx, http.StatusOK, "project", map[string]any{
		"User": user, "Project": project, "Jobs": jobs.Jobs, "CSRF": h.csrfToken(sessionToken), "JobExample": jobExample(project.IdentityMode),
	})
}

func (h *Handler) createProject(ctx *echo.Context) error {
	_, _, ok := h.authorizePost(ctx)
	if !ok {
		return nil
	}
	project := tracker.Project{ID: ctx.FormValue("project_id"), Status: ctx.FormValue("status"), IdentityMode: ctx.FormValue("identity_mode")}
	if err := h.store.PutProject(ctx.Request().Context(), project, h.clock()); err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Project update was rejected")
	}
	return ctx.Redirect(http.StatusSeeOther, "/admin/projects/"+url.PathEscape(project.ID))
}

func jobExample(identityMode string) string {
	if identityMode == tracker.IdentityModeExternalID {
		return "[{\n  \"id\": \"source-1\",\n  \"value\": \"example\"\n}]"
	}
	return "[{\n  \"value\": \"example\"\n}]"
}

func (h *Handler) updateProjectStatus(ctx *echo.Context) error {
	_, _, ok := h.authorizePost(ctx)
	if !ok {
		return nil
	}
	projectID := ctx.Param("project_id")
	if err := h.store.PutProject(ctx.Request().Context(), tracker.Project{ID: projectID, Status: ctx.FormValue("status")}, h.clock()); err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Project update was rejected")
	}
	return ctx.Redirect(http.StatusSeeOther, "/admin/projects/"+url.PathEscape(projectID))
}

func (h *Handler) deleteProject(ctx *echo.Context) error {
	_, _, ok := h.authorizePost(ctx)
	if !ok {
		return nil
	}
	if err := h.store.DeleteProject(ctx.Request().Context(), ctx.Param("project_id")); err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Project deletion was rejected")
	}
	return ctx.Redirect(http.StatusSeeOther, "/admin")
}

func (h *Handler) enqueueJobs(ctx *echo.Context) error {
	_, _, ok := h.authorizePost(ctx)
	if !ok {
		return nil
	}
	decoder := json.NewDecoder(io.LimitReader(strings.NewReader(ctx.FormValue("jobs_json")), maxFormBytes))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var jobs []protocol.JobSpecV1
	if err := decoder.Decode(&jobs); err != nil || decoder.Decode(&struct{}{}) != io.EOF || len(jobs) == 0 || len(jobs) > 256 {
		return h.pageError(ctx, http.StatusBadRequest, "Job batch must be one JSON array containing 1-256 valid jobs")
	}
	projectID := ctx.Param("project_id")
	if _, err := h.store.EnqueueProjectJobs(ctx.Request().Context(), projectID, jobs, h.clock()); err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Job batch was rejected")
	}
	return ctx.Redirect(http.StatusSeeOther, "/admin/projects/"+url.PathEscape(projectID))
}

func (h *Handler) enqueueSource(ctx *echo.Context) error {
	_, _, ok := h.authorizePostLimit(ctx, maxSourceFormBytes)
	if !ok {
		return nil
	}
	file, _, err := ctx.Request().FormFile("source_file")
	if err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Source file is required")
	}
	defer file.Close()
	projectID := ctx.Param("project_id")
	var storeErr error
	_, err = sourceformat.Decode(ctx.Request().Context(), io.LimitReader(file, maxSourceBytes+1), sourceformat.Limits{MaxUncompressedBytes: maxExpandedBytes, MaxJobs: maxSourceJobs}, func(batch []queue.JobSpec) error {
		jobs := make([]protocol.JobSpecV1, 0, len(batch))
		for _, job := range batch {
			jobs = append(jobs, protocol.JobSpecV1{ID: job.ID, Value: job.Value, Type: job.Type, Via: job.Via, Hops: job.Hops, Attrs: job.Attrs})
		}
		_, storeErr = h.store.EnqueueProjectJobs(ctx.Request().Context(), projectID, jobs, h.clock())
		return storeErr
	})
	if storeErr != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Source import was rejected")
	}
	if err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Source file is invalid")
	}
	return ctx.Redirect(http.StatusSeeOther, "/admin/projects/"+url.PathEscape(projectID))
}

func (h *Handler) job(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	user, sessionToken, ok := h.requireAdmin(ctx)
	if !ok {
		return nil
	}
	jobID, err := strconv.ParseInt(ctx.Param("job_id"), 10, 64)
	if err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Invalid job ID")
	}
	job, err := h.store.ProjectJob(ctx.Request().Context(), ctx.Param("project_id"), jobID)
	if err != nil {
		return h.pageError(ctx, http.StatusNotFound, "Job not found")
	}
	return render(ctx, http.StatusOK, "job", map[string]any{
		"User": user, "ProjectID": ctx.Param("project_id"), "Job": job, "CSRF": h.csrfToken(sessionToken),
		"AttrsJSON": displayJSON(job.Attrs), "OutcomeJSON": displayJSON(job.Outcome),
		"ErrorJSON": displayJSON(job.ExecutionError), "ReceiptsJSON": displayJSON(job.WARCReceipts),
	})
}

func displayJSON(value any) string {
	if value == nil {
		return ""
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return ""
	}
	return string(encoded)
}

func (h *Handler) requeueJob(ctx *echo.Context) error {
	_, _, ok := h.authorizePost(ctx)
	if !ok {
		return nil
	}
	jobID, err := strconv.ParseInt(ctx.Param("job_id"), 10, 64)
	if err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Invalid job ID")
	}
	if err := h.store.RequeueProjectJob(ctx.Request().Context(), ctx.Param("project_id"), jobID, h.clock()); err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Job requeue was rejected")
	}
	return ctx.Redirect(http.StatusSeeOther, "/admin/projects/"+url.PathEscape(ctx.Param("project_id"))+"/jobs/"+strconv.FormatInt(jobID, 10))
}

func (h *Handler) deleteJob(ctx *echo.Context) error {
	_, _, ok := h.authorizePost(ctx)
	if !ok {
		return nil
	}
	jobID, err := strconv.ParseInt(ctx.Param("job_id"), 10, 64)
	if err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Invalid job ID")
	}
	if err := h.store.DeleteProjectJob(ctx.Request().Context(), ctx.Param("project_id"), jobID); err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Job deletion was rejected")
	}
	return ctx.Redirect(http.StatusSeeOther, "/admin/projects/"+url.PathEscape(ctx.Param("project_id")))
}

func (h *Handler) logout(ctx *echo.Context) error {
	_, sessionToken, ok := h.authorizePost(ctx)
	if !ok {
		return nil
	}
	if err := h.store.DeleteWebSession(ctx.Request().Context(), sessionHash(sessionToken)); err != nil {
		return h.internal(ctx, err)
	}
	h.clearCookie(ctx, sessionCookieName, "/")
	return ctx.Redirect(http.StatusSeeOther, "/")
}

func (h *Handler) requireAdmin(ctx *echo.Context) (tracker.User, string, bool) {
	user, sessionToken, err := h.currentUser(ctx)
	if err != nil {
		if errors.Is(err, tracker.ErrWebSessionNotFound) {
			_ = ctx.Redirect(http.StatusSeeOther, "/")
		} else {
			_ = h.internal(ctx, err)
		}
		return tracker.User{}, "", false
	}
	if user.Status != tracker.UserStatusActive || !user.HasRole(tracker.RoleAdmin) {
		_ = h.pageError(ctx, http.StatusForbidden, "Administrator role required")
		return tracker.User{}, "", false
	}
	return user, sessionToken, true
}

func (h *Handler) currentUser(ctx *echo.Context) (tracker.User, string, error) {
	cookie, err := ctx.Cookie(sessionCookieName)
	if err != nil || len(cookie.Value) != 43 {
		return tracker.User{}, "", tracker.ErrWebSessionNotFound
	}
	user, err := h.store.AuthenticateWebSession(ctx.Request().Context(), sessionHash(cookie.Value), h.clock())
	return user, cookie.Value, err
}

func (h *Handler) authorizePost(ctx *echo.Context) (tracker.User, string, bool) {
	return h.authorizePostLimit(ctx, maxFormBytes)
}

func (h *Handler) authorizePostLimit(ctx *echo.Context, limit int64) (tracker.User, string, bool) {
	h.webHeaders(ctx.Response().Header())
	ctx.Request().Body = http.MaxBytesReader(ctx.Response(), ctx.Request().Body, limit)
	if err := ctx.Request().ParseForm(); err != nil {
		_ = h.pageError(ctx, http.StatusBadRequest, "Invalid form submission")
		return tracker.User{}, "", false
	}
	user, sessionToken, ok := h.requireAdmin(ctx)
	if !ok {
		return tracker.User{}, "", false
	}
	if !hmac.Equal([]byte(h.csrfToken(sessionToken)), []byte(ctx.FormValue("csrf"))) {
		_ = h.pageError(ctx, http.StatusForbidden, "Invalid CSRF token")
		return tracker.User{}, "", false
	}
	return user, sessionToken, true
}

func (h *Handler) csrfToken(sessionToken string) string {
	mac := hmac.New(sha256.New, h.secret)
	_, _ = mac.Write([]byte("csrf\x00" + sessionToken))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func sessionHash(value string) []byte {
	sum := sha256.Sum256([]byte("saveweb-web-session-v1\x00" + value))
	return sum[:]
}

func randomValue() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func (h *Handler) signOAuthTransaction(state, verifier string, expiresAt int64) string {
	payload := state + "." + verifier + "." + strconv.FormatInt(expiresAt, 10)
	mac := hmac.New(sha256.New, h.secret)
	_, _ = mac.Write([]byte("oauth\x00" + payload))
	return base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (h *Handler) verifyOAuthTransaction(value string) (string, string, error) {
	encoded, signature, found := strings.Cut(value, ".")
	payload, decodeErr := base64.RawURLEncoding.DecodeString(encoded)
	provided, signatureErr := base64.RawURLEncoding.DecodeString(signature)
	if !found || decodeErr != nil || signatureErr != nil {
		return "", "", errors.New("invalid OAuth transaction")
	}
	mac := hmac.New(sha256.New, h.secret)
	_, _ = mac.Write([]byte("oauth\x00" + string(payload)))
	parts := strings.Split(string(payload), ".")
	if !hmac.Equal(provided, mac.Sum(nil)) || len(parts) != 3 {
		return "", "", errors.New("invalid OAuth transaction")
	}
	expiresAt, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || expiresAt < h.clock() || len(parts[0]) != 43 || len(parts[1]) != 43 {
		return "", "", errors.New("expired OAuth transaction")
	}
	return parts[0], parts[1], nil
}

func (h *Handler) clearCookie(ctx *echo.Context, name, path string) {
	ctx.SetCookie(&http.Cookie{Name: name, Value: "", Path: path, MaxAge: -1, HttpOnly: true, Secure: h.secureCookies, SameSite: http.SameSiteLaxMode})
}

func (h *Handler) webHeaders(header http.Header) {
	header.Set("Cache-Control", "no-store")
	header.Set("Content-Security-Policy", "default-src 'none'; style-src 'self'; img-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Content-Type-Options", "nosniff")
}

func (h *Handler) pageError(ctx *echo.Context, status int, message string) error {
	h.webHeaders(ctx.Response().Header())
	return render(ctx, status, "error", map[string]string{"Message": message})
}

func (h *Handler) internal(ctx *echo.Context, err error) error {
	h.logger.Error("web admin request failed", "error", err)
	return h.pageError(ctx, http.StatusInternalServerError, "Internal server error")
}
