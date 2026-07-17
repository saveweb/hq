// Package trackerweb implements the tracker contributor portal and user admin
// pages. It intentionally uses server-rendered HTML and no browser-side token
// storage.
package trackerweb

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"git.saveweb.org/saveweb/hq/internal/tracker"
)

const (
	sessionCookieName = "saveweb_hq_session"
	oauthCookieName   = "saveweb_hq_oauth"
	defaultSessionTTL = 12 * time.Hour
	oauthTTL          = 10 * time.Minute
	maxFormBytes      = int64(32 << 10)
)

type Store interface {
	UpsertGitHubUser(context.Context, tracker.GitHubIdentity, bool, int64) (tracker.User, error)
	CreateWebSession(context.Context, string, []byte, int64, int64) error
	AuthenticateWebSession(context.Context, []byte, int64) (tracker.User, error)
	DeleteWebSession(context.Context, []byte) error
	MachineToken(context.Context, string) (string, error)
	ResetMachineToken(context.Context, string, int64) (string, error)
	ListUserAgents(context.Context, string) ([]tracker.Agent, error)
	ListUsers(context.Context) ([]tracker.User, error)
	UpdateUserAccess(context.Context, string, string, string, map[string]bool, string, int64) error
	ListProjects(context.Context) ([]tracker.Project, error)
	ListAdminShards(context.Context) ([]tracker.AdminShard, error)
	ListReceivers(context.Context) ([]tracker.Receiver, error)
	ListShardAgents(context.Context) ([]tracker.Agent, error)
	ListAuditEvents(context.Context, int) ([]tracker.AuditEvent, error)
	AdminPutProject(context.Context, string, tracker.Project, string, int64) error
	AdminPutShard(context.Context, string, tracker.Shard, string, int64) error
	AdminTransitionShard(context.Context, string, string, string, int64, string, string, int64) error
	AdminPutReceiver(context.Context, string, tracker.Receiver, string, int64) error
}

type OAuth interface {
	AuthorizationURL(state, codeChallenge string) (string, error)
	Exchange(ctx context.Context, code, codeVerifier string) (string, error)
	User(ctx context.Context, accessToken string) (tracker.GitHubIdentity, error)
	TeamMembership(ctx context.Context, accessToken, organization, team, username string) (bool, error)
}

type Config struct {
	PublicURL         string
	Secret            []byte
	SecureCookies     bool
	AdminOrganization string
	AdminTeam         string
	SessionTTL        time.Duration
	Clock             func() int64
}

type Handler struct {
	store             Store
	oauth             OAuth
	logger            *slog.Logger
	secret            []byte
	secureCookies     bool
	adminOrganization string
	adminTeam         string
	sessionTTL        time.Duration
	clock             func() int64
}

func New(store Store, oauth OAuth, config Config, logger *slog.Logger) (*Handler, error) {
	parsed, err := url.Parse(config.PublicURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || (parsed.Path != "" && parsed.Path != "/") ||
		parsed.RawQuery != "" || parsed.Fragment != "" || len(config.Secret) < 32 {
		return nil, fmt.Errorf("tracker web: invalid public URL or session secret")
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
	if oauth != nil && (config.AdminOrganization == "" || config.AdminTeam == "" ||
		strings.TrimSpace(config.AdminOrganization) != config.AdminOrganization ||
		strings.TrimSpace(config.AdminTeam) != config.AdminTeam ||
		len(config.AdminOrganization) > 255 || len(config.AdminTeam) > 255) {
		return nil, fmt.Errorf("tracker web: OAuth admin organization and team are required")
	}
	return &Handler{
		store: store, oauth: oauth, logger: logger, secret: append([]byte(nil), config.Secret...),
		secureCookies:     config.SecureCookies,
		adminOrganization: config.AdminOrganization, adminTeam: config.AdminTeam,
		sessionTTL: config.SessionTTL, clock: config.Clock,
	}, nil
}

func (h *Handler) Register(server *echo.Echo) {
	server.GET("/", h.landing)
	server.GET("/auth/github/start", h.oauthStart)
	server.GET("/auth/github/callback", h.oauthCallback)
	server.GET("/portal", h.portal)
	server.POST("/portal/machine-token/reset", h.resetMachineToken)
	server.POST("/logout", h.logout)
	server.GET("/admin/users", h.adminUsers)
	server.POST("/admin/users/:user_id/access", h.updateUserAccess)
	server.GET("/admin/projects", h.adminProjects)
	server.POST("/admin/projects", h.putProject)
	server.POST("/admin/shards", h.putShard)
	server.POST("/admin/shards/transition", h.transitionShard)
	server.POST("/admin/receivers", h.putReceiver)
}

func (h *Handler) landing(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	if _, _, err := h.currentUser(ctx); err == nil {
		return ctx.Redirect(http.StatusSeeOther, "/portal")
	}
	return render(ctx, http.StatusOK, landingTemplate, map[string]any{"OAuthEnabled": h.oauth != nil})
}

func (h *Handler) oauthStart(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	if h.oauth == nil {
		return h.pageError(ctx, http.StatusServiceUnavailable, "GitHub login is not configured")
	}
	state, err := randomValue()
	if err != nil {
		return h.internal(ctx, err)
	}
	verifier, err := randomValue()
	if err != nil {
		return h.internal(ctx, err)
	}
	digest := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(digest[:])
	authorizeURL, err := h.oauth.AuthorizationURL(state, challenge)
	if err != nil {
		return h.internal(ctx, err)
	}
	expiresAt := h.clock() + int64(oauthTTL/time.Second)
	value := h.signOAuthTransaction(state, verifier, expiresAt)
	ctx.SetCookie(&http.Cookie{
		Name: oauthCookieName, Value: value, Path: "/auth/github/callback",
		MaxAge: int(oauthTTL / time.Second), HttpOnly: true, Secure: h.secureCookies,
		SameSite: http.SameSiteLaxMode,
	})
	return ctx.Redirect(http.StatusFound, authorizeURL)
}

func (h *Handler) oauthCallback(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	if h.oauth == nil {
		return h.pageError(ctx, http.StatusServiceUnavailable, "GitHub login is not configured")
	}
	cookie, err := ctx.Cookie(oauthCookieName)
	h.clearCookie(ctx, oauthCookieName, "/auth/github/callback", http.SameSiteLaxMode)
	if err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Login transaction is missing or expired")
	}
	state, verifier, err := h.verifyOAuthTransaction(cookie.Value)
	if err != nil || !hmac.Equal([]byte(state), []byte(ctx.QueryParam("state"))) {
		return h.pageError(ctx, http.StatusBadRequest, "Login transaction is invalid")
	}
	code := ctx.QueryParam("code")
	if code == "" || ctx.QueryParam("error") != "" {
		return h.pageError(ctx, http.StatusBadRequest, "GitHub authorization was not completed")
	}
	requestContext, cancel := context.WithTimeout(ctx.Request().Context(), 20*time.Second)
	defer cancel()
	accessToken, err := h.oauth.Exchange(requestContext, code, verifier)
	if err != nil {
		h.logger.Warn("GitHub OAuth exchange failed", "error", err)
		return h.pageError(ctx, http.StatusBadGateway, "GitHub login failed")
	}
	identity, err := h.oauth.User(requestContext, accessToken)
	if err != nil {
		accessToken = ""
		h.logger.Warn("GitHub identity lookup failed", "error", err)
		return h.pageError(ctx, http.StatusBadGateway, "GitHub identity lookup failed")
	}
	isAdmin, err := h.oauth.TeamMembership(
		requestContext, accessToken, h.adminOrganization, h.adminTeam, identity.Login,
	)
	accessToken = ""
	if err != nil {
		h.logger.Warn("GitHub team membership lookup failed", "error", err)
		return h.pageError(ctx, http.StatusBadGateway, "GitHub team membership lookup failed")
	}
	now := h.clock()
	user, err := h.store.UpsertGitHubUser(requestContext, identity, isAdmin, now)
	if err != nil {
		return h.internal(ctx, err)
	}
	sessionToken, err := randomValue()
	if err != nil {
		return h.internal(ctx, err)
	}
	expiresAt := now + int64(h.sessionTTL/time.Second)
	if err := h.store.CreateWebSession(requestContext, user.ID, sessionHash(sessionToken), now, expiresAt); err != nil {
		return h.internal(ctx, err)
	}
	ctx.SetCookie(&http.Cookie{
		Name: sessionCookieName, Value: sessionToken, Path: "/", MaxAge: int(h.sessionTTL / time.Second),
		HttpOnly: true, Secure: h.secureCookies, SameSite: http.SameSiteLaxMode,
	})
	return ctx.Redirect(http.StatusSeeOther, "/portal")
}

func (h *Handler) portal(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	user, sessionToken, err := h.currentUser(ctx)
	if err != nil {
		return ctx.Redirect(http.StatusSeeOther, "/")
	}
	machineToken, err := h.store.MachineToken(ctx.Request().Context(), user.ID)
	if err != nil {
		return h.internal(ctx, err)
	}
	agents, err := h.store.ListUserAgents(ctx.Request().Context(), user.ID)
	if err != nil {
		return h.internal(ctx, err)
	}
	return render(ctx, http.StatusOK, portalTemplate, map[string]any{
		"User": user, "Roles": roleNames(user.Roles), "MachineToken": machineToken,
		"Agents": agents, "CSRF": h.csrfToken(sessionToken), "IsAdmin": user.HasRole(tracker.RoleAdmin),
	})
}

func (h *Handler) resetMachineToken(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	user, sessionToken, ok := h.authorizePost(ctx)
	if !ok {
		return nil
	}
	if _, err := h.store.ResetMachineToken(ctx.Request().Context(), user.ID, h.clock()); err != nil {
		return h.internal(ctx, err)
	}
	_ = sessionToken
	return ctx.Redirect(http.StatusSeeOther, "/portal")
}

func (h *Handler) logout(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	_, sessionToken, ok := h.authorizePost(ctx)
	if !ok {
		return nil
	}
	if err := h.store.DeleteWebSession(ctx.Request().Context(), sessionHash(sessionToken)); err != nil {
		return h.internal(ctx, err)
	}
	h.clearCookie(ctx, sessionCookieName, "/", http.SameSiteLaxMode)
	return ctx.Redirect(http.StatusSeeOther, "/")
}

func (h *Handler) adminUsers(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	user, sessionToken, err := h.currentUser(ctx)
	if err != nil {
		return ctx.Redirect(http.StatusSeeOther, "/")
	}
	if user.Status != tracker.UserStatusActive || !user.HasRole(tracker.RoleAdmin) {
		return h.pageError(ctx, http.StatusForbidden, "Administrator role required")
	}
	users, err := h.store.ListUsers(ctx.Request().Context())
	if err != nil {
		return h.internal(ctx, err)
	}
	return render(ctx, http.StatusOK, adminUsersTemplate, map[string]any{
		"Current": user, "Users": users, "CSRF": h.csrfToken(sessionToken),
	})
}

func (h *Handler) updateUserAccess(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	actor, ok := h.authorizeAdminPost(ctx)
	if !ok {
		return nil
	}
	roles := map[string]bool{
		tracker.RoleAdmin:      ctx.FormValue("role_admin") == "on",
		tracker.RoleShardOwner: ctx.FormValue("role_shard_owner") == "on",
		tracker.RoleWorker:     ctx.FormValue("role_worker") == "on",
	}
	if err := h.store.UpdateUserAccess(
		ctx.Request().Context(), actor.ID, ctx.Param("user_id"), ctx.FormValue("status"),
		roles, ctx.FormValue("reason"), h.clock(),
	); err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "User access update was rejected")
	}
	return ctx.Redirect(http.StatusSeeOther, "/admin/users")
}

func (h *Handler) adminProjects(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	user, sessionToken, err := h.currentUser(ctx)
	if err != nil {
		return ctx.Redirect(http.StatusSeeOther, "/")
	}
	if user.Status != tracker.UserStatusActive || !user.HasRole(tracker.RoleAdmin) {
		return h.pageError(ctx, http.StatusForbidden, "Administrator role required")
	}
	projects, err := h.store.ListProjects(ctx.Request().Context())
	if err != nil {
		return h.internal(ctx, err)
	}
	shards, err := h.store.ListAdminShards(ctx.Request().Context())
	if err != nil {
		return h.internal(ctx, err)
	}
	receivers, err := h.store.ListReceivers(ctx.Request().Context())
	if err != nil {
		return h.internal(ctx, err)
	}
	agents, err := h.store.ListShardAgents(ctx.Request().Context())
	if err != nil {
		return h.internal(ctx, err)
	}
	audit, err := h.store.ListAuditEvents(ctx.Request().Context(), 100)
	if err != nil {
		return h.internal(ctx, err)
	}
	return render(ctx, http.StatusOK, adminProjectsTemplate, map[string]any{
		"Projects": projects, "Shards": shards, "Receivers": receivers,
		"ShardAgents": agents, "Audit": audit, "CSRF": h.csrfToken(sessionToken),
	})
}

func (h *Handler) putProject(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	actor, ok := h.authorizeAdminPost(ctx)
	if !ok {
		return nil
	}
	project := tracker.Project{ID: ctx.FormValue("project_id"), Status: ctx.FormValue("status")}
	if err := h.store.AdminPutProject(
		ctx.Request().Context(), actor.ID, project, ctx.FormValue("reason"), h.clock(),
	); err != nil {
		h.logger.Warn("project admin command rejected", "actor", actor.ID, "project", project.ID, "error", err)
		return h.pageError(ctx, http.StatusBadRequest, "Project update was rejected")
	}
	return ctx.Redirect(http.StatusSeeOther, "/admin/projects")
}

func (h *Handler) putShard(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	actor, ok := h.authorizeAdminPost(ctx)
	if !ok {
		return nil
	}
	generation, err := strconv.ParseInt(ctx.FormValue("generation"), 10, 64)
	if err != nil || generation < 1 {
		return h.pageError(ctx, http.StatusBadRequest, "Shard generation must be a positive integer")
	}
	status := ctx.FormValue("status")
	if status != tracker.ShardStatusLoading && status != tracker.ShardStatusRecovering {
		return h.pageError(ctx, http.StatusBadRequest, "Web shard commands only allow loading or recovering")
	}
	shard := tracker.Shard{
		ProjectID: ctx.FormValue("project_id"), ID: ctx.FormValue("shard_id"),
		OwnerAgentID: ctx.FormValue("owner_agent_id"), Status: status, Generation: generation,
	}
	sourceURI, sourceETag := ctx.FormValue("source_uri"), ctx.FormValue("source_etag")
	if status == tracker.ShardStatusLoading {
		sourceFormat := ctx.FormValue("source_format")
		if sourceURI == "" || sourceETag == "" || sourceFormat != "jobs-jsonl-zstd-v1" {
			return h.pageError(ctx, http.StatusBadRequest, "Loading requires an immutable source URI, format, and ETag")
		}
		shard.SourceURI, shard.SourceFormat, shard.SourceETag = &sourceURI, &sourceFormat, &sourceETag
	} else if sourceURI != "" || sourceETag != "" || ctx.FormValue("source_format") != "" {
		return h.pageError(ctx, http.StatusBadRequest, "Recovery uses the published checkpoint and cannot include source fields")
	}
	if err := h.store.AdminPutShard(
		ctx.Request().Context(), actor.ID, shard, ctx.FormValue("reason"), h.clock(),
	); err != nil {
		h.logger.Warn("shard admin command rejected", "actor", actor.ID, "project", shard.ProjectID,
			"shard", shard.ID, "generation", shard.Generation, "error", err)
		return h.pageError(ctx, http.StatusBadRequest, "Shard attach or recovery was rejected")
	}
	return ctx.Redirect(http.StatusSeeOther, "/admin/projects")
}

func (h *Handler) transitionShard(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	actor, ok := h.authorizeAdminPost(ctx)
	if !ok {
		return nil
	}
	generation, err := strconv.ParseInt(ctx.FormValue("expected_generation"), 10, 64)
	if err != nil || generation < 1 {
		return h.pageError(ctx, http.StatusBadRequest, "Expected shard generation must be a positive integer")
	}
	projectID, shardID, targetStatus := ctx.FormValue("project_id"), ctx.FormValue("shard_id"), ctx.FormValue("target_status")
	if err := h.store.AdminTransitionShard(
		ctx.Request().Context(), actor.ID, projectID, shardID, generation,
		targetStatus, ctx.FormValue("reason"), h.clock(),
	); err != nil {
		h.logger.Warn("shard transition rejected", "actor", actor.ID, "project", projectID,
			"shard", shardID, "generation", generation, "target_status", targetStatus, "error", err)
		return h.pageError(ctx, http.StatusBadRequest, "Shard lifecycle transition was rejected")
	}
	return ctx.Redirect(http.StatusSeeOther, "/admin/projects")
}

func (h *Handler) putReceiver(ctx *echo.Context) error {
	h.webHeaders(ctx.Response().Header())
	actor, ok := h.authorizeAdminPost(ctx)
	if !ok {
		return nil
	}
	receiver := tracker.Receiver{
		ProjectID: ctx.FormValue("project_id"), ID: ctx.FormValue("receiver_id"),
		Status: ctx.FormValue("status"), SinkURI: strings.TrimSuffix(ctx.FormValue("sink_uri"), "/"),
		Format: "jobs-jsonl-zstd-v1",
	}
	if err := h.store.AdminPutReceiver(
		ctx.Request().Context(), actor.ID, receiver, ctx.FormValue("reason"), h.clock(),
	); err != nil {
		h.logger.Warn("receiver admin command rejected", "actor", actor.ID, "project", receiver.ProjectID,
			"receiver", receiver.ID, "error", err)
		return h.pageError(ctx, http.StatusBadRequest, "Job Receiver update was rejected")
	}
	return ctx.Redirect(http.StatusSeeOther, "/admin/projects")
}

func (h *Handler) currentUser(ctx *echo.Context) (tracker.User, string, error) {
	cookie, err := ctx.Cookie(sessionCookieName)
	if err != nil || len(cookie.Value) != 43 {
		return tracker.User{}, "", fmt.Errorf("web session not found")
	}
	user, err := h.store.AuthenticateWebSession(ctx.Request().Context(), sessionHash(cookie.Value), h.clock())
	if err != nil {
		return tracker.User{}, "", err
	}
	return user, cookie.Value, nil
}

func (h *Handler) authorizePost(ctx *echo.Context) (tracker.User, string, bool) {
	if ctx.Request().ContentLength > maxFormBytes {
		_ = h.pageError(ctx, http.StatusRequestEntityTooLarge, "Form is too large")
		return tracker.User{}, "", false
	}
	user, sessionToken, err := h.currentUser(ctx)
	if err != nil {
		_ = ctx.Redirect(http.StatusSeeOther, "/")
		return tracker.User{}, "", false
	}
	ctx.Request().Body = http.MaxBytesReader(ctx.Response(), ctx.Request().Body, maxFormBytes)
	if err := ctx.Request().ParseForm(); err != nil ||
		!hmac.Equal([]byte(ctx.Request().PostForm.Get("csrf")), []byte(h.csrfToken(sessionToken))) {
		_ = h.pageError(ctx, http.StatusForbidden, "CSRF validation failed")
		return tracker.User{}, "", false
	}
	return user, sessionToken, true
}

func (h *Handler) authorizeAdminPost(ctx *echo.Context) (tracker.User, bool) {
	user, _, ok := h.authorizePost(ctx)
	if !ok {
		return tracker.User{}, false
	}
	if user.Status != tracker.UserStatusActive || !user.HasRole(tracker.RoleAdmin) {
		_ = h.pageError(ctx, http.StatusForbidden, "Administrator role required")
		return tracker.User{}, false
	}
	return user, true
}

func (h *Handler) csrfToken(sessionToken string) string {
	mac := hmac.New(sha256.New, h.secret)
	_, _ = mac.Write([]byte("saveweb-csrf-v1\x00" + sessionToken))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (h *Handler) signOAuthTransaction(state, verifier string, expiresAt int64) string {
	payload := state + "." + verifier + "." + strconv.FormatInt(expiresAt, 10)
	mac := hmac.New(sha256.New, h.secret)
	_, _ = mac.Write([]byte("saveweb-oauth-v1\x00" + payload))
	return payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (h *Handler) verifyOAuthTransaction(value string) (string, string, error) {
	parts := strings.Split(value, ".")
	if len(parts) != 4 || len(parts[0]) != 43 || len(parts[1]) != 43 || len(parts[3]) != 43 {
		return "", "", fmt.Errorf("invalid OAuth transaction")
	}
	expiresAt, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || h.clock() >= expiresAt {
		return "", "", fmt.Errorf("expired OAuth transaction")
	}
	payload := strings.Join(parts[:3], ".")
	mac := hmac.New(sha256.New, h.secret)
	_, _ = mac.Write([]byte("saveweb-oauth-v1\x00" + payload))
	expected := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(parts[3])) {
		return "", "", fmt.Errorf("invalid OAuth transaction signature")
	}
	return parts[0], parts[1], nil
}

func (h *Handler) webHeaders(header http.Header) {
	header.Set("Cache-Control", "no-store")
	header.Set("Pragma", "no-cache")
	header.Set("Content-Security-Policy", "default-src 'none'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Content-Type-Options", "nosniff")
}

func (h *Handler) clearCookie(ctx *echo.Context, name, path string, sameSite http.SameSite) {
	ctx.SetCookie(&http.Cookie{
		Name: name, Value: "", Path: path, MaxAge: -1, Expires: time.Unix(1, 0),
		HttpOnly: true, Secure: h.secureCookies, SameSite: sameSite,
	})
}

func (h *Handler) internal(ctx *echo.Context, err error) error {
	h.logger.Error("tracker web request failed", "error", err)
	return h.pageError(ctx, http.StatusInternalServerError, "Internal server error")
}

func (h *Handler) pageError(ctx *echo.Context, status int, message string) error {
	return render(ctx, status, errorTemplate, map[string]any{"Message": message})
}

func randomValue() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value[:]), nil
}

func sessionHash(value string) []byte {
	digest := sha256.Sum256([]byte("saveweb-web-session-v1\x00" + value))
	return digest[:]
}

func roleNames(roles map[string]bool) []string {
	result := []string{}
	for _, role := range []string{tracker.RoleAdmin, tracker.RoleShardOwner, tracker.RoleWorker} {
		if roles[role] {
			result = append(result, role)
		}
	}
	return result
}

func render(ctx *echo.Context, status int, source string, data any) error {
	value, err := template.New("page").Parse(source)
	if err != nil {
		return err
	}
	ctx.Response().Header().Set("Content-Type", "text/html; charset=utf-8")
	ctx.Response().WriteHeader(status)
	return value.Execute(ctx.Response(), data)
}
