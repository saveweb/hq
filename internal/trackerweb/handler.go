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

	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const (
	sessionCookieName = "saveweb_hq_session"
	oauthCookieName   = "saveweb_hq_oauth"
	oauthTTL          = 10 * time.Minute
	defaultSessionTTL = 12 * time.Hour
	maxFormBytes      = int64(1 << 20)
)

type Store interface {
	UpsertGitHubAdmin(context.Context, tracker.GitHubIdentity, int64) (tracker.User, error)
	CreateWebSession(context.Context, string, []byte, int64, int64) error
	AuthenticateWebSession(context.Context, []byte, int64) (tracker.User, error)
	DeleteWebSession(context.Context, []byte) error
	ListProjectSummaries(context.Context) ([]protocol.AdminProjectSummary, error)
	ProjectSummary(context.Context, string) (protocol.AdminProjectSummary, error)
	PutProject(context.Context, tracker.Project, int64) error
	EnqueueProjectJobs(context.Context, string, []protocol.JobSpecV1, int64) (int64, error)
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
	server.POST("/admin/projects", h.createProject)
	server.GET("/admin/projects/:project_id", h.project)
	server.POST("/admin/projects/:project_id/status", h.updateProjectStatus)
	server.POST("/admin/projects/:project_id/jobs", h.enqueueJobs)
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
	if !member {
		return h.pageError(ctx, http.StatusForbidden, "GitHub account is not authorized for HQ administration")
	}
	now := h.clock()
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
	return render(ctx, http.StatusOK, "project", map[string]any{
		"User": user, "Project": project, "CSRF": h.csrfToken(sessionToken),
	})
}

func (h *Handler) createProject(ctx *echo.Context) error {
	_, _, ok := h.authorizePost(ctx)
	if !ok {
		return nil
	}
	project := tracker.Project{ID: ctx.FormValue("project_id"), Status: ctx.FormValue("status")}
	if err := h.store.PutProject(ctx.Request().Context(), project, h.clock()); err != nil {
		return h.pageError(ctx, http.StatusBadRequest, "Project update was rejected")
	}
	return ctx.Redirect(http.StatusSeeOther, "/admin/projects/"+url.PathEscape(project.ID))
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
	h.webHeaders(ctx.Response().Header())
	ctx.Request().Body = http.MaxBytesReader(ctx.Response(), ctx.Request().Body, maxFormBytes)
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
