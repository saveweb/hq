package localadmin

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"html/template"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"

	"git.saveweb.org/saveweb/hq/internal/httpapi"
	"git.saveweb.org/saveweb/hq/internal/shard"
)

const (
	localSessionCookie = "saveweb_local_admin"
	localSessionTTL    = 30 * time.Minute
	localFormLimit     = int64(8 << 10)
)

type Provider interface {
	RuntimeStatus(context.Context) (shard.RuntimeStatus, error)
	SetClaimsPaused(bool)
}

type Server struct {
	provider Provider
	token    string
	origin   string
	clock    func() int64
	mu       sync.Mutex
	sessions map[[32]byte]int64
}

func NewServer(provider Provider, token, origin string, clock func() int64) (*echo.Echo, error) {
	if provider == nil || validateToken(token) != nil || !strings.HasPrefix(origin, "http://127.0.0.1:") {
		return nil, fmt.Errorf("local admin: invalid server configuration")
	}
	if clock == nil {
		clock = func() int64 { return time.Now().Unix() }
	}
	handler := &Server{
		provider: provider, token: token, origin: origin, clock: clock,
		sessions: make(map[[32]byte]int64),
	}
	server := echo.New()
	server.Use(middleware.Recover())
	server.Use(middleware.BodyLimit(localFormLimit))
	server.Use(handler.securityHeaders)
	server.GET("/", handler.index)
	server.POST("/login", handler.login)
	server.GET("/admin", handler.admin)
	server.POST("/admin/claims/pause", handler.pauseClaims)
	server.POST("/admin/claims/resume", handler.resumeClaims)
	server.POST("/logout", handler.logout)
	server.GET("/api/v1/status", handler.apiStatus)
	return server, nil
}

func (s *Server) securityHeaders(next echo.HandlerFunc) echo.HandlerFunc {
	return func(ctx *echo.Context) error {
		header := ctx.Response().Header()
		header.Set("Cache-Control", "no-store")
		header.Set("Pragma", "no-cache")
		header.Set("Content-Security-Policy", "default-src 'none'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")
		header.Set("Referrer-Policy", "no-referrer")
		header.Set("X-Content-Type-Options", "nosniff")
		return next(ctx)
	}
}

func (s *Server) index(ctx *echo.Context) error {
	if _, ok := s.session(ctx); ok {
		return ctx.Redirect(http.StatusSeeOther, "/admin")
	}
	return renderLocal(ctx, http.StatusOK, localLoginTemplate, nil)
}

func (s *Server) login(ctx *echo.Context) error {
	if !s.validOrigin(ctx) {
		return renderLocal(ctx, http.StatusForbidden, localErrorTemplate, "Invalid request origin")
	}
	ctx.Request().Body = http.MaxBytesReader(ctx.Response(), ctx.Request().Body, localFormLimit)
	if err := ctx.Request().ParseForm(); err != nil || !constantTokenEqual(ctx.Request().PostForm.Get("token"), s.token) {
		return renderLocal(ctx, http.StatusUnauthorized, localErrorTemplate, "Invalid local admin token")
	}
	value, err := randomSession()
	if err != nil {
		return err
	}
	expiresAt := s.clock() + int64(localSessionTTL/time.Second)
	hash := localSessionHash(value)
	s.mu.Lock()
	for key, expiry := range s.sessions {
		if expiry <= s.clock() {
			delete(s.sessions, key)
		}
	}
	s.sessions[hash] = expiresAt
	s.mu.Unlock()
	ctx.SetCookie(&http.Cookie{
		Name: localSessionCookie, Value: value, Path: "/", MaxAge: int(localSessionTTL / time.Second),
		HttpOnly: true, SameSite: http.SameSiteStrictMode,
	})
	return ctx.Redirect(http.StatusSeeOther, "/admin")
}

func (s *Server) admin(ctx *echo.Context) error {
	session, ok := s.session(ctx)
	if !ok {
		return ctx.Redirect(http.StatusSeeOther, "/")
	}
	status, err := s.provider.RuntimeStatus(ctx.Request().Context())
	if err != nil {
		return err
	}
	return renderLocal(ctx, http.StatusOK, localAdminTemplate, map[string]any{
		"Status": status, "CSRF": s.csrf(session),
	})
}

func (s *Server) pauseClaims(ctx *echo.Context) error {
	if _, ok := s.authorizePost(ctx); !ok {
		return nil
	}
	s.provider.SetClaimsPaused(true)
	return ctx.Redirect(http.StatusSeeOther, "/admin")
}

func (s *Server) resumeClaims(ctx *echo.Context) error {
	if _, ok := s.authorizePost(ctx); !ok {
		return nil
	}
	s.provider.SetClaimsPaused(false)
	return ctx.Redirect(http.StatusSeeOther, "/admin")
}

func (s *Server) logout(ctx *echo.Context) error {
	session, ok := s.authorizePost(ctx)
	if !ok {
		return nil
	}
	hash := localSessionHash(session)
	s.mu.Lock()
	delete(s.sessions, hash)
	s.mu.Unlock()
	ctx.SetCookie(&http.Cookie{
		Name: localSessionCookie, Value: "", Path: "/", MaxAge: -1,
		Expires: time.Unix(1, 0), HttpOnly: true, SameSite: http.SameSiteStrictMode,
	})
	return ctx.Redirect(http.StatusSeeOther, "/")
}

func (s *Server) apiStatus(ctx *echo.Context) error {
	authorized := false
	if token, ok := httpapi.BearerToken(ctx.Request().Header.Get("Authorization")); ok {
		authorized = constantTokenEqual(token, s.token)
	}
	if !authorized {
		_, authorized = s.session(ctx)
	}
	if !authorized {
		return ctx.JSON(http.StatusUnauthorized, map[string]string{"error": "local admin authentication required"})
	}
	status, err := s.provider.RuntimeStatus(ctx.Request().Context())
	if err != nil {
		return err
	}
	return ctx.JSON(http.StatusOK, status)
}

func (s *Server) authorizePost(ctx *echo.Context) (string, bool) {
	if !s.validOrigin(ctx) {
		_ = renderLocal(ctx, http.StatusForbidden, localErrorTemplate, "Invalid request origin")
		return "", false
	}
	session, ok := s.session(ctx)
	if !ok {
		_ = ctx.Redirect(http.StatusSeeOther, "/")
		return "", false
	}
	ctx.Request().Body = http.MaxBytesReader(ctx.Response(), ctx.Request().Body, localFormLimit)
	if err := ctx.Request().ParseForm(); err != nil ||
		!hmac.Equal([]byte(ctx.Request().PostForm.Get("csrf")), []byte(s.csrf(session))) {
		_ = renderLocal(ctx, http.StatusForbidden, localErrorTemplate, "CSRF validation failed")
		return "", false
	}
	return session, true
}

func (s *Server) validOrigin(ctx *echo.Context) bool {
	value := ctx.Request().Header.Get("Origin")
	if value == s.origin {
		return true
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "http" || parsed.Host != ctx.Request().Host ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	host := parsed.Hostname()
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (s *Server) session(ctx *echo.Context) (string, bool) {
	cookie, err := ctx.Cookie(localSessionCookie)
	if err != nil || len(cookie.Value) != 43 {
		return "", false
	}
	hash := localSessionHash(cookie.Value)
	s.mu.Lock()
	expiresAt, ok := s.sessions[hash]
	if ok && expiresAt <= s.clock() {
		delete(s.sessions, hash)
		ok = false
	}
	s.mu.Unlock()
	return cookie.Value, ok
}

func (s *Server) csrf(session string) string {
	mac := hmac.New(sha256.New, []byte(s.token))
	_, _ = mac.Write([]byte("saveweb-local-csrf-v1\x00" + session))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func constantTokenEqual(left, right string) bool {
	leftHash := sha256.Sum256([]byte("saveweb-local-token-v1\x00" + left))
	rightHash := sha256.Sum256([]byte("saveweb-local-token-v1\x00" + right))
	return hmac.Equal(leftHash[:], rightHash[:])
}

func localSessionHash(value string) [32]byte {
	return sha256.Sum256([]byte("saveweb-local-session-v1\x00" + value))
}

func randomSession() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value[:]), nil
}

func renderLocal(ctx *echo.Context, status int, source string, data any) error {
	page, err := template.New("local").Parse(source)
	if err != nil {
		return err
	}
	ctx.Response().Header().Set("Content-Type", "text/html; charset=utf-8")
	ctx.Response().WriteHeader(status)
	return page.Execute(ctx.Response(), data)
}

const localLoginTemplate = `<!doctype html><html lang="en"><head><meta charset="utf-8"><title>SavewebHQ shard</title></head>
<body><main><h1>Shard local administration</h1><form method="post" action="/login">
<label>Local admin token <input type="password" name="token" autocomplete="current-password" required></label>
<button type="submit">Sign in</button></form></main></body></html>`

const localAdminTemplate = `<!doctype html><html lang="en"><head><meta charset="utf-8"><title>SavewebHQ shard status</title></head>
<body><main><h1>Shard local administration</h1><p>Agent: <code>{{.Status.AgentID}}</code> · UNIX time: {{.Status.ServerTime}}</p>
<p>New claims paused: {{.Status.ClaimsPaused}}</p>
{{if .Status.ClaimsPaused}}<form method="post" action="/admin/claims/resume"><input type="hidden" name="csrf" value="{{.CSRF}}"><button>Resume claims</button></form>
{{else}}<form method="post" action="/admin/claims/pause"><input type="hidden" name="csrf" value="{{.CSRF}}"><button>Pause new claims</button></form>{{end}}
<h2>Owned shards</h2><table><thead><tr><th>Project</th><th>Shard</th><th>Generation</th><th>Status</th><th>Owner lease</th><th>todo</th><th>wip</th><th>done</th><th>failed</th><th>reset exhausted</th></tr></thead><tbody>
{{range .Status.Shards}}<tr><td>{{.ProjectID}}</td><td>{{.ShardID}}</td><td>{{.Generation}}</td><td>{{.Status}}</td><td>{{.OwnerLeaseExpiresAt}}</td><td>{{.Stats.Todo}}</td><td>{{.Stats.WIP}}</td><td>{{.Stats.Done}}</td><td>{{.Stats.Failed}}</td><td>{{.Stats.ResetExhausted}}</td></tr>
{{else}}<tr><td colspan="10">No shard assignments.</td></tr>{{end}}</tbody></table>
<form method="post" action="/logout"><input type="hidden" name="csrf" value="{{.CSRF}}"><button>Sign out</button></form>
</main></body></html>`

const localErrorTemplate = `<!doctype html><html lang="en"><head><meta charset="utf-8"><title>SavewebHQ shard error</title></head>
<body><main><h1>Request failed</h1><p>{{.}}</p><p><a href="/">Return</a></p></main></body></html>`
