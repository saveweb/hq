// Package shardhttp exposes the shard runtime as the v1 Echo Queue API.
package shardhttp

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"

	"git.saveweb.org/saveweb/hq/internal/httpapi"
	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/shard"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const (
	challengeBodyLimit = int64(4 << 10)
	queueBodyLimit     = int64(1 << 20)
	defaultConcurrency = 128
	defaultMaxResets   = 3
	contextAuthKey     = "saveweb.shard.authorization"
)

type Config struct {
	AgentID       string
	BasePath      string
	MaxConcurrent int
	MaxResets     int
}

type Handler struct {
	manager   *shard.Manager
	agentID   string
	basePath  string
	logger    *slog.Logger
	semaphore chan struct{}
	maxResets int
}

func DefaultConfig(agentID string) Config {
	return Config{AgentID: agentID, MaxConcurrent: defaultConcurrency, MaxResets: defaultMaxResets}
}

func New(manager *shard.Manager, config Config, logger *slog.Logger) (*echo.Echo, error) {
	if manager == nil || !queue.ValidateIdentifier(config.AgentID) {
		return nil, fmt.Errorf("shard HTTP: invalid dependencies")
	}
	basePath, err := normalizeBasePath(config.BasePath)
	if err != nil {
		return nil, err
	}
	if config.MaxConcurrent == 0 {
		config.MaxConcurrent = defaultConcurrency
	}
	if config.MaxConcurrent < 1 || config.MaxConcurrent > 65536 || config.MaxResets < 0 || config.MaxResets > 1000 {
		return nil, fmt.Errorf("shard HTTP: invalid limits")
	}
	if logger == nil {
		logger = slog.Default()
	}
	handler := &Handler{
		manager: manager, agentID: config.AgentID, basePath: basePath, logger: logger,
		semaphore: make(chan struct{}, config.MaxConcurrent), maxResets: config.MaxResets,
	}
	server := echo.New()
	server.Logger = logger
	server.HTTPErrorHandler = handler.echoError
	server.Pre(sanitizeRequestID)
	server.Use(middleware.RequestID())
	server.Use(middleware.Recover())
	server.Use(middleware.Secure())
	server.Use(noStore)

	server.GET(basePath+"/healthz", handler.health)
	server.POST(basePath+"/api/v1/shard/endpoint-challenge", handler.endpointChallenge)
	server.POST(basePath+"/api/v1/queue/claim", handler.claim, handler.queueBoundary)
	server.POST(basePath+"/api/v1/queue/complete", handler.complete, handler.queueBoundary)
	server.POST(basePath+"/api/v1/queue/fail", handler.fail, handler.queueBoundary)
	server.POST(basePath+"/api/v1/queue/extend-lease", handler.extendLease, handler.queueBoundary)
	return server, nil
}

func sanitizeRequestID(next echo.HandlerFunc) echo.HandlerFunc {
	return func(ctx *echo.Context) error {
		requestID := ctx.Request().Header.Get(echo.HeaderXRequestID)
		if len(requestID) > 128 || strings.ContainsAny(requestID, "\r\n") {
			ctx.Request().Header.Del(echo.HeaderXRequestID)
		}
		return next(ctx)
	}
}

func normalizeBasePath(value string) (string, error) {
	if value == "" || value == "/" {
		return "", nil
	}
	if !strings.HasPrefix(value, "/") || strings.HasSuffix(value, "/") || path.Clean(value) != value {
		return "", fmt.Errorf("shard HTTP: base path must be a clean absolute path without trailing slash")
	}
	return value, nil
}

func noStore(next echo.HandlerFunc) echo.HandlerFunc {
	return func(ctx *echo.Context) error {
		httpapi.SetNoStore(ctx.Response().Header())
		return next(ctx)
	}
}

func (h *Handler) queueBoundary(next echo.HandlerFunc) echo.HandlerFunc {
	return func(ctx *echo.Context) error {
		select {
		case h.semaphore <- struct{}{}:
			defer func() { <-h.semaphore }()
		default:
			ctx.Response().Header().Set("Retry-After", "1")
			h.writeQueueError(ctx, &queue.Error{
				Code: protocol.ErrorBackpressure, Message: "shard request concurrency limit reached", Retryable: true,
			})
			return nil
		}
		token, ok := httpapi.BearerToken(ctx.Request().Header.Get(echo.HeaderAuthorization))
		if !ok {
			h.writeQueueError(ctx, &queue.Error{Code: protocol.ErrorInvalidAccessToken, Message: "shard access token is required"})
			return nil
		}
		authorization, err := h.manager.Authorize(token)
		if err != nil {
			h.writeQueueError(ctx, err)
			return nil
		}
		ctx.Set(contextAuthKey, authorization)
		return next(ctx)
	}
}

func (h *Handler) health(ctx *echo.Context) error {
	return ctx.JSON(http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) endpointChallenge(ctx *echo.Context) error {
	var request protocol.EndpointChallenge
	if err := httpapi.DecodeJSON(ctx.Response(), ctx.Request(), challengeBodyLimit, &request); err != nil {
		h.writeQueueError(ctx, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: err.Error()})
		return nil
	}
	if request.AgentID != h.agentID || len(request.Challenge) < 32 || len(request.Challenge) > 256 {
		h.writeQueueError(ctx, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: "invalid endpoint challenge"})
		return nil
	}
	return ctx.JSON(http.StatusOK, request)
}

func (h *Handler) claim(ctx *echo.Context) error {
	authorization := mustAuthorization(ctx)
	var request protocol.ClaimRequest
	if err := httpapi.DecodeJSON(ctx.Response(), ctx.Request(), queueBodyLimit, &request); err != nil {
		h.writeQueueError(ctx, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: err.Error()})
		return nil
	}
	if err := authorization.CheckRoute(request.SessionRoute); err != nil {
		h.writeQueueError(ctx, err)
		return nil
	}
	if err := authorization.AllowsClaim(); err != nil {
		h.writeQueueError(ctx, err)
		return nil
	}
	jobs, err := authorization.Store.ClaimBatch(ctx.Request().Context(), request.Generation, h.manager.Now(),
		request.SessionID, request.AcceptTypes, request.MaxJobs, request.LeaseSeconds)
	if err != nil {
		h.writeQueueError(ctx, err)
		return nil
	}
	wireJobs := make([]protocol.ClaimedJob, len(jobs))
	for index, job := range jobs {
		wireJobs[index] = toProtocolJob(job)
	}
	retryAfter := int64(0)
	if len(wireJobs) == 0 {
		retryAfter = 250
	}
	return ctx.JSON(http.StatusOK, protocol.ClaimResponse{
		Route: request.Route, Jobs: wireJobs, RetryAfterMS: retryAfter,
	})
}

func (h *Handler) complete(ctx *echo.Context) error {
	authorization := mustAuthorization(ctx)
	var request protocol.CompleteRequest
	if err := httpapi.DecodeJSON(ctx.Response(), ctx.Request(), queueBodyLimit, &request); err != nil {
		h.writeQueueError(ctx, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: err.Error()})
		return nil
	}
	if err := authorization.CheckRoute(request.SessionRoute); err != nil {
		h.writeQueueError(ctx, err)
		return nil
	}
	if err := authorization.AllowsMutation(); err != nil {
		h.writeQueueError(ctx, err)
		return nil
	}
	results, err := authorization.Store.CompleteBatch(ctx.Request().Context(), request.Generation, h.manager.Now(),
		request.SessionID, toQueueComplete(request.Items))
	if err != nil {
		h.writeQueueError(ctx, err)
		return nil
	}
	return ctx.JSON(http.StatusOK, protocol.BatchResultResponse{Results: toProtocolResults(results)})
}

func (h *Handler) fail(ctx *echo.Context) error {
	authorization := mustAuthorization(ctx)
	var request protocol.FailRequest
	if err := httpapi.DecodeJSON(ctx.Response(), ctx.Request(), queueBodyLimit, &request); err != nil {
		h.writeQueueError(ctx, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: err.Error()})
		return nil
	}
	if err := authorization.CheckRoute(request.SessionRoute); err != nil {
		h.writeQueueError(ctx, err)
		return nil
	}
	if err := authorization.AllowsMutation(); err != nil {
		h.writeQueueError(ctx, err)
		return nil
	}
	results, err := authorization.Store.FailBatch(ctx.Request().Context(), request.Generation, h.manager.Now(),
		request.SessionID, h.maxResets, toQueueFail(request.Items))
	if err != nil {
		h.writeQueueError(ctx, err)
		return nil
	}
	return ctx.JSON(http.StatusOK, protocol.BatchResultResponse{Results: toProtocolResults(results)})
}

func (h *Handler) extendLease(ctx *echo.Context) error {
	authorization := mustAuthorization(ctx)
	var request protocol.ExtendLeaseRequest
	if err := httpapi.DecodeJSON(ctx.Response(), ctx.Request(), queueBodyLimit, &request); err != nil {
		h.writeQueueError(ctx, &queue.Error{Code: protocol.ErrorInvalidRequest, Message: err.Error()})
		return nil
	}
	if err := authorization.CheckRoute(request.SessionRoute); err != nil {
		h.writeQueueError(ctx, err)
		return nil
	}
	if err := authorization.AllowsMutation(); err != nil {
		h.writeQueueError(ctx, err)
		return nil
	}
	results, err := authorization.Store.ExtendLeaseBatch(ctx.Request().Context(), request.Generation, h.manager.Now(),
		request.SessionID, request.ExtendSeconds, toQueueAttempts(request.Items))
	if err != nil {
		h.writeQueueError(ctx, err)
		return nil
	}
	return ctx.JSON(http.StatusOK, protocol.BatchResultResponse{Results: toProtocolResults(results)})
}

func mustAuthorization(ctx *echo.Context) shard.Authorization {
	return ctx.Get(contextAuthKey).(shard.Authorization)
}

func (h *Handler) writeQueueError(ctx *echo.Context, err error) {
	var domainError *queue.Error
	if !errors.As(err, &domainError) {
		h.logger.Error("shard request failed", "error", err)
		domainError = &queue.Error{Code: protocol.ErrorInternal, Message: "internal server error"}
	}
	details := protocol.Attrs(domainError.Details)
	if details == nil {
		details = protocol.Attrs{}
	}
	httpapi.WriteError(ctx.Response(), statusForCode(domainError.Code), protocol.APIError{
		Code: domainError.Code, Message: domainError.Message,
		Retryable: domainError.Retryable, Details: details,
	})
}

func (h *Handler) echoError(ctx *echo.Context, err error) {
	status, code, message := http.StatusInternalServerError, protocol.ErrorInternal, "internal server error"
	var echoError *echo.HTTPError
	if errors.As(err, &echoError) {
		status = echoError.Code
		switch status {
		case http.StatusNotFound:
			code, message = protocol.ErrorNotFound, "route not found"
		case http.StatusMethodNotAllowed:
			code, message = protocol.ErrorInvalidRequest, "method not allowed"
		default:
			if status < 500 {
				code, message = protocol.ErrorInvalidRequest, "request rejected"
			}
		}
	}
	if status >= 500 {
		h.logger.Error("shard HTTP request failed", "error", err)
	}
	httpapi.WriteError(ctx.Response(), status, protocol.APIError{Code: code, Message: message})
}

func statusForCode(code string) int {
	switch code {
	case protocol.ErrorInvalidRequest, protocol.ErrorInvalidJob:
		return http.StatusBadRequest
	case protocol.ErrorInvalidAccessToken:
		return http.StatusUnauthorized
	case protocol.ErrorPermissionDenied:
		return http.StatusForbidden
	case protocol.ErrorNotFound:
		return http.StatusNotFound
	case protocol.ErrorStaleGeneration, protocol.ErrorShardNotActive, protocol.ErrorIdentityConflict,
		protocol.ErrorLeaseExpired, protocol.ErrorStaleAttempt, protocol.ErrorSessionExpired,
		protocol.ErrorAttemptAlreadyFinalized:
		return http.StatusConflict
	case protocol.ErrorRateLimited, protocol.ErrorBackpressure:
		return http.StatusTooManyRequests
	case protocol.ErrorShardUnavailable, protocol.ErrorOwnerLeaseExpired:
		return http.StatusServiceUnavailable
	case protocol.ErrorUnsupportedOperation:
		return http.StatusNotImplemented
	default:
		return http.StatusInternalServerError
	}
}
