// Package trackerhttp exposes tracker.Service as the v1 Echo HTTP/JSON API.
package trackerhttp

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"

	"git.saveweb.org/saveweb/hq/internal/httpapi"
	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const trackerBodyLimit = int64(64 << 10)

type Handler struct {
	service *tracker.Service
	logger  *slog.Logger
}

func New(service *tracker.Service, logger *slog.Logger) *echo.Echo {
	if logger == nil {
		logger = slog.Default()
	}
	handler := &Handler{service: service, logger: logger}
	server := echo.New()
	server.Logger = logger
	server.HTTPErrorHandler = handler.echoError
	server.Pre(sanitizeRequestID)
	server.Use(middleware.RequestID())
	server.Use(middleware.Recover())
	server.Use(middleware.BodyLimit(trackerBodyLimit))
	server.Use(middleware.Secure())
	server.Use(noStoreAPI)

	server.GET("/healthz", handler.health)
	server.PUT("/api/v1/agents/:agent_id", handler.upsertAgent)
	server.POST("/api/v1/agents/:agent_id/heartbeat", handler.heartbeatAgent)
	server.POST("/api/v1/worker/sessions", handler.createSession)
	server.POST("/api/v1/worker/sessions/:session_id/heartbeat", handler.heartbeatSession)
	server.POST("/api/v1/worker/assignments", handler.getAssignment)
	return server
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

func noStoreAPI(next echo.HandlerFunc) echo.HandlerFunc {
	return func(ctx *echo.Context) error {
		if strings.HasPrefix(ctx.Request().URL.Path, "/api/") {
			httpapi.SetNoStore(ctx.Response().Header())
		}
		return next(ctx)
	}
}

func (h *Handler) echoError(ctx *echo.Context, err error) {
	status := http.StatusInternalServerError
	code := protocol.ErrorInternal
	message := "internal server error"
	var echoError *echo.HTTPError
	if errors.As(err, &echoError) {
		status = echoError.Code
		switch status {
		case http.StatusNotFound:
			code, message = protocol.ErrorNotFound, "route not found"
		case http.StatusMethodNotAllowed:
			code, message = protocol.ErrorInvalidRequest, "method not allowed"
		case http.StatusRequestEntityTooLarge:
			code, message = protocol.ErrorInvalidRequest, "request body is too large"
		case http.StatusBadRequest:
			code, message = protocol.ErrorInvalidRequest, "invalid request"
		default:
			if status < 500 {
				code, message = protocol.ErrorInvalidRequest, "request rejected"
			}
		}
	}
	if status >= 500 {
		h.logger.Error("tracker HTTP request failed", "error", err)
	}
	httpapi.WriteError(ctx.Response(), status, protocol.APIError{Code: code, Message: message})
}

func (h *Handler) health(ctx *echo.Context) error {
	return ctx.JSON(http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) upsertAgent(ctx *echo.Context) error {
	token, agentID, ok := machineCredentials(ctx)
	if !ok {
		return nil
	}
	if pathID := ctx.Param("agent_id"); pathID != agentID {
		return h.writeDomainError(ctx, &tracker.Error{Code: protocol.ErrorPermissionDenied, Message: "agent header and path do not match"})
	}
	var body protocol.AgentUpsertRequest
	if err := httpapi.DecodeJSON(ctx.Response(), ctx.Request(), trackerBodyLimit, &body); err != nil {
		return h.writeDomainError(ctx, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: err.Error()})
	}
	result, err := h.service.UpsertAgent(ctx.Request().Context(), token, agentID, body)
	if err != nil {
		return h.writeDomainError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, result)
}

func (h *Handler) heartbeatAgent(ctx *echo.Context) error {
	token, agentID, ok := machineCredentials(ctx)
	if !ok {
		return nil
	}
	if ctx.Param("agent_id") != agentID {
		return h.writeDomainError(ctx, &tracker.Error{Code: protocol.ErrorPermissionDenied, Message: "agent header and path do not match"})
	}
	var body protocol.AgentHeartbeatRequest
	if err := httpapi.DecodeJSON(ctx.Response(), ctx.Request(), trackerBodyLimit, &body); err != nil {
		return h.writeDomainError(ctx, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: err.Error()})
	}
	result, err := h.service.HeartbeatAgent(ctx.Request().Context(), token, agentID, body)
	if err != nil {
		return h.writeDomainError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, result)
}

func (h *Handler) createSession(ctx *echo.Context) error {
	token, agentID, ok := machineCredentials(ctx)
	if !ok {
		return nil
	}
	var body protocol.CreateSessionRequest
	if err := httpapi.DecodeJSON(ctx.Response(), ctx.Request(), trackerBodyLimit, &body); err != nil {
		return h.writeDomainError(ctx, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: err.Error()})
	}
	result, err := h.service.CreateSession(ctx.Request().Context(), token, agentID, body)
	if err != nil {
		return h.writeDomainError(ctx, err)
	}
	return ctx.JSON(http.StatusCreated, result)
}

func (h *Handler) heartbeatSession(ctx *echo.Context) error {
	token, agentID, ok := machineCredentials(ctx)
	if !ok {
		return nil
	}
	result, err := h.service.HeartbeatSession(ctx.Request().Context(), token, agentID, ctx.Param("session_id"))
	if err != nil {
		return h.writeDomainError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, result)
}

func (h *Handler) getAssignment(ctx *echo.Context) error {
	token, agentID, ok := machineCredentials(ctx)
	if !ok {
		return nil
	}
	var body protocol.GetAssignmentRequest
	if err := httpapi.DecodeJSON(ctx.Response(), ctx.Request(), trackerBodyLimit, &body); err != nil {
		return h.writeDomainError(ctx, &tracker.Error{Code: protocol.ErrorInvalidRequest, Message: err.Error()})
	}
	result, err := h.service.GetAssignment(ctx.Request().Context(), token, agentID, body)
	if err != nil {
		return h.writeDomainError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, result)
}

func machineCredentials(ctx *echo.Context) (token, agentID string, ok bool) {
	token, ok = httpapi.BearerToken(ctx.Request().Header.Get(echo.HeaderAuthorization))
	agentID = ctx.Request().Header.Get("X-Saveweb-Agent-ID")
	if !ok || agentID == "" {
		httpapi.WriteError(ctx.Response(), http.StatusUnauthorized, protocol.APIError{
			Code: protocol.ErrorInvalidMachineToken, Message: "machine credentials are required",
		})
		return "", "", false
	}
	return token, agentID, true
}

func (h *Handler) writeDomainError(ctx *echo.Context, err error) error {
	var domainError *tracker.Error
	if !errors.As(err, &domainError) {
		h.logger.Error("tracker request failed", "error", err)
		httpapi.WriteError(ctx.Response(), http.StatusInternalServerError, protocol.APIError{
			Code: protocol.ErrorInternal, Message: "internal server error",
		})
		return nil
	}
	status := statusForCode(domainError.Code)
	httpapi.WriteError(ctx.Response(), status, protocol.APIError{
		Code: domainError.Code, Message: domainError.Message, Retryable: domainError.Retryable,
		RetryAfterMS: domainError.RetryAfter, Details: domainError.Details,
	})
	return nil
}

func statusForCode(code string) int {
	switch code {
	case protocol.ErrorInternal:
		return http.StatusInternalServerError
	case protocol.ErrorInvalidRequest, protocol.ErrorInvalidJob:
		return http.StatusBadRequest
	case protocol.ErrorInvalidMachineToken, protocol.ErrorInvalidAccessToken:
		return http.StatusUnauthorized
	case protocol.ErrorPermissionDenied, protocol.ErrorAgentDisabled:
		return http.StatusForbidden
	case protocol.ErrorNotFound:
		return http.StatusNotFound
	case protocol.ErrorStaleGeneration, protocol.ErrorStaleEndpointVersion,
		protocol.ErrorShardNotActive, protocol.ErrorIdentityConflict,
		protocol.ErrorSessionExpired:
		return http.StatusConflict
	case protocol.ErrorRateLimited, protocol.ErrorBackpressure:
		return http.StatusTooManyRequests
	case protocol.ErrorShardUnavailable, protocol.ErrorOwnerLeaseExpired:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
