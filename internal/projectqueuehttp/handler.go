// Package projectqueuehttp exposes the single-site PostgreSQL job queue.
package projectqueuehttp

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"

	"git.saveweb.org/saveweb/hq/internal/httpapi"
	"git.saveweb.org/saveweb/hq/internal/queue"
	"git.saveweb.org/saveweb/hq/internal/sourceformat"
	"git.saveweb.org/saveweb/hq/internal/tracker"
	"git.saveweb.org/saveweb/hq/internal/tracker/postgres"
	"git.saveweb.org/saveweb/hq/pkg/protocol"
)

const (
	bodyLimit             = int64(8 << 20)
	sourceCompressedLimit = int64(256 << 20)
	sourceExpandedLimit   = int64(4 << 30)
	sourceJobLimit        = int64(10_000_000)
)

type handler struct {
	store *postgres.Store
	now   func() int64
}

func New(store *postgres.Store, now func() int64, logger *slog.Logger) *echo.Echo {
	if logger == nil {
		logger = slog.Default()
	}
	server := echo.New()
	server.Logger = logger
	server.HTTPErrorHandler = func(ctx *echo.Context, err error) {
		status := http.StatusInternalServerError
		var statusError interface{ StatusCode() int }
		if errors.As(err, &statusError) {
			status = statusError.StatusCode()
		}
		code := protocol.ErrorInternal
		message := "internal server error"
		if status == http.StatusNotFound {
			code, message = protocol.ErrorNotFound, "route not found"
		} else if status == http.StatusMethodNotAllowed {
			code, message = protocol.ErrorInvalidRequest, "method not allowed"
		} else if status >= 400 && status < 500 {
			code, message = protocol.ErrorInvalidRequest, "request rejected"
		}
		if status >= 500 {
			logger.Error("HTTP request failed", "error", err)
		}
		httpapi.WriteError(ctx.Response(), status, protocol.APIError{Code: code, Message: message})
	}
	server.Use(middleware.RequestID())
	server.Use(middleware.Recover())
	server.Use(middleware.BodyLimit(sourceCompressedLimit))
	server.Use(middleware.Secure())
	server.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(ctx *echo.Context) error {
			if strings.HasPrefix(ctx.Request().URL.Path, "/api/") {
				httpapi.SetNoStore(ctx.Response().Header())
			}
			return next(ctx)
		}
	})
	server.GET("/healthz", func(ctx *echo.Context) error { return ctx.JSON(http.StatusOK, map[string]string{"status": "ok"}) })
	Register(server, store, now)
	return server
}

func Register(server *echo.Echo, store *postgres.Store, now func() int64) {
	h := &handler{store: store, now: now}
	server.GET("/api/v1/admin/projects", h.listProjects)
	server.GET("/api/v1/admin/users", h.listUsers)
	server.PUT("/api/v1/admin/users/:user_id", h.putUser)
	server.DELETE("/api/v1/admin/users/:user_id", h.deleteUser)
	server.GET("/api/v1/admin/users/:user_id/machine-token", h.getMachineToken)
	server.POST("/api/v1/admin/users/:user_id/machine-token", h.rotateMachineToken)
	server.DELETE("/api/v1/admin/users/:user_id/machine-token", h.revokeMachineToken)
	server.GET("/api/v1/admin/projects/:project_id", h.getProject)
	server.PUT("/api/v1/admin/projects/:project_id", h.putProject)
	server.DELETE("/api/v1/admin/projects/:project_id", h.deleteProject)
	server.POST("/api/v1/admin/projects/:project_id/jobs", h.enqueueJobs)
	server.POST("/api/v1/admin/projects/:project_id/source", h.enqueueSource)
	server.GET("/api/v1/admin/projects/:project_id/jobs", h.listJobs)
	server.GET("/api/v1/admin/projects/:project_id/jobs/:job_id", h.getJob)
	server.POST("/api/v1/admin/projects/:project_id/jobs/:job_id/requeue", h.requeueJob)
	server.DELETE("/api/v1/admin/projects/:project_id/jobs/:job_id", h.deleteJob)
	server.POST("/api/v1/projects/:project_id/jobs/claim", h.claim)
	server.POST("/api/v1/projects/:project_id/jobs/complete", h.complete)
	server.POST("/api/v1/projects/:project_id/jobs/fail", h.fail)
	server.POST("/api/v1/projects/:project_id/jobs/extend-lease", h.extendLease)
}

func (h *handler) listUsers(ctx *echo.Context) error {
	if _, ok := h.authenticateAdmin(ctx); !ok {
		return nil
	}
	users, err := h.store.ListUsers(ctx.Request().Context())
	if err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, protocol.AdminUserListResponse{Users: users})
}

func (h *handler) putUser(ctx *echo.Context) error {
	if _, ok := h.authenticateAdmin(ctx); !ok {
		return nil
	}
	var request protocol.AdminUserRequest
	if !h.decode(ctx, &request) {
		return nil
	}
	if err := h.store.PutUser(ctx.Request().Context(), ctx.Param("user_id"), request.Status, request.Roles, h.now()); err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.NoContent(http.StatusNoContent)
}

func (h *handler) deleteUser(ctx *echo.Context) error {
	if _, ok := h.authenticateAdmin(ctx); !ok {
		return nil
	}
	if err := h.store.DeleteUser(ctx.Request().Context(), ctx.Param("user_id")); err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.NoContent(http.StatusNoContent)
}

func (h *handler) rotateMachineToken(ctx *echo.Context) error {
	if _, ok := h.authenticateAdmin(ctx); !ok {
		return nil
	}
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return h.writeError(ctx, err)
	}
	token := "hq_" + base64.RawURLEncoding.EncodeToString(raw[:])
	userID := ctx.Param("user_id")
	if err := h.store.RotateMachineToken(ctx.Request().Context(), userID, token, h.now()); err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.JSON(http.StatusCreated, protocol.AdminMachineTokenResponse{UserID: userID, Token: token})
}

func (h *handler) getMachineToken(ctx *echo.Context) error {
	if _, ok := h.authenticateAdmin(ctx); !ok {
		return nil
	}
	userID := ctx.Param("user_id")
	token, active, err := h.store.MachineToken(ctx.Request().Context(), userID)
	if err != nil {
		return h.writeError(ctx, err)
	}
	if !active || token == "" {
		return h.writeError(ctx, &tracker.Error{Code: protocol.ErrorNotFound, Message: "machine token not found"})
	}
	return ctx.JSON(http.StatusOK, protocol.AdminMachineTokenResponse{UserID: userID, Token: token})
}

func (h *handler) revokeMachineToken(ctx *echo.Context) error {
	if _, ok := h.authenticateAdmin(ctx); !ok {
		return nil
	}
	if err := h.store.RevokeMachineToken(ctx.Request().Context(), ctx.Param("user_id"), h.now()); err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.NoContent(http.StatusNoContent)
}

func (h *handler) listProjects(ctx *echo.Context) error {
	if _, ok := h.authenticateAdmin(ctx); !ok {
		return nil
	}
	projects, err := h.store.ListProjectSummaries(ctx.Request().Context())
	if err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, protocol.AdminProjectListResponse{Projects: projects})
}

func (h *handler) getProject(ctx *echo.Context) error {
	if _, ok := h.authenticateAdmin(ctx); !ok {
		return nil
	}
	project, err := h.store.ProjectSummary(ctx.Request().Context(), ctx.Param("project_id"))
	if err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, project)
}

func (h *handler) putProject(ctx *echo.Context) error {
	if _, ok := h.authenticateAdmin(ctx); !ok {
		return nil
	}
	var request protocol.AdminProjectRequest
	if !h.decode(ctx, &request) {
		return nil
	}
	projectID := ctx.Param("project_id")
	if err := h.store.PutProject(ctx.Request().Context(), tracker.Project{ID: projectID, Status: request.Status, IdentityMode: request.IdentityMode}, h.now()); err != nil {
		return h.writeError(ctx, err)
	}
	project, err := h.store.ProjectSummary(ctx.Request().Context(), projectID)
	if err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, project)
}

func (h *handler) deleteProject(ctx *echo.Context) error {
	if _, ok := h.authenticateAdmin(ctx); !ok {
		return nil
	}
	if err := h.store.DeleteProject(ctx.Request().Context(), ctx.Param("project_id")); err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.NoContent(http.StatusNoContent)
}

func (h *handler) enqueueJobs(ctx *echo.Context) error {
	if _, ok := h.authenticateAdmin(ctx); !ok {
		return nil
	}
	var request protocol.AdminEnqueueJobsRequest
	if !h.decode(ctx, &request) {
		return nil
	}
	if len(request.Jobs) == 0 || len(request.Jobs) > 256 {
		return h.writeAPIError(ctx, http.StatusBadRequest, protocol.APIError{Code: protocol.ErrorInvalidRequest, Message: "jobs must contain 1-256 items"})
	}
	projectID := ctx.Param("project_id")
	inserted, err := h.store.EnqueueProjectJobs(ctx.Request().Context(), projectID, request.Jobs, h.now())
	if err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, protocol.AdminEnqueueJobsResponse{ProjectID: projectID, Submitted: len(request.Jobs), Inserted: inserted})
}

func (h *handler) enqueueSource(ctx *echo.Context) error {
	if _, ok := h.authenticateAdmin(ctx); !ok {
		return nil
	}
	mediaType, _, err := mime.ParseMediaType(ctx.Request().Header.Get("Content-Type"))
	if err != nil || (mediaType != "application/zstd" && mediaType != "application/octet-stream") {
		return h.writeAPIError(ctx, http.StatusUnsupportedMediaType, protocol.APIError{Code: protocol.ErrorInvalidRequest, Message: "source body must be application/zstd"})
	}
	projectID := ctx.Param("project_id")
	var inserted int64
	var enqueueErr error
	stats, err := sourceformat.Decode(ctx.Request().Context(), io.LimitReader(ctx.Request().Body, sourceCompressedLimit+1), sourceformat.Limits{MaxUncompressedBytes: sourceExpandedLimit, MaxJobs: sourceJobLimit}, func(batch []queue.JobSpec) error {
		jobs := make([]protocol.JobSpecV1, 0, len(batch))
		for _, job := range batch {
			jobs = append(jobs, protocol.JobSpecV1{ID: job.ID, Value: job.Value, Type: job.Type, Via: job.Via, Hops: job.Hops, Attrs: job.Attrs})
		}
		count, err := h.store.EnqueueProjectJobs(ctx.Request().Context(), projectID, jobs, h.now())
		inserted += count
		enqueueErr = err
		return err
	})
	if enqueueErr != nil {
		return h.writeError(ctx, enqueueErr)
	}
	if err != nil {
		return h.writeAPIError(ctx, http.StatusBadRequest, protocol.APIError{Code: protocol.ErrorInvalidJob, Message: err.Error()})
	}
	return ctx.JSON(http.StatusOK, protocol.AdminEnqueueSourceResponse{ProjectID: projectID, Jobs: stats.Jobs, Inserted: inserted, UncompressedBytes: stats.UncompressedBytes})
}

func (h *handler) listJobs(ctx *echo.Context) error {
	if _, ok := h.authenticateAdmin(ctx); !ok {
		return nil
	}
	after, limit := int64(0), 50
	var err error
	if text := ctx.QueryParam("after_job_id"); text != "" {
		after, err = strconv.ParseInt(text, 10, 64)
		if err != nil {
			return h.writeAPIError(ctx, http.StatusBadRequest, protocol.APIError{Code: protocol.ErrorInvalidRequest, Message: "invalid after_job_id"})
		}
	}
	if text := ctx.QueryParam("limit"); text != "" {
		limit, err = strconv.Atoi(text)
		if err != nil {
			return h.writeAPIError(ctx, http.StatusBadRequest, protocol.APIError{Code: protocol.ErrorInvalidRequest, Message: "invalid limit"})
		}
	}
	result, err := h.store.ListProjectJobs(ctx.Request().Context(), ctx.Param("project_id"), ctx.QueryParam("status"), after, limit)
	if err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, result)
}

func (h *handler) getJob(ctx *echo.Context) error {
	if _, ok := h.authenticateAdmin(ctx); !ok {
		return nil
	}
	jobID, err := strconv.ParseInt(ctx.Param("job_id"), 10, 64)
	if err != nil {
		return h.writeAPIError(ctx, http.StatusBadRequest, protocol.APIError{Code: protocol.ErrorInvalidRequest, Message: "invalid job_id"})
	}
	job, err := h.store.ProjectJob(ctx.Request().Context(), ctx.Param("project_id"), jobID)
	if err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, job)
}

func (h *handler) requeueJob(ctx *echo.Context) error {
	if _, ok := h.authenticateAdmin(ctx); !ok {
		return nil
	}
	jobID, err := strconv.ParseInt(ctx.Param("job_id"), 10, 64)
	if err != nil {
		return h.writeAPIError(ctx, http.StatusBadRequest, protocol.APIError{Code: protocol.ErrorInvalidRequest, Message: "invalid job_id"})
	}
	if err := h.store.RequeueProjectJob(ctx.Request().Context(), ctx.Param("project_id"), jobID, h.now()); err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.NoContent(http.StatusNoContent)
}

func (h *handler) deleteJob(ctx *echo.Context) error {
	if _, ok := h.authenticateAdmin(ctx); !ok {
		return nil
	}
	jobID, err := strconv.ParseInt(ctx.Param("job_id"), 10, 64)
	if err != nil {
		return h.writeAPIError(ctx, http.StatusBadRequest, protocol.APIError{Code: protocol.ErrorInvalidRequest, Message: "invalid job_id"})
	}
	if err := h.store.DeleteProjectJob(ctx.Request().Context(), ctx.Param("project_id"), jobID); err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.NoContent(http.StatusNoContent)
}

func (h *handler) claim(ctx *echo.Context) error {
	user, ok := h.authenticate(ctx)
	if !ok {
		return nil
	}
	var request protocol.ProjectClaimRequest
	if !h.decode(ctx, &request) {
		return nil
	}
	jobs, err := h.store.ClaimProjectJobs(ctx.Request().Context(), user.ID, ctx.Param("project_id"), request, h.now())
	if err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, protocol.ProjectClaimResponse{ProjectID: ctx.Param("project_id"), Jobs: jobs, RetryAfterMS: 1000})
}

func (h *handler) complete(ctx *echo.Context) error {
	user, ok := h.authenticate(ctx)
	if !ok {
		return nil
	}
	var request protocol.ProjectCompleteRequest
	if !h.decode(ctx, &request) {
		return nil
	}
	result, err := h.store.CompleteProjectJobs(ctx.Request().Context(), user.ID, ctx.Param("project_id"), request, h.now())
	if err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, result)
}

func (h *handler) fail(ctx *echo.Context) error {
	user, ok := h.authenticate(ctx)
	if !ok {
		return nil
	}
	var request protocol.ProjectFailRequest
	if !h.decode(ctx, &request) {
		return nil
	}
	result, err := h.store.FailProjectJobs(ctx.Request().Context(), user.ID, ctx.Param("project_id"), request, h.now())
	if err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, result)
}

func (h *handler) extendLease(ctx *echo.Context) error {
	user, ok := h.authenticate(ctx)
	if !ok {
		return nil
	}
	var request protocol.ProjectExtendLeaseRequest
	if !h.decode(ctx, &request) {
		return nil
	}
	result, err := h.store.ExtendProjectJobLeases(ctx.Request().Context(), user.ID, ctx.Param("project_id"), request, h.now())
	if err != nil {
		return h.writeError(ctx, err)
	}
	return ctx.JSON(http.StatusOK, result)
}

func (h *handler) authenticate(ctx *echo.Context) (tracker.User, bool) {
	token, valid := httpapi.BearerToken(ctx.Request().Header.Get("Authorization"))
	if !valid {
		h.writeAPIError(ctx, http.StatusUnauthorized, protocol.APIError{Code: protocol.ErrorInvalidMachineToken, Message: "machine token required"})
		return tracker.User{}, false
	}
	user, err := h.store.AuthenticateMachineToken(ctx.Request().Context(), token)
	if err != nil {
		h.writeError(ctx, err)
		return tracker.User{}, false
	}
	return user, true
}

func (h *handler) authenticateAdmin(ctx *echo.Context) (tracker.User, bool) {
	user, ok := h.authenticate(ctx)
	if !ok {
		return tracker.User{}, false
	}
	if user.Status != tracker.UserStatusActive || !user.HasRole(tracker.RoleAdmin) {
		h.writeAPIError(ctx, http.StatusForbidden, protocol.APIError{Code: protocol.ErrorPermissionDenied, Message: "active admin role required"})
		return tracker.User{}, false
	}
	return user, true
}

func (h *handler) decode(ctx *echo.Context, target any) bool {
	if err := httpapi.DecodeJSON(ctx.Response(), ctx.Request(), bodyLimit, target); err != nil {
		h.writeAPIError(ctx, http.StatusBadRequest, protocol.APIError{Code: protocol.ErrorInvalidRequest, Message: err.Error()})
		return false
	}
	return true
}

func (h *handler) writeError(ctx *echo.Context, err error) error {
	var domainError *tracker.Error
	if !errors.As(err, &domainError) {
		return h.writeAPIError(ctx, http.StatusInternalServerError, protocol.APIError{Code: protocol.ErrorInternal, Message: "internal server error"})
	}
	status := http.StatusConflict
	switch domainError.Code {
	case protocol.ErrorInvalidMachineToken:
		status = http.StatusUnauthorized
	case protocol.ErrorPermissionDenied:
		status = http.StatusForbidden
	case protocol.ErrorNotFound:
		status = http.StatusNotFound
	case protocol.ErrorInvalidRequest, protocol.ErrorInvalidJob:
		status = http.StatusBadRequest
	}
	return h.writeAPIError(ctx, status, protocol.APIError{Code: domainError.Code, Message: domainError.Message, Retryable: domainError.Retryable})
}

func (h *handler) writeAPIError(ctx *echo.Context, status int, value protocol.APIError) error {
	httpapi.WriteError(ctx.Response(), status, value)
	return nil
}
