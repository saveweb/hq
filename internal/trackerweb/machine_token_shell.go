package trackerweb

import (
	_ "embed"
	"net/http"

	"github.com/labstack/echo/v5"
)

//go:embed machine_token_shell.sh
var machineTokenShellScript string

func (h *Handler) machineTokenShell(ctx *echo.Context) error {
	ctx.Response().Header().Set("Content-Type", "text/x-shellscript; charset=utf-8")
	ctx.Response().Header().Set("Cache-Control", "public, max-age=300")
	ctx.Response().Header().Set("X-Content-Type-Options", "nosniff")
	return ctx.String(http.StatusOK, machineTokenShellScript)
}
