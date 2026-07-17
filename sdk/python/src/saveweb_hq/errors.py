"""Stable worker SDK errors."""

from __future__ import annotations

from typing import Any


class SavewebHQError(Exception):
    """Base class for SDK failures."""


class TransportError(SavewebHQError):
    """The peer did not return a valid HTTP/JSON response."""


class APIError(SavewebHQError):
    """A stable API error returned by tracker or shard."""

    def __init__(self, status: int, error: dict[str, Any]) -> None:
        self.status = status
        self.code = str(error.get("code", "internal_error"))
        self.message = str(error.get("message", "request failed"))
        self.retryable = bool(error.get("retryable", False))
        try:
            self.retry_after_ms = int(error.get("retry_after_ms", 0))
        except (TypeError, ValueError):
            self.retry_after_ms = 0
        details = error.get("details", {})
        self.details = details if isinstance(details, dict) else {}
        super().__init__(f"HTTP {status}: {self.code}: {self.message}")


class NoAssignmentError(SavewebHQError):
    """No active shard is currently assignable."""


class RouteRetiredError(SavewebHQError):
    """The claimed owner/generation changed; the local outcome must be discarded."""


class SessionClosedError(SavewebHQError):
    """The worker session has been closed."""


class ClaimsPausedError(SavewebHQError):
    """New claims are paused by local administration."""
