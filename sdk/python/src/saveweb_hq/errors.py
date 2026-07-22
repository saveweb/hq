"""SavewebHQ SDK errors."""

from typing import Any


class SavewebHQError(Exception):
    pass


class TransportError(SavewebHQError):
    pass


class APIError(SavewebHQError):
    def __init__(self, status: int, error: dict[str, Any]) -> None:
        self.status = status
        self.error = error
        self.code = str(error.get("code", "internal_error"))
        retry_after_ms = error.get("retry_after_ms", 0)
        self.retryable = bool(error.get("retryable", False))
        self.retry_after_ms = retry_after_ms if isinstance(retry_after_ms, int) else 0
        super().__init__(f"HTTP {status}: {self.code}: {error.get('message', '')}")
