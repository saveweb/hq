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
        super().__init__(f"HTTP {status}: {self.code}: {error.get('message', '')}")
