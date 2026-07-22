"""Small synchronous HTTP/JSON transport for one HQ origin."""

from __future__ import annotations

import http.client
import json
import ssl
import threading
from typing import Any
from urllib.parse import quote, urlsplit

from .errors import APIError, TransportError


def _loads(data: bytes) -> Any:
    return json.loads(data, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))


def _dumps(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()


class TrackerClient:
    def __init__(
        self,
        base_url: str,
        machine_token: str,
        worker_id: str,
        client_version: str,
        *,
        timeout: float,
        allow_http: bool,
    ) -> None:
        parsed = urlsplit(base_url)
        if (
            not parsed.hostname
            or parsed.query
            or parsed.fragment
            or parsed.scheme not in {"http", "https"}
            or (parsed.scheme == "http" and not allow_http)
        ):
            raise TransportError("invalid or disallowed HQ URL")
        if not machine_token or not worker_id or not client_version:
            raise ValueError("machine token, worker ID, and client version are required")
        self._scheme, self._host = parsed.scheme, parsed.hostname
        self._port = parsed.port or (443 if parsed.scheme == "https" else 80)
        self._base_path = parsed.path.rstrip("/")
        self._timeout = timeout
        self._headers = {
            "Authorization": f"Bearer {machine_token}",
            "X-SavewebHQ-Client-Version": client_version,
        }
        self._connection: http.client.HTTPConnection | None = None
        self._lock = threading.Lock()

    def close(self) -> None:
        with self._lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None

    def project_jobs(
        self, project_id: str, operation: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        return self._request(
            "POST", f"/api/v1/projects/{quote(project_id, safe='')}/jobs/{operation}", payload
        )

    def project_policy(self, project_id: str) -> dict[str, Any]:
        return self._request("GET", f"/api/v1/projects/{quote(project_id, safe='')}")

    def _request(
        self, method: str, path: str, payload: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        body = None if payload is None else _dumps(payload)
        headers = {
            **self._headers,
            "Accept": "application/json",
            "Cache-Control": "no-store",
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        with self._lock:
            try:
                connection = self._get_connection()
                connection.request(method, f"{self._base_path}{path}", body=body, headers=headers)
                response = connection.getresponse()
                raw = response.read((8 << 20) + 1)
            except (OSError, ssl.SSLError, http.client.HTTPException) as error:
                self._connection = None
                raise TransportError(f"HTTP request failed: {error}") from error
        if len(raw) > 8 << 20:
            raise TransportError("HTTP response exceeds size limit")
        if "no-store" not in response.headers.get("Cache-Control", "").lower():
            raise TransportError("HQ response is cacheable")
        try:
            decoded = _loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise TransportError(f"invalid JSON response: {error}") from error
        if not isinstance(decoded, dict):
            raise TransportError("JSON response must be an object")
        if not 200 <= response.status < 300:
            error = decoded.get("error")
            if not isinstance(error, dict):
                raise TransportError(f"HTTP {response.status} has no error envelope")
            raise APIError(response.status, error)
        return decoded

    def _get_connection(self) -> http.client.HTTPConnection:
        if self._connection is None:
            if self._scheme == "https":
                self._connection = http.client.HTTPSConnection(
                    self._host,
                    self._port,
                    timeout=self._timeout,
                    context=ssl.create_default_context(),
                )
            else:
                self._connection = http.client.HTTPConnection(
                    self._host, self._port, timeout=self._timeout
                )
        return self._connection
