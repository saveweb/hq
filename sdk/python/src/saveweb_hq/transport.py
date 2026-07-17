"""Small synchronous HTTP/JSON transports with explicit TLS and cache policy."""

from __future__ import annotations

import base64
import hashlib
import hmac
import http.client
import json
import ssl
import threading
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote, urlsplit

from cryptography import x509
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from .errors import APIError, TransportError


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def _loads(data: bytes) -> Any:
    return json.loads(
        data,
        object_pairs_hook=_strict_object,
        parse_constant=_reject_constant,
    )


def _dumps(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()


def _decode_pin(value: str) -> bytes:
    try:
        decoded = base64.urlsafe_b64decode(value + "=")
    except (ValueError, TypeError) as error:
        raise TransportError("invalid shard SPKI pin") from error
    if len(decoded) != hashlib.sha256().digest_size:
        raise TransportError("invalid shard SPKI pin")
    return decoded


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host: str, port: int, timeout: float, pin: bytes) -> None:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        super().__init__(host, port, timeout=timeout, context=context)
        self._pin = pin

    def connect(self) -> None:
        super().connect()
        assert self.sock is not None
        certificate_der = self.sock.getpeercert(binary_form=True)
        if not certificate_der:
            self.close()
            raise ssl.SSLCertVerificationError("shard TLS peer has no certificate")
        try:
            certificate = x509.load_der_x509_certificate(certificate_der)
            now = datetime.now(timezone.utc)
            if now < certificate.not_valid_before_utc or now > certificate.not_valid_after_utc:
                raise ssl.SSLCertVerificationError(
                    "shard TLS certificate is outside its validity period"
                )
            spki = certificate.public_key().public_bytes(
                Encoding.DER, PublicFormat.SubjectPublicKeyInfo
            )
            actual = hashlib.sha256(spki).digest()
            if not hmac.compare_digest(actual, self._pin):
                raise ssl.SSLCertVerificationError("shard TLS SPKI pin mismatch")
        except Exception:
            self.close()
            raise


class JSONTransport:
    """One-origin, keep-alive HTTP transport. Requests are serialized per origin."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float,
        allow_http: bool,
        spki_pin: str | None = None,
        max_request_bytes: int = 1 << 20,
        max_response_bytes: int = 16 << 20,
    ) -> None:
        parsed = urlsplit(base_url)
        if (
            not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.scheme not in {"http", "https"}
            or (parsed.scheme == "http" and not allow_http)
        ):
            raise TransportError("invalid or disallowed HTTP endpoint")
        if parsed.scheme == "http" and spki_pin is not None:
            raise TransportError("HTTP endpoint cannot use a TLS pin")
        try:
            port = parsed.port
        except ValueError as error:
            raise TransportError("invalid HTTP endpoint port") from error
        self._scheme = parsed.scheme
        self._host = parsed.hostname
        self._port = port or (443 if parsed.scheme == "https" else 80)
        self._base_path = parsed.path.rstrip("/")
        self._timeout = timeout
        self._pin = _decode_pin(spki_pin) if spki_pin is not None else None
        self._max_request_bytes = max_request_bytes
        self._max_response_bytes = max_response_bytes
        self._connection: http.client.HTTPConnection | None = None
        self._lock = threading.Lock()

    def close(self) -> None:
        with self._lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None

    def request_json(
        self,
        method: str,
        endpoint: str,
        payload: Any | None,
        headers: dict[str, str],
    ) -> dict[str, Any]:
        body = b"" if payload is None else _dumps(payload)
        if len(body) > self._max_request_bytes:
            raise APIError(
                0,
                {
                    "code": "invalid_request",
                    "message": f"request exceeds {self._max_request_bytes} byte limit",
                },
            )
        request_headers = {
            "Accept": "application/json",
            "Cache-Control": "no-store, no-cache, max-age=0",
            "Pragma": "no-cache",
            **headers,
        }
        if payload is not None:
            request_headers["Content-Type"] = "application/json"
        with self._lock:
            try:
                connection = self._get_connection()
                connection.request(
                    method, self._base_path + endpoint, body=body, headers=request_headers
                )
                response = connection.getresponse()
                raw = response.read(self._max_response_bytes + 1)
                if response.will_close:
                    connection.close()
                    self._connection = None
            except (OSError, ssl.SSLError, http.client.HTTPException) as error:
                if self._connection is not None:
                    self._connection.close()
                    self._connection = None
                raise TransportError(f"HTTP request failed: {error}") from error
        if len(raw) > self._max_response_bytes:
            self.close()
            raise TransportError("HTTP response exceeds size limit")
        self._validate_cache(response.headers)
        content_type = response.headers.get_content_type()
        if content_type != "application/json":
            raise TransportError("HTTP response is not application/json")
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
        if self._connection is not None:
            return self._connection
        if self._scheme == "http":
            connection: http.client.HTTPConnection = http.client.HTTPConnection(
                self._host, self._port, timeout=self._timeout
            )
        elif self._pin is not None:
            connection = _PinnedHTTPSConnection(self._host, self._port, self._timeout, self._pin)
        else:
            context = ssl.create_default_context()
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            connection = http.client.HTTPSConnection(
                self._host, self._port, timeout=self._timeout, context=context
            )
        self._connection = connection
        return connection

    @staticmethod
    def _validate_cache(headers: http.client.HTTPMessage) -> None:
        if "no-store" not in headers.get("Cache-Control", "").lower():
            raise APIError(
                0,
                {
                    "code": "cache_misconfigured",
                    "message": "response is missing Cache-Control: no-store",
                },
            )
        cache_status = headers.get("CF-Cache-Status", "").strip().upper()
        if cache_status not in {"", "DYNAMIC", "BYPASS"}:
            raise APIError(
                0,
                {"code": "cache_misconfigured", "message": "response may be cached"},
            )


class TrackerClient:
    def __init__(
        self,
        base_url: str,
        machine_token: str,
        agent_id: str,
        *,
        timeout: float,
        allow_http: bool,
    ) -> None:
        if not machine_token or not agent_id:
            raise ValueError("machine token and agent ID are required")
        self._agent_id = agent_id
        self._headers = {
            "Authorization": f"Bearer {machine_token}",
            "X-Saveweb-Agent-ID": agent_id,
        }
        self._transport = JSONTransport(
            base_url,
            timeout=timeout,
            allow_http=allow_http,
            max_request_bytes=8 << 20,
            max_response_bytes=8 << 20,
        )

    def close(self) -> None:
        self._transport.close()

    def upsert_agent(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._transport.request_json(
            "PUT", f"/api/v1/agents/{quote(self._agent_id, safe='')}", payload, self._headers
        )

    def heartbeat_agent(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._transport.request_json(
            "POST",
            f"/api/v1/agents/{quote(self._agent_id, safe='')}/heartbeat",
            payload,
            self._headers,
        )

    def create_session(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._transport.request_json(
            "POST", "/api/v1/worker/sessions", payload, self._headers
        )

    def heartbeat_session(self, session_id: str) -> dict[str, Any]:
        return self._transport.request_json(
            "POST",
            f"/api/v1/worker/sessions/{quote(session_id, safe='')}/heartbeat",
            None,
            self._headers,
        )

    def get_assignment(self, session_id: str, accept_types: list[str]) -> dict[str, Any]:
        return self._transport.request_json(
            "POST",
            "/api/v1/worker/assignments",
            {"session_id": session_id, "accept_types": accept_types},
            self._headers,
        )

    def submit_receiver_batch(
        self,
        receiver_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return self._transport.request_json(
            "POST",
            f"/api/v1/worker/receivers/{quote(receiver_id, safe='')}/batches",
            payload,
            self._headers,
        )
