"""Synchronous worker session and generation-bound queue batches."""

from __future__ import annotations

import copy
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

from .errors import (
    APIError,
    NoAssignmentError,
    RouteRetiredError,
    SessionClosedError,
    TransportError,
)
from .transport import JSONTransport, TrackerClient, _dumps, _loads


@dataclass(frozen=True, slots=True)
class Config:
    tracker_url: str
    machine_token: str
    agent_id: str
    agent_name: str = "Saveweb worker"
    agent_version: str = "python-sdk-dev"
    allow_http_tracker: bool = False
    allow_http_shard: bool = False
    request_timeout: float = 45.0
    on_background_error: Callable[[Exception], None] | None = None

    def validate(self) -> None:
        if not self.tracker_url or not self.machine_token or not self.agent_id:
            raise ValueError("tracker URL and machine credentials are required")
        if not 1.0 <= self.request_timeout <= 600.0:
            raise ValueError("request_timeout must be between 1 and 600 seconds")


@dataclass(frozen=True, slots=True)
class Route:
    project_id: str
    shard_id: str
    generation: int


@dataclass(frozen=True, slots=True)
class _RouteIdentity:
    project_id: str
    shard_id: str
    generation: int
    owner_agent_id: str

    def matches(self, assignment: dict[str, Any]) -> bool:
        return (
            self.project_id == assignment.get("project_id")
            and self.shard_id == assignment.get("shard_id")
            and self.generation == assignment.get("generation")
            and self.owner_agent_id == assignment.get("owner_agent_id")
        )


class _RouteClient:
    def __init__(self, assignment: dict[str, Any], config: Config) -> None:
        try:
            self.identity = _RouteIdentity(
                project_id=str(assignment["project_id"]),
                shard_id=str(assignment["shard_id"]),
                generation=int(assignment["generation"]),
                owner_agent_id=str(assignment["owner_agent_id"]),
            )
            self.endpoint_version = int(assignment["endpoint_version"])
            self.access_token = str(assignment["access_token"])
            self.access_token_expires_at = int(assignment["access_token_expires_at"])
            endpoint = str(assignment["endpoint"])
            pin_value = assignment.get("tls_spki_sha256")
            pin = None if pin_value is None else str(pin_value)
        except (KeyError, TypeError, ValueError) as error:
            raise TransportError(f"invalid tracker assignment: {error}") from error
        if self.identity.generation < 1 or not self.access_token:
            raise TransportError("invalid tracker assignment identity or token")
        self._transport = JSONTransport(
            endpoint,
            timeout=config.request_timeout,
            allow_http=config.allow_http_shard,
            spki_pin=pin,
            max_request_bytes=1 << 20,
            max_response_bytes=16 << 20,
        )

    def close(self) -> None:
        self._transport.close()

    def request(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._transport.request_json(
            "POST",
            endpoint,
            payload,
            {"Authorization": f"Bearer {self.access_token}"},
        )


class Session:
    def __init__(
        self,
        config: Config,
        tracker: TrackerClient,
        session_id: str,
        project_id: str,
        attrs: dict[str, Any],
        lease_expires_at: int,
        agent_heartbeat_seconds: int,
        session_heartbeat_seconds: int,
    ) -> None:
        self.config = config
        self.id = session_id
        self.project_id = project_id
        self._tracker = tracker
        self._attrs = attrs
        self._lease_expires_at = lease_expires_at
        self._agent_heartbeat_seconds = max(agent_heartbeat_seconds, 1)
        self._session_heartbeat_seconds = max(session_heartbeat_seconds, 1)
        self._assignment: _RouteClient | None = None
        self._assignment_session_lease = 0
        self._last_background_error: Exception | None = None
        self._closed = False
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads = [
            threading.Thread(target=self._agent_heartbeat_loop, daemon=True),
            threading.Thread(target=self._session_heartbeat_loop, daemon=True),
        ]
        for thread in self._threads:
            thread.start()

    @property
    def last_background_error(self) -> Exception | None:
        with self._lock:
            return self._last_background_error

    def __enter__(self) -> Session:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            route = self._assignment
            self._assignment = None
            self._stop.set()
        for thread in self._threads:
            thread.join(timeout=self.config.request_timeout + 1.0)
        if route is not None:
            route.close()
        self._tracker.close()

    def claim(
        self,
        max_jobs: int = 64,
        lease_seconds: int = 300,
        accept_types: list[str] | None = None,
    ) -> Batch:
        accepted = [] if accept_types is None else list(accept_types)
        route = self._route(accepted, expected=None, force=False)
        request = {
            **self._route_request(route.identity),
            "max_jobs": max_jobs,
            "lease_seconds": lease_seconds,
            "accept_types": accepted,
        }
        response = route.request("/api/v1/queue/claim", request)
        if (
            response.get("project_id") != route.identity.project_id
            or response.get("shard_id") != route.identity.shard_id
            or response.get("generation") != route.identity.generation
        ):
            raise TransportError("shard returned a mismatched claim route")
        jobs = response.get("jobs")
        if not isinstance(jobs, list):
            raise TransportError("claim response jobs must be a list")
        return Batch(self, route.identity, jobs)

    def _route(
        self,
        accept_types: list[str],
        *,
        expected: _RouteIdentity | None,
        force: bool,
    ) -> _RouteClient:
        with self._lock:
            if self._closed:
                raise SessionClosedError("worker session is closed")
            now = int(time.time())
            if (
                not force
                and self._assignment is not None
                and self._assignment.access_token_expires_at > now + 30
                and self._assignment_session_lease == self._lease_expires_at
                and (expected is None or expected == self._assignment.identity)
            ):
                return self._assignment
            lease_at_request = self._lease_expires_at
        response = self._tracker.get_assignment(self.id, accept_types)
        assignment = response.get("assignment")
        if assignment is None:
            if expected is not None:
                raise RouteRetiredError("claimed shard route is no longer assigned")
            raise NoAssignmentError("no active shard assignment")
        if not isinstance(assignment, dict):
            raise TransportError("tracker assignment must be an object or null")
        if expected is not None and not expected.matches(assignment):
            raise RouteRetiredError("claimed shard owner or generation changed")
        created = _RouteClient(assignment, self.config)
        with self._lock:
            if self._closed:
                created.close()
                raise SessionClosedError("worker session is closed")
            previous = self._assignment
            self._assignment = created
            self._assignment_session_lease = lease_at_request
        if previous is not None:
            previous.close()
        return created

    def _route_request(self, identity: _RouteIdentity) -> dict[str, Any]:
        return {
            "project_id": identity.project_id,
            "shard_id": identity.shard_id,
            "generation": identity.generation,
            "session_id": self.id,
        }

    def _report_background_error(self, error: Exception) -> None:
        with self._lock:
            self._last_background_error = error
            callback = self.config.on_background_error
        if callback is not None:
            try:
                callback(error)
            except Exception as callback_error:  # noqa: BLE001 - keep heartbeat thread alive.
                with self._lock:
                    self._last_background_error = callback_error

    def _agent_heartbeat_loop(self) -> None:
        interval = float(self._agent_heartbeat_seconds)
        while not self._stop.wait(interval):
            try:
                response = self._tracker.heartbeat_agent(
                    {"version": self.config.agent_version, "attrs": self._attrs}
                )
                interval = float(max(int(response["heartbeat_after_seconds"]), 1))
            except Exception as error:  # noqa: BLE001 - surfaced through the SDK callback.
                self._report_background_error(error)
                interval = 5.0

    def _session_heartbeat_loop(self) -> None:
        interval = float(self._session_heartbeat_seconds)
        while not self._stop.wait(interval):
            try:
                response = self._tracker.heartbeat_session(self.id)
                with self._lock:
                    self._lease_expires_at = int(response["lease_expires_at"])
                interval = float(max(int(response["heartbeat_after_seconds"]), 1))
            except Exception as error:  # noqa: BLE001 - surfaced through the SDK callback.
                self._report_background_error(error)
                interval = 5.0


class Batch:
    def __init__(self, session: Session, identity: _RouteIdentity, jobs: list[Any]) -> None:
        self._session = session
        self._identity = identity
        if not all(isinstance(job, dict) for job in jobs):
            raise TransportError("claim response contains a non-object job")
        self.jobs: tuple[dict[str, Any], ...] = tuple(copy.deepcopy(jobs))

    @property
    def route(self) -> Route:
        return Route(
            project_id=self._identity.project_id,
            shard_id=self._identity.shard_id,
            generation=self._identity.generation,
        )

    def refresh(self) -> None:
        self._session._route([], expected=self._identity, force=True)

    def complete(self, items: list[dict[str, Any]]) -> dict[str, Any]:
        return self._mutation("/api/v1/queue/complete", {"items": items})

    def fail(self, items: list[dict[str, Any]]) -> dict[str, Any]:
        return self._mutation("/api/v1/queue/fail", {"items": items})

    def extend_lease(self, extend_seconds: int, items: list[dict[str, Any]]) -> dict[str, Any]:
        return self._mutation(
            "/api/v1/queue/extend-lease",
            {"extend_seconds": extend_seconds, "items": items},
        )

    def _mutation(self, endpoint: str, fields: dict[str, Any]) -> dict[str, Any]:
        route = self._session._route([], expected=self._identity, force=False)
        payload = {**self._session._route_request(self._identity), **fields}
        try:
            return route.request(endpoint, payload)
        except (APIError, TransportError) as operation_error:
            if isinstance(operation_error, APIError) and operation_error.code not in {
                "stale_generation",
                "shard_not_active",
                "owner_lease_expired",
                "shard_unavailable",
                "invalid_access_token",
                "session_expired",
            }:
                raise
            try:
                self._session._route([], expected=self._identity, force=True)
            except RouteRetiredError:
                raise RouteRetiredError(
                    "claimed route was retired; discard this outcome"
                ) from operation_error
            except Exception:  # noqa: BLE001 - the original operation error is authoritative.
                pass
            raise


def open_session(
    config: Config,
    project_id: str,
    attrs: dict[str, Any] | None = None,
) -> Session:
    config.validate()
    if not project_id:
        raise ValueError("project_id is required")
    cloned_attrs = _loads(_dumps({} if attrs is None else attrs))
    if not isinstance(cloned_attrs, dict):
        raise ValueError("attrs must be an object")
    tracker = TrackerClient(
        config.tracker_url,
        config.machine_token,
        config.agent_id,
        timeout=config.request_timeout,
        allow_http=config.allow_http_tracker,
    )
    try:
        agent = tracker.upsert_agent(
            {
                "kind": "worker",
                "name": config.agent_name,
                "version": config.agent_version,
                "attrs": cloned_attrs,
                "endpoint": None,
                "endpoint_version": None,
                "tls_spki_sha256": None,
            }
        )
        session = tracker.create_session({"project_id": project_id, "attrs": cloned_attrs})
        try:
            agent_data = agent["agent"]
            if not isinstance(agent_data, dict) or agent_data.get("kind") != "worker":
                raise ValueError("agent is not a worker")
            return Session(
                config=config,
                tracker=tracker,
                session_id=str(session["session_id"]),
                project_id=project_id,
                attrs=cloned_attrs,
                lease_expires_at=int(session["lease_expires_at"]),
                agent_heartbeat_seconds=int(agent["heartbeat_after_seconds"]),
                session_heartbeat_seconds=int(session["heartbeat_after_seconds"]),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise TransportError(f"tracker returned invalid session metadata: {error}") from error
    except Exception:
        tracker.close()
        raise
