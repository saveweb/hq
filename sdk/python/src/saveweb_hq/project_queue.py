"""Direct client for the single-site PostgreSQL project queue."""

from __future__ import annotations

import copy
import math
import random
import threading
import time
from typing import Any

from .config import Config
from .errors import APIError
from .transport import TrackerClient


class ProjectQueue:
    def __init__(self, config: Config, project_id: str) -> None:
        config.validate()
        if not project_id:
            raise ValueError("project_id is required")
        self.project_id = project_id
        self.worker_id = config.worker_id
        self._tracker = TrackerClient(
            config.tracker_url,
            config.machine_token,
            config.worker_id,
            config.client_version,
            timeout=config.request_timeout,
            allow_http=config.allow_http_tracker,
        )
        self._policy: dict[str, Any] | None = None
        self._policy_fetched_at = 0.0
        self._next_claim_at = 0.0
        self._policy_lock = threading.Lock()
        self._claim_lock = threading.Lock()

    def close(self) -> None:
        self._tracker.close()

    def __enter__(self) -> ProjectQueue:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def claim(
        self,
        max_jobs: int = 64,
        lease_seconds: int = 300,
        accept_types: list[str] | None = None,
    ) -> dict[str, Any]:
        if max_jobs < 1:
            raise ValueError("max_jobs must be positive")
        while True:
            policy = self._current_policy()
            self._wait_for_claim(policy["worker_claim_qps"])
            try:
                response = self._tracker.project_jobs(
                    self.project_id,
                    "claim",
                    {
                        "worker_id": self.worker_id,
                        "max_jobs": min(max_jobs, policy["max_jobs_per_claim"]),
                        "lease_seconds": lease_seconds,
                        "accept_types": [] if accept_types is None else list(accept_types),
                        "policy_version": policy["policy_version"],
                    },
                )
            except APIError as error:
                if error.status == 429 and error.retryable and error.retry_after_ms > 0:
                    delay = min(error.retry_after_ms / 1000, threading.TIMEOUT_MAX / 1.1)
                    time.sleep(_jittered(delay))
                    continue
                raise
            if response.get("project_id") != self.project_id or not isinstance(
                response.get("jobs"), list
            ):
                raise ValueError("tracker returned a mismatched claim response")
            if response.get("policy_version") != policy["policy_version"]:
                with self._policy_lock:
                    self._policy_fetched_at = 0.0
            return copy.deepcopy(response)

    def complete(self, items: list[dict[str, Any]]) -> dict[str, Any]:
        return self._mutation("complete", items)

    def fail(self, items: list[dict[str, Any]]) -> dict[str, Any]:
        return self._mutation("fail", items)

    def extend_lease(self, extend_seconds: int, items: list[dict[str, Any]]) -> dict[str, Any]:
        return self._tracker.project_jobs(
            self.project_id,
            "extend-lease",
            {
                "worker_id": self.worker_id,
                "extend_seconds": extend_seconds,
                "items": copy.deepcopy(items),
            },
        )

    def _mutation(self, operation: str, items: list[dict[str, Any]]) -> dict[str, Any]:
        return self._tracker.project_jobs(
            self.project_id,
            operation,
            {"worker_id": self.worker_id, "items": copy.deepcopy(items)},
        )

    def _current_policy(self) -> dict[str, Any]:
        with self._policy_lock:
            now = time.monotonic()
            if self._policy is not None:
                refresh_after = self._policy["refresh_after_ms"] / 1000
                if now - self._policy_fetched_at < refresh_after:
                    return self._policy
            policy = self._tracker.project_policy(self.project_id)
            if not _valid_policy(policy, self.project_id):
                raise ValueError("tracker returned an invalid project policy")
            if (
                self._policy is not None
                and self._policy["policy_version"] != policy["policy_version"]
            ):
                with self._claim_lock:
                    self._next_claim_at = 0.0
            self._policy = copy.deepcopy(policy)
            self._policy_fetched_at = time.monotonic()
            return self._policy

    def _wait_for_claim(self, qps: float | None) -> None:
        if qps is None:
            return
        interval = min(1 / qps, threading.TIMEOUT_MAX)
        with self._claim_lock:
            now = time.monotonic()
            if self._next_claim_at == 0.0:
                scheduled = now + random.random() * interval
            else:
                scheduled = max(now, self._next_claim_at) + random.random() * interval / 10
            self._next_claim_at = scheduled + interval
        delay = scheduled - time.monotonic()
        if delay > 0:
            time.sleep(delay)


def _valid_policy(policy: dict[str, Any], project_id: str) -> bool:
    dispatch_qps = policy.get("dispatch_qps")
    worker_claim_qps = policy.get("worker_claim_qps")
    return (
        policy.get("project_id") == project_id
        and isinstance(policy.get("max_jobs_per_claim"), int)
        and 1 <= policy["max_jobs_per_claim"] <= 256
        and isinstance(policy.get("policy_version"), int)
        and policy["policy_version"] >= 1
        and isinstance(policy.get("refresh_after_ms"), int)
        and policy["refresh_after_ms"] >= 1
        and _valid_qps(dispatch_qps)
        and _valid_qps(worker_claim_qps)
    )


def _valid_qps(value: object) -> bool:
    return value is None or (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


def _jittered(delay: float) -> float:
    return delay + random.random() * delay / 10


def open_project_queue(config: Config, project_id: str) -> ProjectQueue:
    return ProjectQueue(config, project_id)
