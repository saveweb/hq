from __future__ import annotations

from typing import Any

import saveweb_hq.project_queue as project_queue_module
from saveweb_hq import Config, ProjectQueue
from saveweb_hq.errors import APIError


class FakeTracker:
    def __init__(self, *_: object, **__: object) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.closed = False

    def project_policy(self, project_id: str) -> dict[str, Any]:
        return {
            "project_id": project_id,
            "dispatch_qps": None,
            "worker_claim_qps": None,
            "max_jobs_per_claim": 1,
            "recommended_lease_seconds": 300,
            "policy_version": 3,
            "refresh_after_ms": 60_000,
        }

    def close(self) -> None:
        self.closed = True

    def project_jobs(
        self, project_id: str, operation: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        self.calls.append((project_id, operation, payload))
        if operation == "claim":
            return {
                "project_id": project_id,
                "jobs": [],
                "retry_after_ms": 1000,
                "policy_version": 3,
            }
        return {"results": []}


def test_project_queue_builds_direct_requests(monkeypatch: Any) -> None:
    monkeypatch.setattr(project_queue_module, "TrackerClient", FakeTracker)
    monkeypatch.setattr(project_queue_module.secrets, "choice", lambda _: "a")
    queue = ProjectQueue(Config("https://hq.test", "machine-token", "worker-v2"), "project-1")

    assert queue.claim(max_jobs=2, accept_types=["seed"])["jobs"] == []
    queue.complete([{"job_id": 41, "attempt_id": "at-1"}])
    queue.extend_lease(30, [{"job_id": 41, "attempt_id": "at-1"}])

    tracker = queue._tracker
    assert isinstance(tracker, FakeTracker)
    assert tracker.calls == [
        (
            "project-1",
            "claim",
            {
                "worker_id": "aaaaaaa",
                "max_jobs": 1,
                "lease_seconds": 300,
                "accept_types": ["seed"],
                "policy_version": 3,
            },
        ),
        (
            "project-1",
            "complete",
            {
                "worker_id": "aaaaaaa",
                "items": [{"job_id": 41, "attempt_id": "at-1"}],
            },
        ),
        (
            "project-1",
            "extend-lease",
            {
                "worker_id": "aaaaaaa",
                "extend_seconds": 30,
                "items": [{"job_id": 41, "attempt_id": "at-1"}],
            },
        ),
    ]
    queue.close()
    assert tracker.closed


def test_project_queue_retries_explicit_rate_limit(monkeypatch: Any) -> None:
    class RateLimitedTracker(FakeTracker):
        def project_jobs(
            self, project_id: str, operation: str, payload: dict[str, Any]
        ) -> dict[str, Any]:
            self.calls.append((project_id, operation, payload))
            if len(self.calls) == 1:
                raise APIError(
                    429,
                    {
                        "code": "project_dispatch_rate_limited",
                        "message": "rate limited",
                        "retryable": True,
                        "retry_after_ms": 25,
                    },
                )
            return {
                "project_id": project_id,
                "jobs": [],
                "retry_after_ms": 1000,
                "policy_version": 3,
            }

    sleeps: list[float] = []
    monkeypatch.setattr(project_queue_module, "TrackerClient", RateLimitedTracker)
    monkeypatch.setattr(project_queue_module.secrets, "choice", lambda _: "b")
    monkeypatch.setattr(project_queue_module.time, "sleep", sleeps.append)
    monkeypatch.setattr(project_queue_module.random, "random", lambda: 0.0)
    queue = ProjectQueue(Config("https://hq.test", "machine-token", "worker-v2"), "project-1")

    assert queue.claim()["jobs"] == []
    assert sleeps == [0.025]
    assert len(queue._tracker.calls) == 2
