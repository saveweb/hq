from __future__ import annotations

from typing import Any

import saveweb_hq.project_queue as project_queue_module
from saveweb_hq import Config, ProjectQueue


class FakeTracker:
    def __init__(self, *_: object, **__: object) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def project_jobs(
        self, project_id: str, operation: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        self.calls.append((project_id, operation, payload))
        if operation == "claim":
            return {"project_id": project_id, "jobs": [], "retry_after_ms": 1000}
        return {"results": []}


def test_project_queue_builds_direct_requests(monkeypatch: Any) -> None:
    monkeypatch.setattr(project_queue_module, "TrackerClient", FakeTracker)
    queue = ProjectQueue(Config("https://hq.test", "machine-token", "worker-1"), "project-1")

    assert queue.claim(max_jobs=2, lease_seconds=60, accept_types=["seed"])["jobs"] == []
    queue.complete([{"job_id": 41, "attempt_id": "at-1"}])
    queue.extend_lease(30, [{"job_id": 41, "attempt_id": "at-1"}])

    tracker = queue._tracker
    assert isinstance(tracker, FakeTracker)
    assert tracker.calls == [
        (
            "project-1",
            "claim",
            {
                "worker_id": "worker-1",
                "max_jobs": 2,
                "lease_seconds": 60,
                "accept_types": ["seed"],
            },
        ),
        (
            "project-1",
            "complete",
            {
                "worker_id": "worker-1",
                "items": [{"job_id": 41, "attempt_id": "at-1"}],
            },
        ),
        (
            "project-1",
            "extend-lease",
            {
                "worker_id": "worker-1",
                "extend_seconds": 30,
                "items": [{"job_id": 41, "attempt_id": "at-1"}],
            },
        ),
    ]
    queue.close()
    assert tracker.closed
