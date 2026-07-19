"""Direct client for the single-site PostgreSQL project queue."""

from __future__ import annotations

import copy
from typing import Any

from .config import Config
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
            timeout=config.request_timeout,
            allow_http=config.allow_http_tracker,
        )

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
        response = self._tracker.project_jobs(
            self.project_id,
            "claim",
            {
                "worker_id": self.worker_id,
                "max_jobs": max_jobs,
                "lease_seconds": lease_seconds,
                "accept_types": [] if accept_types is None else list(accept_types),
            },
        )
        if response.get("project_id") != self.project_id or not isinstance(
            response.get("jobs"), list
        ):
            raise ValueError("tracker returned a mismatched claim response")
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


def open_project_queue(config: Config, project_id: str) -> ProjectQueue:
    return ProjectQueue(config, project_id)
