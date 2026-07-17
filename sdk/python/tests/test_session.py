from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from saveweb_hq import Config, RouteRetiredError, open_session


class State:
    generation = 1
    shard_url = ""


def send_json(handler: BaseHTTPRequestHandler, status: int, value: dict[str, Any]) -> None:
    encoded = json.dumps(value, separators=(",", ":")).encode()
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(encoded)))
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Cloudflare-CDN-Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(encoded)


class TrackerHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_: object) -> None:
        pass

    def do_PUT(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API.
        self.rfile.read(int(self.headers.get("Content-Length", "0")))
        assert self.headers["Authorization"] == "Bearer worker-token"
        assert self.headers["X-Saveweb-Agent-ID"] == "worker-agent"
        send_json(
            self,
            200,
            {
                "agent": {"id": "worker-agent", "kind": "worker"},
                "heartbeat_after_seconds": 30,
                "server_time": int(time.time()),
            },
        )

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API.
        request_body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        if self.path == "/api/v1/worker/sessions":
            send_json(
                self,
                201,
                {
                    "session_id": "session-1",
                    "lease_expires_at": int(time.time()) + 120,
                    "heartbeat_after_seconds": 30,
                },
            )
            return
        if self.path == "/api/v1/worker/receivers/receiver-1/batches":
            request = json.loads(request_body)
            assert request == {
                "session_id": "session-1",
                "jobs": [
                    {
                        "id": "discovered-1",
                        "url": "https://example.test/discovered",
                    }
                ],
            }
            send_json(
                self,
                201,
                {
                    "project_id": "project-1",
                    "receiver_id": "receiver-1",
                    "object_uri": "s3://receiver-output/stage-1/object.jobs.jsonl.zst",
                    "format": "jobs-jsonl-zstd-v1",
                    "jobs_count": 1,
                    "size_bytes": 99,
                    "sha256": "a" * 64,
                    "created_at": int(time.time()),
                },
            )
            return
        if self.path == "/api/v1/worker/assignments":
            send_json(
                self,
                200,
                {
                    "assignment": {
                        "project_id": "project-1",
                        "shard_id": "shard-a",
                        "generation": State.generation,
                        "owner_agent_id": "shard-agent",
                        "endpoint": State.shard_url,
                        "endpoint_version": 1,
                        "tls_spki_sha256": None,
                        "access_token": f"token-generation-{State.generation}",
                        "access_token_expires_at": int(time.time()) + 600,
                    },
                    "retry_after_ms": 0,
                },
            )
            return
        if self.path.endswith("/heartbeat"):
            if "/worker/sessions/" in self.path:
                send_json(
                    self,
                    200,
                    {
                        "session_id": "session-1",
                        "lease_expires_at": int(time.time()) + 120,
                        "heartbeat_after_seconds": 30,
                    },
                )
            else:
                send_json(
                    self,
                    200,
                    {
                        "server_time": int(time.time()),
                        "heartbeat_after_seconds": 30,
                        "owner_assignments": [],
                        "signing_keys": [],
                    },
                )
            return
        send_json(self, 404, {"error": api_error("not_found", "not found")})


class ShardHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_: object) -> None:
        pass

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API.
        assert "no-store" in self.headers["Cache-Control"]
        length = int(self.headers.get("Content-Length", "0"))
        request = json.loads(self.rfile.read(length))
        if self.path == "/api/v1/queue/claim":
            send_json(
                self,
                200,
                {
                    "project_id": "project-1",
                    "shard_id": "shard-a",
                    "generation": State.generation,
                    "jobs": [
                        {
                            "id": "job-1",
                            "url": "https://example.test/",
                            "type": "seed",
                            "via": None,
                            "hops": 0,
                            "attr": {},
                            "attempt_id": "attempt-1",
                            "lease_expires_at": int(time.time()) + 60,
                        }
                    ],
                    "retry_after_ms": 0,
                },
            )
            return
        if request["generation"] != State.generation:
            send_json(self, 409, {"error": api_error("stale_generation", "generation changed")})
            return
        send_json(
            self,
            200,
            {
                "results": [
                    {
                        "job_id": request["items"][0]["job_id"],
                        "attempt_id": request["items"][0]["attempt_id"],
                        "status": "applied",
                        "job_status": "done",
                        "lease_expires_at": None,
                        "error": None,
                    }
                ]
            },
        )


def api_error(code: str, message: str) -> dict[str, Any]:
    return {
        "code": code,
        "message": message,
        "retryable": False,
        "retry_after_ms": 0,
        "details": {},
    }


def start_server(
    handler: type[BaseHTTPRequestHandler],
) -> tuple[ThreadingHTTPServer, threading.Thread]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def test_python_sdk_flow_and_generation_retirement() -> None:
    State.generation = 1
    shard, shard_thread = start_server(ShardHandler)
    tracker, tracker_thread = start_server(TrackerHandler)
    State.shard_url = f"http://127.0.0.1:{shard.server_port}"
    try:
        with open_session(
            Config(
                tracker_url=f"http://127.0.0.1:{tracker.server_port}",
                machine_token="worker-token",
                agent_id="worker-agent",
                allow_http_tracker=True,
                allow_http_shard=True,
                request_timeout=5,
            ),
            "project-1",
            {"sdk": "python-test"},
        ) as session:
            receiver = session.submit_receiver(
                "receiver-1",
                [{"id": "discovered-1", "url": "https://example.test/discovered"}],
            )
            assert receiver["jobs_count"] == 1
            assert receiver["receiver_id"] == "receiver-1"

            batch = session.claim(max_jobs=1, lease_seconds=60, accept_types=["seed"])
            assert batch.route.generation == 1
            assert batch.jobs[0]["id"] == "job-1"
            result = batch.complete(
                [
                    {
                        "job_id": "job-1",
                        "attempt_id": "attempt-1",
                        "outcome": {"kind": "success", "code": None, "uri": None, "meta": {}},
                        "discovered_jobs": [],
                    }
                ]
            )
            assert result["results"][0]["status"] == "applied"

            second_batch = session.claim(max_jobs=1, lease_seconds=60)
            State.generation = 2
            with pytest.raises(RouteRetiredError):
                second_batch.complete(
                    [
                        {
                            "job_id": "job-1",
                            "attempt_id": "attempt-1",
                            "outcome": {
                                "kind": "success",
                                "code": None,
                                "uri": None,
                                "meta": {},
                            },
                            "discovered_jobs": [],
                        }
                    ]
                )
    finally:
        tracker.shutdown()
        shard.shutdown()
        tracker.server_close()
        shard.server_close()
        tracker_thread.join()
        shard_thread.join()
