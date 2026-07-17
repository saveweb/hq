from __future__ import annotations

from pathlib import Path

import yaml
from openapi_spec_validator import validate


ROOT = Path(__file__).parents[3]
SPEC = ROOT / "api/openapi-v1.yaml"


def test_openapi_contract_has_all_v1_operations() -> None:
    document = yaml.safe_load(SPEC.read_text())
    validate(document)
    assert document["openapi"] == "3.1.0"

    expected = {
        "/healthz": "get",
        "/api/v1/agents/{agent_id}": "put",
        "/api/v1/agents/{agent_id}/heartbeat": "post",
        "/api/v1/shards/{project_id}/{shard_id}/load-result": "post",
        "/api/v1/shards/{project_id}/{shard_id}/recovery-result": "post",
        "/api/v1/shards/{project_id}/{shard_id}/checkpoints": "post",
        "/api/v1/shards/{project_id}/{shard_id}/checkpoints/{upload_id}/parts": "post",
        "/api/v1/shards/{project_id}/{shard_id}/checkpoints/{upload_id}/complete": "post",
        "/api/v1/shards/{project_id}/{shard_id}/checkpoints/{upload_id}/abort": "post",
        "/api/v1/worker/sessions": "post",
        "/api/v1/worker/sessions/{session_id}/heartbeat": "post",
        "/api/v1/worker/assignments": "post",
        "/api/v1/shard/endpoint-challenge": "post",
        "/api/v1/queue/claim": "post",
        "/api/v1/queue/complete": "post",
        "/api/v1/queue/fail": "post",
        "/api/v1/queue/extend-lease": "post",
    }
    assert set(document["paths"]) == set(expected)
    for path, method in expected.items():
        assert set(document["paths"][path]) - {"parameters"} == {method}


def test_openapi_has_no_worker_inbound_endpoint() -> None:
    document = yaml.safe_load(SPEC.read_text())
    assert all("callback" not in path for path in document["paths"])


def test_all_named_timestamp_schemas_are_int64() -> None:
    document = yaml.safe_load(SPEC.read_text())
    unix_time = document["components"]["schemas"]["UnixTime"]
    assert unix_time == {"type": "integer", "format": "int64", "minimum": 0}
