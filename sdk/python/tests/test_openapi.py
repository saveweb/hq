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
        "/healthz": {"get"},
        "/api/v1/whoami": {"get"},
        "/api/v1/admin/projects": {"get"},
        "/api/v1/admin/users": {"get"},
        "/api/v1/admin/users/{user_id}": {"put", "delete"},
        "/api/v1/admin/users/{user_id}/machine-token": {"get", "post", "delete"},
        "/api/v1/admin/projects/{project_id}": {"get", "put", "delete"},
        "/api/v1/admin/projects/{project_id}/jobs": {"get", "post"},
        "/api/v1/admin/projects/{project_id}/source": {"post"},
        "/api/v1/admin/projects/{project_id}/jobs/{job_id}": {"get", "delete"},
        "/api/v1/admin/projects/{project_id}/jobs/{job_id}/requeue": {"post"},
        "/api/v1/projects/{project_id}/jobs/claim": {"post"},
        "/api/v1/projects/{project_id}": {"get"},
        "/api/v1/projects/{project_id}/jobs/complete": {"post"},
        "/api/v1/projects/{project_id}/jobs/fail": {"post"},
        "/api/v1/projects/{project_id}/jobs/extend-lease": {"post"},
    }
    assert set(document["paths"]) == set(expected)
    for path, methods in expected.items():
        assert set(document["paths"][path]) - {"parameters"} == methods


def test_openapi_has_no_worker_inbound_endpoint() -> None:
    document = yaml.safe_load(SPEC.read_text())
    assert all("callback" not in path for path in document["paths"])


def test_all_named_timestamp_fields_are_int64() -> None:
    document = yaml.safe_load(SPEC.read_text())
    receipt = document["components"]["schemas"]["WARCReceipt"]
    assert receipt["properties"]["accepted_at"]["format"] == "int64"
