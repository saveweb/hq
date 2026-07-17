from __future__ import annotations

import json
from pathlib import Path

from saveweb_hq import default_job_id


ROOT = Path(__file__).parents[3]


def test_default_job_id_conformance() -> None:
    vectors = json.loads((ROOT / "api/testdata/job-id-v1.json").read_text())
    for vector in vectors:
        assert default_job_id(vector["type"], vector["url"]) == vector["id"]


def test_empty_type_defaults_to_seed() -> None:
    url = "https://example.org/"
    assert default_job_id("", url) == default_job_id("seed", url)
