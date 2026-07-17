from __future__ import annotations

import argparse
from pathlib import Path

from saveweb_hq import Config, open_session


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tracker-url", required=True)
    parser.add_argument("--machine-token-file", type=Path, required=True)
    args = parser.parse_args()
    token = args.machine_token_file.read_text(encoding="utf-8").strip()
    with open_session(
        Config(
            tracker_url=args.tracker_url,
            machine_token=token,
            agent_id="worker-python-e2e",
            agent_name="Python E2E worker",
            agent_version="e2e",
            allow_http_tracker=True,
            request_timeout=10,
        ),
        "project-e2e",
        {"sdk": "python"},
    ) as session:
        receiver = session.submit_receiver(
            "receiver-e2e",
            [
                {
                    "id": "stage2-python",
                    "url": "https://example.test/stage-2/python",
                    "type": "seed",
                    "attr": {"discovered_by": "python"},
                }
            ],
        )
        assert receiver["project_id"] == "project-e2e"
        assert receiver["jobs_count"] == 1

        batch = session.claim(max_jobs=1, lease_seconds=60, accept_types=["seed"])
        assert batch.route.generation == 1
        assert len(batch.jobs) == 1 and batch.jobs[0]["id"] == "b-python"
        job = batch.jobs[0]
        result = batch.complete(
            [
                {
                    "job_id": job["id"],
                    "attempt_id": job["attempt_id"],
                    "outcome": {
                        "kind": "success",
                        "code": None,
                        "uri": None,
                        "meta": {"worker": "python"},
                    },
                    "discovered_jobs": [
                        {
                            "id": "d-python-done",
                            "url": "https://example.test/python-discovered",
                            "type": "seed",
                            "via": job["url"],
                            "hops": 1,
                            "attr": {"discovered_by": "python"},
                        }
                    ],
                }
            ]
        )
        assert result["results"][0]["status"] == "applied"


if __name__ == "__main__":
    main()
