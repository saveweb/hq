# SavewebHQ Python worker SDK

The synchronous Python worker SDK for SavewebHQ. The package is managed and
tested exclusively with `uv`.

```python
from saveweb_hq import ClaimsPausedError, Config, RouteRetiredError, open_session

with open_session(
    Config(
        tracker_url="https://tracker.example",
        machine_token=machine_token,
        agent_id=worker_agent_id,
    ),
    "project-1",
) as session:
    batch = session.claim(max_jobs=64, lease_seconds=300, accept_types=["seed"])
    for job in batch.jobs:
        ...
```

Use `batch.complete()`, `batch.fail()`, and `batch.extend_lease()` with protocol
dictionaries. A batch remains bound to the shard owner and generation that
claimed it. If tracker reports a takeover, mutation methods raise
`RouteRetiredError`; discard the local outcome instead of replaying it.

Worker hosts can inspect `session.runtime_status()` and call
`session.set_claims_paused(True)` from their loopback administration surface.
While paused, new `claim()` calls raise `ClaimsPausedError`; heartbeat and
existing `Batch.complete()`, `fail()`, and `extend_lease()` operations continue.
The Python SDK deliberately exposes control hooks instead of selecting a Python
web framework; the bundled Go local HTTP servers use Echo.

Development commands always use `uv`:

```bash
uv sync --project sdk/python --dev
uv run --project sdk/python pytest
uv run --project sdk/python ruff check sdk/python
```
