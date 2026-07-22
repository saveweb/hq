# SavewebHQ Python worker SDK

The SDK talks directly to one PostgreSQL-backed Project Queue. It has no
background heartbeat, shard route, generation, or inbound server.

```python
from saveweb_hq import Config, open_project_queue

with open_project_queue(
    Config(
        tracker_url="https://hq.example",
        machine_token=machine_token,
        worker_id="sinavideo-1",
        client_version="sinavideo/2.4.0",
    ),
    "sinavideo",
) as queue:
    response = queue.claim(max_jobs=64)
    for job in response["jobs"]:
        # Upload WARC to WARC Core, then include its receipt in complete().
        ...
```

`complete`, `fail`, and `extend_lease` accept bounded lists matching the
Project Queue OpenAPI contract.

With no `lease_seconds` argument, `claim` uses the project's
`recommended_lease_seconds` policy. The Python SDK does not renew leases in the
background; long-running Python workers must still call `extend_lease`.

Before claiming, the SDK periodically fetches project policy, clamps the batch
size, and applies the cooperative per-worker claim rate with monotonic timing
and a randomized initial phase. Explicit retryable HTTP 429 responses are
delayed using their precise `retry_after_ms` and retried automatically. Other
transport and API errors are returned to the caller.
