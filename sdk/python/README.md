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
    ),
    "sinavideo",
) as queue:
    response = queue.claim(max_jobs=64, lease_seconds=300)
    for job in response["jobs"]:
        # Upload WARC to WARC Core, then include its receipt in complete().
        ...
```

`complete`, `fail`, and `extend_lease` accept bounded lists matching the
Project Queue OpenAPI contract.
