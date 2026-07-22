# SavewebHQ Project Queue API v1

All endpoints require `Authorization: Bearer <machine-token>`. The token owner
must be active and have the role required by the route: `admin` for management
and `worker` for scheduling. JSON requests reject unknown fields. All
timestamps are signed 64-bit UNIX seconds.

## Administration

Administration uses the same machine-token authentication. The token owner
must be active and have the `admin` role.

```text
GET  /api/v1/admin/projects
GET  /api/v1/admin/projects/{project_id}
PUT  /api/v1/admin/projects/{project_id}
POST /api/v1/admin/projects/{project_id}/jobs
POST /api/v1/admin/projects/{project_id}/source
GET  /api/v1/admin/projects/{project_id}/jobs
GET  /api/v1/admin/projects/{project_id}/jobs/{job_id}
POST /api/v1/admin/projects/{project_id}/jobs/{job_id}/requeue
DELETE /api/v1/admin/projects/{project_id}/jobs/{job_id}
DELETE /api/v1/admin/projects/{project_id}
GET  /api/v1/admin/users
PUT  /api/v1/admin/users/{user_id}
DELETE /api/v1/admin/users/{user_id}
GET  /api/v1/admin/users/{user_id}/machine-token
POST /api/v1/admin/users/{user_id}/machine-token
DELETE /api/v1/admin/users/{user_id}/machine-token
```

`PUT` creates a project or changes its status to `active`, `draining`, or
`archived`. Creation may set `identity_mode` to `none`, `external_id`, or
`unique_value`; omitted mode defaults to `external_id`. The mode is immutable.
`claim_order` is `fifo` or `random`, defaults to `fifo`, and may be changed at
any time. `dispatch_qps` and `worker_claim_qps` are either `null` or any positive
finite number. `max_jobs_per_claim` is 1-256 and defaults to 256. `max_resets`
is 0-1000 and defaults to 3; it limits the combined number of lease-expiration
resets and retryable failures before a job enters `reset_exhausted`.
`recommended_lease_seconds` is 1-3600 and defaults to 300. A server-owned
`client_versions` is an exact-match allowlist of up to 64 worker client version
strings. It may be empty to stop all worker access. `policy_version` starts at 1
and increases when a claim policy, reset limit, lease recommendation, or client
allowlist changes.
Project responses include these settings and `todo`, `wip`,
`done`, `failed`, and `reset_exhausted` counts.

The jobs endpoint accepts one or more JobSpecs within the 8 MiB JSON request
body limit. `value` is required; `type`, `via`, `hops`, `attr`, and the signed
32-bit `random_key` are optional. HQ generates `random_key` when it is omitted.
An explicitly supplied key is stored unchanged and ties are ordered by internal
job ID. `id` is required only by `external_id` projects and rejected by the
other modes:

- `none` inserts every submitted job;
- `external_id` deduplicates by `(project_id, id)`;
- `unique_value` deduplicates by `(project_id, SHA-256(UTF-8(value)))` and verifies the
  original value before treating a conflict as an identical job.

Identical retries in the two deduplicating modes report zero new inserts. A
matching identity with different immutable job data returns `identity_conflict`.
`random_key` applies only when a job is first inserted; an idempotent retry does
not change the stored key.

The source endpoint accepts a `jobs-jsonl-zstd-v1` body as `application/zstd`
and streams it through the same project identity rules. The compressed body is
limited to 256 MiB, expanded content to 4 GiB, and each import to 10 million
jobs.

Job listing is cursor-paginated with `after_job_id`, `limit` (1-200), and an
optional status filter. Job detail includes the immutable spec, current attempt,
terminal outcome or execution error, WARC receipts, reset count, and timestamps.
Only `failed` and `reset_exhausted` jobs can be manually requeued. WIP jobs
cannot be deleted, and projects with WIP jobs cannot be deleted.

User management creates or updates `pending`, `active`, or `suspended` users
with explicit `admin` and `worker` roles. Token rotation invalidates the old
machine token and returns the replacement; `GET` returns the current token for
repeated use. Revocation removes machine API access. Deleting a user also
deletes its machine token and browser sessions. Deleting a GitHub team
administrator is temporary while that account remains in the configured team:
its next OAuth login recreates it from the team source of truth. The initial
machine-API administrator must still be bootstrapped from the CLI because HTTP
administration itself requires an administrator token.

These endpoints also back the same-origin management UI. The UI uses a
separate HttpOnly browser session established through GitHub OAuth; machine
tokens remain the only authentication accepted by `/api/v1/**` routes.
OAuth users outside the configured administrator team are registered as
`pending` workers without a browser session. An administrator must activate the
user before it can sign in at `/worker` and generate, rotate, or revoke its own
machine token. The worker page keeps the value hidden until the user chooses
`View token`, and it remains available on later visits.

## Claim

Every worker project request, including policy reads and job mutations, must
send `X-SavewebHQ-Client-Version`. A missing version or a value absent from the
project's `client_versions` returns HTTP 426 with `client_upgrade_required` and
the current allowlist in `error.details.client_versions`. Matching is exact;
HQ does not interpret semantic-version ranges.

```text
GET  /api/v1/projects/{project_id}
POST /api/v1/projects/{project_id}/jobs/claim
```

The GET returns the current claim policy, `recommended_lease_seconds` (1-3600),
and a refresh interval. Claim request fields are `worker_id`, `max_jobs`
(1-256), `lease_seconds` (1-3600),
`accept_types`, and the fetched `policy_version`. The tracker clamps `max_jobs`
again to the project policy. A successful response contains the project ID,
claimed JobSpecs, internal numeric job IDs, unique attempt IDs, lease deadlines,
a retry delay, and the current policy version. Workers use `job_id`, not an
optional source `id`, for mutations.

The official SDKs generate a fresh seven-character `a-z0-9` `worker_id` when a
project queue is opened and reuse it for that queue instance. The tracker keeps
a best-effort worker-to-user mapping. Mapping rows may be deleted independently
for privacy or storage reclamation, after which reverse lookup is unavailable.

Claims use one PostgreSQL transaction and `FOR UPDATE SKIP LOCKED`. An expired
attempt is reset before new rows are selected. The project's `max_resets`
applies to both lease expirations and retryable failures; zero disables retries.
FIFO projects order eligible
jobs by creation time and internal job ID. Random projects order them by the
stored random key and internal job ID. Changing the project setting affects
subsequent claims without changing WIP attempts.

When `dispatch_qps` is set, the tracker uses a continuous token bucket under the
project row lock. Rates up to and including 1000 QPS have capacity one; higher
rates have capacity `min(dispatch_qps / 10, 256)`, limiting accumulated work to
100 ms and one protocol batch. This avoids cold-start bursts while retaining
practical batch claims at high throughput. If matching todo jobs exist but no
full token is available, claim
returns retryable HTTP 429 with a precise JSON `retry_after_ms` and a rounded-up
`Retry-After` header. A genuinely empty queue returns HTTP 200 with `jobs: []`.

Official SDKs refresh policy, apply `worker_claim_qps` with a monotonic clock and
a random initial phase plus positive ongoing jitter, and retry only explicit
retryable 429 responses. The tracker does not reject excess per-worker request
rates; when `worker_claim_qps` is configured, it records per-minute worker claim
buckets for later audit.

The Go SDK claims with the recommended lease unless explicitly overridden. It
returns job handles with attempt-scoped contexts and renews held attempts every
quarter lease through bounded `extend-lease` batches. A job context is canceled
when the last known lease expires, Tracker rejects the attempt, or the owning
queue closes.

## Complete

```text
POST /api/v1/projects/{project_id}/jobs/complete
```

The request contains `worker_id` and 1-256 items. Each item contains `job_id`,
`attempt_id`, a bounded outcome, and zero or more bounded WARC receipts.

A WARC receipt means that the independent WARC Core durably accepted the WARC.
It does not mean that a final sink accepted it.

## Fail

```text
POST /api/v1/projects/{project_id}/jobs/fail
```

Each item supplies an execution error and whether it is retryable. A retryable
failure increments `reset_count` and returns the job to `todo`; exceeding the
limit enters `reset_exhausted`. A non-retryable failure enters `failed`.

## Extend lease

```text
POST /api/v1/projects/{project_id}/jobs/extend-lease
```

The request contains `worker_id`, `extend_seconds`, and 1-256 job/attempt
references. The deadline never moves backwards and is maintained at
`now + extend_seconds`, rather than accumulating extensions.

## Item results

Mutations return one result per input item:

- `applied`: the current attempt was updated;
- `rejected` with `stale_attempt`: the attempt was expired, replaced, already
  finalized, or belongs to another worker.

An item-level stale attempt does not roll back other valid items in the batch.
Request-level authentication or validation errors reject the entire batch.
