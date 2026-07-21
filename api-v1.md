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
POST /api/v1/admin/users/{user_id}/machine-token
DELETE /api/v1/admin/users/{user_id}/machine-token
```

`PUT` creates a project or changes its status to `active`, `draining`, or
`archived`. Creation may set `identity_mode` to `none`, `external_id`, or
`unique_value`; omitted mode defaults to `external_id`. The mode is immutable.
Project responses include the identity mode and `todo`, `wip`, `done`, `failed`,
and `reset_exhausted` counts.

The jobs endpoint accepts 1-256 JobSpecs. `value` is required; `type`, `via`,
`hops`, and `attr` are optional. `id` is required only by `external_id` projects
and rejected by the other modes:

- `none` inserts every submitted job;
- `external_id` deduplicates by `(project_id, id)`;
- `unique_value` deduplicates by `(project_id, SHA-256(UTF-8(value)))` and verifies the
  original value before treating a conflict as an identical job.

Identical retries in the two deduplicating modes report zero new inserts. A
matching identity with different immutable job data returns `identity_conflict`.

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
machine token and returns the replacement exactly once; revocation removes
machine API access. Deleting a user also deletes its machine token and browser
sessions. Deleting a GitHub team administrator is temporary while that account
remains in the configured team: its next OAuth login recreates it from the team
source of truth. The initial machine-API administrator must still be bootstrapped
from the CLI because HTTP administration itself requires an administrator token.

These endpoints also back the same-origin management UI. The UI uses a
separate HttpOnly browser session established through GitHub OAuth; machine
tokens remain the only authentication accepted by `/api/v1/**` routes.
OAuth users outside the configured administrator team are registered as
`pending` workers without a browser session. An administrator must activate the
user and issue a machine token before it can call worker endpoints.

## Claim

```text
POST /api/v1/projects/{project_id}/jobs/claim
```

Request fields are `worker_id`, `max_jobs` (1-256), `lease_seconds` (1-3600),
and `accept_types`. A successful response contains the project ID, claimed
JobSpecs, internal numeric job IDs, unique attempt IDs, lease deadlines, and a
retry delay. Workers use `job_id`, not an optional source `id`, for mutations.

Claims use one PostgreSQL transaction and `FOR UPDATE SKIP LOCKED`. An expired
attempt is reset before new rows are selected.

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

The request contains `worker_id`, `extend_seconds`, and job/attempt references.
The deadline never moves backwards.

## Item results

Mutations return one result per input item:

- `applied`: the current attempt was updated;
- `rejected` with `stale_attempt`: the attempt was expired, replaced, already
  finalized, or belongs to another worker.

An item-level stale attempt does not roll back other valid items in the batch.
Request-level authentication or validation errors reject the entire batch.
