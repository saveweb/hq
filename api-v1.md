# SavewebHQ Project Queue API v1

All endpoints require `Authorization: Bearer <machine-token>`. The token owner
must be active and have the `worker` role. JSON requests reject unknown fields.
All timestamps are signed 64-bit UNIX seconds.

## Claim

```text
POST /api/v1/projects/{project_id}/jobs/claim
```

Request fields are `worker_id`, `max_jobs` (1-256), `lease_seconds` (1-3600),
and `accept_types`. A successful response contains the project ID, claimed
JobSpecs, unique attempt IDs, lease deadlines, and a retry delay.

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
