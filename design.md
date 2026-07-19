# SavewebHQ design

## 1. Purpose

HQ coordinates bounded archive jobs at one Saveweb-operated site. PostgreSQL
is both the control database and job queue. Horizontal volunteer-owned queue
shards are outside the design.

The design optimizes for understandable failure recovery rather than maximum
theoretical throughput. Capacity changes require production evidence.

## 2. Ownership

HQ owns:

- projects and their active/draining/archived lifecycle;
- immutable job identity and work specification;
- claims, attempt IDs, leases, retry counts, and terminal outcomes;
- WARC receipts supplied by workers.

HQ does not own:

- WARC bytes or upload sessions;
- WARC validation, packing, retention, or garbage collection;
- MegaWARC creation;
- Internet Archive or other sink delivery state;
- worker process supervision;
- queue shards, owners, routing, generations, or checkpoints.

## 3. Data model

A job is identified by `(project_id, job_id)`. Reimporting the identical spec
is idempotent. Reusing an ID for a different spec is `identity_conflict`.

```text
todo -> wip -> done
          |  -> failed
          |  -> todo                 retryable failure
          +  -> reset_exhausted      retry or lease limit exceeded
```

`claim` selects `todo` rows with `FOR UPDATE SKIP LOCKED`, assigns a random
attempt ID, records the worker ID, and sets a lease deadline in one PostgreSQL
transaction.

`complete`, `fail`, and `extend-lease` require the current project, job ID,
attempt ID, worker ID, non-expired lease, and `wip` status. A stale mutation is
rejected per item and cannot affect a later attempt.

Expired WIP rows are reset during a later claim. The first version uses a
global maximum of three resets. This may become a project setting only after a
real project demonstrates that need.

## 4. WARC receipt contract

WARC Core returns a signed receipt only after it has validated and durably
accepted a WARC object. HQ stores the receipt as part of job completion.

A receipt contains:

- stable receipt ID and issuer;
- WARC Core object ID;
- SHA-256 and size;
- acceptance time;
- signature.

Receipts are bounded in count and encoded size. HQ stores but does not itself
verify a particular WARC Core signature scheme until that external protocol is
frozen. Deployment policy must configure workers to trust the intended WARC
Core; signature verification belongs in the shared receipt library once that
project exists.

Receipt acceptance is the job/file boundary:

```text
worker has WARC
  -> WARC Core accepted and issued receipt
  -> worker completes HQ attempt with receipt
  -> WARC Core independently processes and delivers the file
```

Later WARC processing or sink failure never reopens an HQ job. WARC Core owns
its durable retry queue and operator-visible delivery state.

## 5. Authentication

The first deployment uses administrator-created machine tokens. A token maps
to an active user with the `worker` role. `worker_id` identifies a process for
attempt ownership and logs; it is not a separately leased agent or routing
identity.

GitHub OAuth, team synchronization, shard-owner roles, agent registration, and
session heartbeat are not required by the production queue API.

## 6. Source import

`jobs-jsonl-zstd-v1` remains the immutable interchange format. The operator
streams it directly into PostgreSQL using `tracker enqueue-source`; HQ does not
download sources from object storage.

Imports are transactional in bounded batches. A large source may be partially
imported if a later batch is invalid; rerunning it is safe because prior
identical jobs are idempotent.

## 7. PostgreSQL operations

PostgreSQL is intentionally the only HQ state store. Required operational
controls are ordinary database backups, WAL retention appropriate to the
deployment, connection monitoring, and restore exercises.

WARC bytes, HTTP bodies, and unbounded logs must never be stored in HQ.

No table partitioning is required initially. Add it only after measurements
show that completed-job history or vacuum behavior is a real problem.

## 8. Explicitly excluded

- volunteer-owned public shard endpoints;
- SQLite queue files in production;
- owner leases and generation fencing;
- checkpoint publication and takeover recovery;
- tracker-issued shard access tokens;
- R2 source loading and receiver objects;
- computed routing and online resharding;
- automatic multi-stage pipelines;
- final-sink state in job status;
- a 100,000 completed-jobs/s promise without production evidence.
