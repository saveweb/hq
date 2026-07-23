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
- artifact receipts supplied by workers.

Each project publishes a recommended lease from one second through one hour.
It is a worker policy rather than a server-enforced fixed lease: raw clients may
request another valid duration, while official SDKs use the recommendation by
default. Changing it increments the project policy version.

HQ does not own:

- artifact contents or upload sessions;
- artifact validation, packing, retention, or garbage collection;
- MegaWARC creation;
- Internet Archive or other sink delivery state;
- worker process supervision;
- queue shards, owners, routing, generations, or checkpoints.

## 3. Data model

Every job has an internal PostgreSQL-generated `bigint` `job_id`. Worker
mutations address `(project_id, job_id)` and the current attempt. Source identity
is separate and selected once when a project is created:

- `none` stores no deduplication key;
- `external_id` stores an optional-format source ID and enforces
  `(project_id, external_id)` uniqueness;
- `unique_value` stores a binary SHA-256 digest of the UTF-8 value and enforces
  `(project_id, unique_value_digest)` uniqueness.

`external_id` projects require a JobSpec `id`; the other modes reject it. A
digest conflict is checked against the original value so a hash collision is
reported rather than silently deduplicated. The project identity mode is
immutable, which prevents mixed identity rules inside one queue.

`value` is stored in its own column. PostgreSQL JSONB stores only the optional
`type`, `via`, `hops`, and `attr` fields, so neither internal nor external IDs
are duplicated inside the work specification.

```text
todo -> wip -> done
          |  -> failed
          |  -> todo                 retryable failure
          +  -> reset_exhausted      retry or lease limit exceeded
```

Every job receives a signed 32-bit random key during enqueue. Administrators
may provide it for deterministic ordering; otherwise HQ generates it. A
project's live `claim_order` setting chooses FIFO ordering by creation time and
job ID, or random ordering by random key and job ID. Both paths use partial
indexes.

`claim` selects `todo` rows in the configured order with `FOR UPDATE SKIP
LOCKED`, assigns a random attempt ID, records the worker ID, and sets a lease
deadline in one PostgreSQL transaction. A setting change affects later claims
but does not alter WIP attempts.

Each project has five live execution-policy values. `dispatch_qps` is a hard,
tracker-owned limit on jobs dispatched across all workers. `worker_claim_qps`
is a cooperative per-worker request rate applied by official SDKs.
`max_jobs_per_claim` is enforced by both SDK and tracker. Null QPS values mean
unlimited; the dispatch fast path then avoids the project row lock entirely.
`max_resets` is a tracker-owned 0-1000 limit shared by lease-expiration resets
and retryable failures; it defaults to three, and zero disables retries.
`recommended_lease_seconds` is the default claim and renewal duration used by
official SDKs; it defaults to 300 seconds.
`policy_version` increments when one of these values changes.

The hard dispatch limiter is a continuous token bucket, not a fixed time
window. Capacity is one job through 1000 QPS. Above 1000 QPS it is the smaller
of 100 ms of work (`qps / 10` tokens) and the 256-job protocol batch limit.
Idle time cannot accumulate a larger burst. A claim that finds matching todo
but lacks a complete token receives retryable 429; no matching todo remains a
normal empty response. Per-minute `(project, worker)` counters retain the
client-reported policy version when `worker_claim_qps` is configured, so
cooperative-limit violations can be audited without adding writes to unlimited
projects or putting that check on the hot rejection path.

`complete`, `fail`, and `extend-lease` require the current project, internal job ID,
attempt ID, worker ID, non-expired lease, and `wip` status. A stale mutation is
rejected per item and cannot affect a later attempt.

Expired WIP rows are reset during a later claim. Each project's `max_resets`
setting limits the combined number of lease-expiration resets and retryable
failures before the job enters `reset_exhausted`.

## 4. Artifact receipt contract

The external Artifact Receiver returns a receipt only after it has
validated and durably accepted an artifact. HQ stores the receipt as part of job
completion and never receives the artifact contents.

A receipt contains:

- stable receipt ID and issuer;
- Artifact Receiver object ID;
- self-describing content checksum and size;
- acceptance time.

Receipts are bounded in count and encoded size. HQ treats them as worker-reported
acceptance metadata; it does not verify receipt authenticity.

Receipt acceptance is the job/file boundary:

```text
worker produces artifact
  -> Artifact Receiver accepted and issued receipt
  -> worker completes HQ attempt with receipt
  -> Artifact Receiver independently processes and delivers the artifact
```

Later artifact processing or sink failure never reopens an HQ job. The Artifact
Receiver owns its durable retry queue and operator-visible delivery state.

## 5. Authentication

Workers and automation use administrator-created machine tokens. A token maps
to an active user with an explicit `worker` or `admin` role. Each SDK queue
initialization generates a random seven-character `a-z0-9` `worker_id` for
attempt ownership and logs; it is not a separately leased agent or routing
identity. The tracker keeps a best-effort many-workers-to-one-user mapping.
Operators may delete mapping rows for privacy or storage reclamation; doing so
only makes reverse lookup unavailable and does not affect queue correctness.
The admin Web UI can search these mappings, and WIP job details link their
worker ID to the matching relation when it is still available.

Human administrators use GitHub OAuth with state and PKCE. HQ requests
`read:org`, fetches the GitHub identity, and verifies active membership in one
configured organization team at every login. The GitHub access token is then
discarded. Team members are synchronized to active administrators. Other GitHub
users are registered as pending workers without receiving a browser session;
an administrator must activate them. Once active, a worker can establish its own
browser session and generate, rotate, or revoke its own machine token. HQ stores
only a hash of a random, short-lived browser session token and requires a
derived CSRF token for every state-changing web request.

The web UI and machine API share project/job services but not credentials:
browser sessions never authorize `/api/v1/**`, and machine tokens are never
placed in browser storage. Shard-owner roles, agent registration, and session
heartbeat remain outside the production queue API.

Administrators can manage user status and roles, delete users, rotate or revoke
machine tokens, inspect current job state, requeue terminal failures, and delete
jobs or projects that have no active attempt. HQ stores each machine token in
plain text for repeated display and stores its digest for indexed
authentication. HQ does not synthesize attempt history: the administration API
exposes the current row and its terminal data.

## 6. Source import

`jobs-jsonl-zstd-v1` is the immutable interchange format. The operator
streams it directly into PostgreSQL using `tracker enqueue-source`; HQ does not
download sources from object storage.

Imports are transactional in bounded batches. A large source may be partially
imported if a later batch is invalid. Rerunning is idempotent for `external_id`
and `unique_value` projects; `none` intentionally inserts another copy.

## 7. PostgreSQL operations

PostgreSQL is intentionally the only HQ state store. Required operational
controls are ordinary database backups, WAL retention appropriate to the
deployment, connection monitoring, and restore exercises.

Artifact contents, HTTP bodies, and unbounded logs must never be stored in HQ.

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
