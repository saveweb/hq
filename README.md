# SavewebHQ

SavewebHQ is a single-site PostgreSQL job coordinator for Saveweb archive
workers. HQ owns projects, job attempts, leases, outcomes, and bounded WARC
receipts. It does not receive WARC files, operate volunteer queue shards, or
track delivery to Internet Archive.

## Boundary

```text
source -> HQ/PostgreSQL -> worker -> WARC Core -> sink adapters
                ^            |
                +-- receipt --+
```

- HQ decides whether a job is `todo`, `wip`, `done`, `failed`, or
  `reset_exhausted`.
- WARC Core validates and durably accepts WARC files, then issues signed
  receipts.
- A receipt proves that WARC Core accepted responsibility for an object. It
  does not prove delivery to Internet Archive or another final sink.
- `go2internetarchive` is expected to remain an IA client library used by WARC
  Core, not an HQ component or WARC receiver.

## Components

The production image contains only:

- `tracker`: migrations, GitHub-authenticated web administration, machine API,
  and source import;
- `hqctl`: remote administration CLI with bounded batch and packed-source
  enqueue commands;
- `source`: tools for producing `jobs-jsonl-zstd-v1` source files.

PostgreSQL is the only HQ state store. There is no shard daemon, queue relay,
R2 credential, checkpoint protocol, owner generation, or route assignment in
the production service.

## Local development

Prerequisites are Go 1.25 or newer, `uv` 0.9 or newer, and Docker for the
PostgreSQL integration test.

```bash
make test
make check
make test-postgres
```

## Deployment

The included Compose file starts PostgreSQL and HQ. PostgreSQL data is stored
under `./data/postgres`. Before the first start, create a GitHub OAuth App with
this callback URL:

```text
https://hq.saveweb.org/auth/github/callback
```

Set its client ID in `.env`, and put its client secret in the ignored secrets
directory. Replace the public URL and team when deploying under another host
or organization.

```bash
cp .env.example .env
install -d -m 0700 secrets
printf '%s\n' 'GITHUB_OAUTH_CLIENT_SECRET' > secrets/github-client.secret
chmod 0600 secrets/github-client.secret
docker compose build
docker compose up -d
docker compose ps
```

HQ runs migrations before serving and listens on `127.0.0.74:8080` for a host
HTTPS reverse proxy. PostgreSQL is not published to the host.

## Bootstrap

Create a private machine-token file, then create a worker and a project:

```bash
tracker bootstrap-user \
  --database-url "$HQ_DATABASE_URL" \
  --user-id worker-1 \
  --roles worker \
  --machine-token-file ./worker.token

tracker put-project \
  --database-url "$HQ_DATABASE_URL" \
  --project-id sinavideo \
  --identity-mode external_id \
  --dispatch-qps 250 \
  --worker-claim-qps 0.2 \
  --max-jobs-per-claim 64
```

Pack URLs and import them directly into the project queue:

```bash
source pack --input urls.txt --output sinavideo.jobs.jsonl.zst

tracker enqueue-source \
  --database-url "$HQ_DATABASE_URL" \
  --project-id sinavideo \
  --input sinavideo.jobs.jsonl.zst
```

Every job receives a compact internal PostgreSQL `bigint` ID used by workers.
Project identity mode controls enqueue deduplication:

- `none` accepts repeated values and requires no external `id`;
- `external_id` requires `id` and makes it unique within the project;
- `unique_value` requires no `id` and makes `value` unique within the project.

An identity mode is fixed when the project is created. Reimporting an identical
job into `external_id` or `unique_value` is idempotent; reusing the identity with
different immutable job data returns `identity_conflict`.

Each project also has a live `claim_order` setting. `fifo` is the default and
orders jobs by creation time; `random` uses a stored signed 32-bit random key.
HQ generates the key during enqueue unless the administrator supplies
`random_key`. Equal keys are resolved by internal job ID, so custom keys do not
need to be unique. Switching the setting affects later claims and leaves WIP
attempts unchanged. For deduplicated jobs, a retry never replaces the stored
key.

Projects may also set a tracker-enforced `dispatch_qps`, an SDK-enforced
per-worker `worker_claim_qps`, and `max_jobs_per_claim` (1-256). Omit either QPS
to leave that limit disabled; this preserves the unlimited fast path. At high
dispatch rates the tracker permits at most 100 ms of accumulated tokens, while
lower rates permit no burst larger than one job.

Active members of the configured GitHub organization team can sign in at `/`
and manage projects, statuses, and bounded job batches. The callback verifies
team membership on every login; GitHub access tokens are not persisted. Other
GitHub users are registered as pending workers and receive no administration
session. After an administrator activates the account, the worker signs in
again and generates its own machine token at `/worker`.

An active administrator machine token can perform the same operations through
`/api/v1/admin/projects`. Project responses include queue counts for all job
states. Browser sessions are not accepted by machine API routes.

## Remote enqueue CLI

Build `hqctl`, then store an active administrator's machine token in a private
file. `HQ_URL` defaults to `https://hq.saveweb.org`, and
`HQ_MACHINE_TOKEN_FILE` can replace the corresponding flags.

```bash
install -d ./bin
go build -o ./bin/hqctl ./cmd/hqctl
chmod 0600 ./admin.token
```

Batch enqueue reads one exact value per non-empty line. It fetches the project
first, generates stable IDs for `external_id` projects, and sends sequential
requests using the selected batch size (default 1000). There is no job-count
cap per request; the server's 8 MiB JSON body limit is the effective boundary.
The final JSON reports submitted and newly inserted totals; progress is written
to stderr every 100 batches by default.

```bash
jq -r '.vid' /home/yzqzss/git/sinavideo/records.jsonl |
  ./bin/hqctl enqueue \
    --machine-token-file ./admin.token \
    --project-id sina_bilivideo \
    --batch-size 5000
```

Use `--format jsonl` for one complete enqueue job per line. It may include a
signed 32-bit `random_key`; HQ generates one when omitted. A missing `id` is filled
for `external_id` projects; `id` is rejected locally for `unique_value` and
`none` projects.

```bash
./bin/hqctl enqueue \
  --machine-token-file ./admin.token \
  --project-id sina_bilivideo \
  --format jsonl \
  --input jobs.jsonl \
  --batch-size 128
```

For very large imports, pack once and stream the compressed source through one
request:

```bash
./bin/hqctl enqueue-source \
  --machine-token-file ./admin.token \
  --project-id sina_bilivideo \
  --input sina_bilivideo.jobs.jsonl.zst
```

Batch enqueue is intentionally sequential. If a later batch fails, earlier
batches remain committed and the error reports how many jobs were submitted.
Rerunning is idempotent for `external_id` and `unique_value`; a `none` project
will insert another copy.

The administration API and Web Dashboard also import packed source files,
manage user creation, deletion, and machine-token rotation, inspect current job
state, requeue terminal failures, and delete non-WIP jobs or projects without
WIP work. New machine tokens are displayed only in the rotation response or
the Web Dashboard. Active workers can view, generate, rotate, or revoke their
own machine token from the OAuth-authenticated worker page.

## Worker API

All requests use `Authorization: Bearer <machine-token>`. Workers call:

```text
GET  /api/v1/projects/{project_id}
POST /api/v1/projects/{project_id}/jobs/claim
POST /api/v1/projects/{project_id}/jobs/complete
POST /api/v1/projects/{project_id}/jobs/fail
POST /api/v1/projects/{project_id}/jobs/extend-lease
```

Claims and mutations are bounded batches. A claim returns the internal numeric
`job_id` and a unique `attempt_id`; late or replayed outcomes cannot finalize a
newer attempt. An optional source `id` is metadata and is never used to finish
an attempt.

The Go SDK exposes `worker.OpenProjectQueue`; the Python SDK exposes
`open_project_queue`. Both call the project queue directly and contain no
routing or inbound-server lifecycle. They refresh project policy, clamp claim
batches, pace each worker's claims, and retry explicit retryable 429 responses.

See [design.md](./design.md) for state semantics and
[operations.md](./operations.md) for the minimal runbook.
