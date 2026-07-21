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
  --identity-mode external_id
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

Active members of the configured GitHub organization team can sign in at `/`
and manage projects, statuses, and bounded job batches. The callback verifies
team membership on every login; GitHub access tokens are not persisted.

An active administrator machine token can perform the same operations through
`/api/v1/admin/projects`. Project responses include queue counts for all job
states. Browser sessions are not accepted by machine API routes.

The administration API and Web Dashboard also import packed source files,
manage users and machine-token rotation, inspect current job state, requeue
terminal failures, and delete non-WIP jobs or projects without WIP work. New
machine tokens are displayed only in the rotation response or one-time Web
page.

## Worker API

All requests use `Authorization: Bearer <machine-token>`. Workers call:

```text
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
routing or inbound-server lifecycle.

See [design.md](./design.md) for state semantics and
[operations.md](./operations.md) for the minimal runbook.
