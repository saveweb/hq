# SavewebHQ operations

## Services

HQ requires PostgreSQL and an HTTPS reverse proxy. It has no object-storage
credentials and does not require a public endpoint for any worker. The web
administrator does require a public HTTPS URL and a GitHub OAuth App.

Before first startup:

1. Configure the OAuth App callback as
   `https://hq.saveweb.org/auth/github/callback`.
2. Set `HQ_GITHUB_CLIENT_ID` and `HQ_PUBLIC_URL` in `.env`.
3. Put the OAuth client secret in `secrets/github-client.secret` with mode
   `0600`.
4. Set `HQ_OAUTH_ADMIN_ORG` and `HQ_OAUTH_ADMIN_TEAM` to the team allowed to
   administer HQ.

```bash
docker compose up -d
docker compose ps
```

`GET /healthz` reports process liveness. PostgreSQL health and backup status
must be monitored separately.

## Project setup

1. Create a `0600` machine-token file.
2. Run `tracker bootstrap-user` with the `worker` role.
3. Run `tracker put-project --identity-mode MODE`. Use `external_id` for
   replayable source imports, `unique_value` when values must be unique, or
   `none` when every submission must become a job.
4. Produce a `jobs-jsonl-zstd-v1` file with `source pack --identity-mode MODE`,
   using the project mode.
5. Run `tracker enqueue-source`.
6. Start workers with the HQ URL, project ID, worker ID, and machine token.

The same setup can be performed over the management backend with an active
administrator token:

```text
GET  /api/v1/admin/projects
PUT  /api/v1/admin/projects/{project_id}
POST /api/v1/admin/projects/{project_id}/jobs
POST /api/v1/admin/projects/{project_id}/source
```

Routine remote administration also supports user status and roles, machine
token rotation or revocation, compressed source import, paginated job
inspection, terminal-failure requeue, and deletion of non-WIP jobs. Bootstrap,
migrations, secret generation, and source pack/merge remain local operational
commands.

The Web Dashboard exposes the same identity-mode selection when creating a
project and shows the fixed mode on project lists and detail pages.

Worker credentials cannot call these endpoints. Keep administrator tokens out
of worker deployments and browser storage. The web UI uses an HttpOnly,
SameSite browser session and CSRF tokens; it never stores the GitHub access
token or a machine token in the browser.

## Normal shutdown

Stop workers first if possible, then stop HQ. Attempts that lose their workers
remain WIP until their lease expires and are reset by a later claim. A clean HQ
shutdown does not need to rewrite job state.

## Backup and restore

- Back up PostgreSQL regularly.
- Test restoration into a separate database.
- Preserve machine-token files independently; rotating a token invalidates the
  previous value.
- Preserve the web-session secret to avoid invalidating all browser sessions
  on routine redeploys. Rotate it to revoke every browser session.
- WARC Core has a separate backup and recovery plan because HQ stores receipts,
  not WARC bytes.

After restoring an older PostgreSQL backup, some completed work may run again.
WARC Core and workers must therefore use stable object identity and tolerate
at-least-once execution.

## Minimum monitoring

- PostgreSQL availability, storage, connections, and backup age;
- job counts by project and status;
- oldest `todo` age;
- expired WIP count;
- reset-exhausted and permanent-failure rates;
- claim and mutation latency;
- receipt-bearing completion rate for archive projects.

Do not add a metrics subsystem until these queries have been exercised during
a real pilot. Structured logs and direct PostgreSQL queries are sufficient for
the first controlled run.
