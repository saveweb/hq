# SavewebHQ operations

## Services

HQ requires PostgreSQL and an HTTPS reverse proxy. It has no object-storage
credentials and does not require a public endpoint for any worker.

```bash
docker compose up -d
docker compose ps
```

`GET /healthz` reports process liveness. PostgreSQL health and backup status
must be monitored separately.

## Project setup

1. Create a `0600` machine-token file.
2. Run `tracker bootstrap-user` with the `worker` role.
3. Run `tracker put-project`.
4. Produce a `jobs-jsonl-zstd-v1` file with `source pack`.
5. Run `tracker enqueue-source`.
6. Start workers with the HQ URL, project ID, worker ID, and machine token.

## Normal shutdown

Stop workers first if possible, then stop HQ. Attempts that lose their workers
remain WIP until their lease expires and are reset by a later claim. A clean HQ
shutdown does not need to rewrite job state.

## Backup and restore

- Back up PostgreSQL regularly.
- Test restoration into a separate database.
- Preserve machine-token files independently; rotating a token invalidates the
  previous value.
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
