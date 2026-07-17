# SavewebHQ

SavewebHQ is a small, explicit distributed queue for volunteer-operated web
archive workers. A trusted tracker owns the control plane, each active shard
owns one SQLite queue, and workers connect directly to the shard selected by
the tracker. Go HTTP adapters use Echo v5; domain and storage packages do not
depend on the web framework.

The first version intentionally supports only explicit, pre-split projects.
Its design priorities are layering, modularity, low operational weight, and
explicit behavior over implicit automation.

## Repository layout

```text
api/                 OpenAPI contract and cross-language conformance vectors
cmd/tracker/         Go tracker process
cmd/shard/           Go shard process
internal/            Go implementation packages
pkg/protocol/        Shared public Go protocol types
sdk/worker/          Go worker SDK
sdk/python/          Python worker SDK (managed with uv)
design.md            System design
api-v1.md            Queue API rationale and semantics
control-api-v1.md    Agent and access-token control-plane semantics
review.md            Scoped design review
```

## Development

Prerequisites are Go 1.25 or newer and `uv` 0.9 or newer.

```bash
make test
make check
```

The PostgreSQL store contract test is explicit because it starts a temporary
Docker container:

```bash
make test-postgres
```

The cross-process E2E test also needs Docker. It starts PostgreSQL, tracker,
and a pinned-HTTPS shard, then runs both worker SDKs and a generation takeover:

```bash
make test-e2e
```

Python commands always run through `uv`:

```bash
uv sync --project sdk/python --dev
uv run --project sdk/python pytest
```

See [api-v1.md](./api-v1.md) for protocol semantics and
[design.md](./design.md) for the full design.

## Tracker commands

Tracker state is PostgreSQL-backed. Schema migration and key creation are
deliberately separate from serving:

```bash
go run ./cmd/tracker keygen --out ./tracker-key.json --key-id key-2026-01
go run ./cmd/tracker web-keygen --out ./tracker-web.secret
go run ./cmd/tracker migrate --database-url "$HQ_DATABASE_URL"
go run ./cmd/tracker serve \
  --database-url "$HQ_DATABASE_URL" \
  --public-url https://tracker.example \
  --signing-key-file ./tracker-key.json \
  --github-client-id "$HQ_GITHUB_CLIENT_ID" \
  --github-client-secret-file ./github-client.secret \
  --web-session-secret-file ./tracker-web.secret
```

The GitHub OAuth callback is
`https://tracker.example/auth/github/callback`. Tracker uses OAuth `state` and
PKCE S256, requests no repository or email scope, and discards the GitHub
access token after fetching `/user`. New users are pending by default; use
`--oauth-auto-grant-worker` only when the deployment intentionally allows open
worker registration. The contributor portal is at `/`, and active admins can
review users at `/admin/users`.

`bootstrap-user` exists only for creating the first administrator before the
web administration flow is configured. It reads the reusable machine token
from a private `0600` file and never writes the token to logs. The trusted
tracker database retains the current value so the contributor can reuse it on
multiple machines, as defined in the v1 design.

To link the first administrator to GitHub, pass its immutable numeric GitHub
ID during bootstrap. The login updates only display metadata and retains the
trusted roles:

```bash
go run ./cmd/tracker bootstrap-user \
  --database-url "$HQ_DATABASE_URL" --user-id initial-admin \
  --github-user-id 123456 --roles admin,shard_owner,worker \
  --machine-token-file ./initial-admin.token
```

Projects and pre-split shards are also explicit commands until the admin page
is available:

```bash
go run ./cmd/tracker put-project --database-url "$HQ_DATABASE_URL" --project-id project-1
go run ./cmd/tracker put-shard --database-url "$HQ_DATABASE_URL" \
  --project-id project-1 --shard-id shard-1 --owner-agent-id sh_xxx
```

## Shard commands

Initialization creates a stable agent ID and a random local-admin token in one
private identity file:

```bash
go run ./cmd/shard init --out ./shard-identity.json
```

For a daemon exposed directly with pinned HTTPS, create a stable P-256 key and
self-signed certificate. `tls-init` prints the SPKI SHA-256 value to register:

```bash
go run ./cmd/shard tls-init \
  --key-out ./shard-tls.key --cert-out ./shard-tls.crt \
  --server-name shard.example
```

Then run `serve` with both TLS files and the printed pin. An HTTPS endpoint
terminated by Caddy or cloudflared instead uses `--tls-terminated-by-proxy` and
normally omits the pin. Plain HTTP requires
`--allow-insecure-public-endpoint`. Tracker HTTP likewise requires the separate
`--allow-http-tracker` local-test opt-in.

Shard also starts a separate management server on `127.0.0.1:9081`. By
default, each `serve` invocation rotates a 256-bit token into
`<data-dir>/runtime/local-admin.token` with mode `0600`; only the file path is
logged. Set `SAVEWEB_LOCAL_ADMIN_TOKEN` to provide a stable value of at least
32 characters. The local page can inspect assignments and queue counts and can
pause/resume new claims without blocking completion of existing attempts. It
cannot change tracker ownership or generation. Use an SSH tunnel for remote
access; the admin listener cannot bind a non-loopback address.

## Go worker SDK

The Go SDK opens and heartbeats a worker session, obtains a direct shard route,
and returns a route-bound batch:

```go
session, err := worker.OpenSession(ctx, worker.Config{
    TrackerURL:   "https://tracker.example",
    MachineToken: machineToken,
    AgentID:      workerAgentID,
}, "project-1", protocol.Attrs{})
if err != nil { /* handle */ }
defer session.Close()

batch, err := session.Claim(ctx, 64, 300, []string{protocol.JobTypeSeed})
```

Call `batch.Complete`, `batch.Fail`, or `batch.ExtendLease` for the returned
attempts. Those methods can refresh an expired access token only while the
tracker still reports the same shard owner and generation. If takeover has
occurred they return `worker.ErrRouteRetired`; the caller must discard the
local outcome and must not replay it to the new generation.
