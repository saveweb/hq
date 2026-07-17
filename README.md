# SavewebHQ

SavewebHQ is a small, explicit distributed queue for volunteer-operated web
archive workers. A trusted tracker owns the control plane, each active shard
owns one SQLite queue, and workers connect directly to the shard selected by
the tracker.

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

Python commands always run through `uv`:

```bash
uv sync --project sdk/python --dev
uv run --project sdk/python pytest
```

See [api-v1.md](./api-v1.md) for protocol semantics and
[design.md](./design.md) for the full design.
