#!/bin/sh
set -eu

if [ "${1:-}" != "serve" ]; then
  exec "$@"
fi

: "${HQ_DATABASE_URL:?HQ_DATABASE_URL is required}"

set -- /usr/local/bin/tracker serve \
  --listen :8080 \
  --database-url "${HQ_DATABASE_URL}"

/usr/local/bin/tracker migrate --database-url "${HQ_DATABASE_URL}"
exec "$@"
