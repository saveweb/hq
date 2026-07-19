#!/bin/sh
set -eu

if [ "${1:-}" != "serve" ]; then
  exec "$@"
fi

: "${HQ_DATABASE_URL:?HQ_DATABASE_URL is required}"

if [ -n "${HQ_PUBLIC_URL:-}${HQ_GITHUB_CLIENT_ID:-}${HQ_GITHUB_CLIENT_SECRET_FILE:-}${HQ_WEB_SESSION_SECRET_FILE:-}${HQ_OAUTH_ADMIN_ORG:-}${HQ_OAUTH_ADMIN_TEAM:-}" ]; then
  : "${HQ_PUBLIC_URL:?HQ_PUBLIC_URL is required when web administration is enabled}"
  : "${HQ_GITHUB_CLIENT_ID:?HQ_GITHUB_CLIENT_ID is required when web administration is enabled}"
  : "${HQ_GITHUB_CLIENT_SECRET_FILE:?HQ_GITHUB_CLIENT_SECRET_FILE is required when web administration is enabled}"
  : "${HQ_WEB_SESSION_SECRET_FILE:?HQ_WEB_SESSION_SECRET_FILE is required when web administration is enabled}"
  : "${HQ_OAUTH_ADMIN_ORG:?HQ_OAUTH_ADMIN_ORG is required when web administration is enabled}"
  : "${HQ_OAUTH_ADMIN_TEAM:?HQ_OAUTH_ADMIN_TEAM is required when web administration is enabled}"
fi

set -- /usr/local/bin/tracker serve \
  --listen :8080 \
  --database-url "${HQ_DATABASE_URL}"

/usr/local/bin/tracker migrate --database-url "${HQ_DATABASE_URL}"
exec "$@"
