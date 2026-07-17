#!/bin/sh
set -eu

if [ "${1:-}" != "serve" ]; then
  exec "$@"
fi

: "${HQ_DATABASE_URL:?HQ_DATABASE_URL is required}"
: "${HQ_PUBLIC_URL:?HQ_PUBLIC_URL is required}"

signing_key=/run/secrets/tracker-signing.json
web_secret=/run/secrets/tracker-web.secret
github_secret=/run/secrets/github-client.secret
r2_access_key=/run/secrets/r2-access-key
r2_secret_key=/run/secrets/r2-secret-key

if [ ! -r "${signing_key}" ]; then
  echo "tracker signing key is missing or unreadable" >&2
  exit 1
fi

set -- /usr/local/bin/tracker serve \
  --listen :8080 \
  --database-url "${HQ_DATABASE_URL}" \
  --public-url "${HQ_PUBLIC_URL}" \
  --signing-key-file "${signing_key}"

if [ -n "${HQ_GITHUB_CLIENT_ID:-}" ]; then
  if [ ! -r "${github_secret}" ] || [ ! -r "${web_secret}" ]; then
    echo "GitHub OAuth requires github-client.secret and tracker-web.secret" >&2
    exit 1
  fi
  if [ -z "${HQ_OAUTH_ADMIN_ORG:-}" ] || [ -z "${HQ_OAUTH_ADMIN_TEAM:-}" ]; then
    echo "GitHub OAuth requires HQ_OAUTH_ADMIN_ORG and HQ_OAUTH_ADMIN_TEAM" >&2
    exit 1
  fi
  set -- "$@" \
    --github-client-id "${HQ_GITHUB_CLIENT_ID}" \
    --github-client-secret-file "${github_secret}" \
    --web-session-secret-file "${web_secret}" \
    --oauth-admin-org "${HQ_OAUTH_ADMIN_ORG}" \
    --oauth-admin-team "${HQ_OAUTH_ADMIN_TEAM}"
elif [ -e "${github_secret}" ]; then
  echo "github-client.secret exists but HQ_GITHUB_CLIENT_ID is empty" >&2
  exit 1
fi

if [ -n "${HQ_S3_ENDPOINT:-}" ]; then
  if [ ! -r "${r2_access_key}" ] || [ ! -r "${r2_secret_key}" ]; then
    echo "S3 configuration requires r2-access-key and r2-secret-key" >&2
    exit 1
  fi
  set -- "$@" \
    --s3-endpoint "${HQ_S3_ENDPOINT}" \
    --s3-region "${HQ_S3_REGION:-auto}" \
    --s3-access-key-id-file "${r2_access_key}" \
    --s3-secret-access-key-file "${r2_secret_key}"
  if [ -n "${HQ_CHECKPOINT_PREFIX_URI:-}" ]; then
    set -- "$@" --checkpoint-prefix-uri "${HQ_CHECKPOINT_PREFIX_URI}"
  fi
  if [ "${HQ_S3_PATH_STYLE:-0}" = "1" ]; then
    set -- "$@" --s3-path-style
  fi
elif [ -e "${r2_access_key}" ] || [ -e "${r2_secret_key}" ]; then
  echo "R2 credential files exist but HQ_S3_ENDPOINT is empty" >&2
  exit 1
fi

/usr/local/bin/tracker migrate --database-url "${HQ_DATABASE_URL}"
exec "$@"
