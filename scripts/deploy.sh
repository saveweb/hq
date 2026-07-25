#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "${root}"

if [ -n "$(git status --porcelain)" ]; then
  echo "refusing to deploy from a dirty worktree" >&2
  git status --short >&2
  exit 1
fi
if [ ! -f .env ]; then
  echo ".env does not exist; create it from .env.example first" >&2
  exit 1
fi

deploy_commit=$(git rev-parse --verify HEAD)
deploy_version=$(git rev-parse --short=7 HEAD)

set_env() {
  local key=$1
  local value=$2
  if grep -q "^${key}=" .env; then
    sed -i "s/^${key}=.*/${key}=${value}/" .env
  else
    printf '%s=%s\n' "${key}" "${value}" >>.env
  fi
}

set_env HQ_VERSION "${deploy_version}"
set_env HQ_COMMIT "${deploy_commit}"

echo "deploying SavewebHQ ${deploy_version} (${deploy_commit})"
docker compose up -d --build

tracker_container=$(docker compose ps -q tracker)
if [ -z "${tracker_container}" ]; then
  echo "tracker container was not created" >&2
  exit 1
fi

for _ in $(seq 1 60); do
  tracker_health=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "${tracker_container}")
  if [ "${tracker_health}" = healthy ]; then
    break
  fi
  if [ "${tracker_health}" = unhealthy ]; then
    docker compose logs --tail=100 tracker >&2
    exit 1
  fi
  sleep 2
done
if [ "${tracker_health}" != healthy ]; then
  echo "tracker did not become healthy within 120 seconds" >&2
  docker compose logs --tail=100 tracker >&2
  exit 1
fi

running_image=$(docker inspect --format '{{.Config.Image}}' "${tracker_container}")
image_revision=$(docker image inspect "${running_image}" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')
if [ "${image_revision}" != "${deploy_commit}" ]; then
  echo "running image revision ${image_revision} does not match ${deploy_commit}" >&2
  exit 1
fi

public_url=$(sed -n 's/^HQ_PUBLIC_URL=//p' .env | tail -n 1)
if [ -n "${public_url}" ]; then
  curl --fail --silent --show-error --max-time 20 "${public_url%/}/healthz" >/dev/null
fi

docker compose ps
echo "deployed SavewebHQ ${deploy_version} (${deploy_commit})"
