#!/usr/bin/env bash
set -euo pipefail

container="saveweb-hq-pgtest-$$"
cleanup() {
  docker rm -f "${container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --rm -d --name "${container}" \
  -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=saveweb_hq_test \
  -p 127.0.0.1::5432 \
  postgres:17-alpine >/dev/null

ready=0
for _ in $(seq 1 300); do
  ready_count=$(docker logs "${container}" 2>&1 | grep -c 'database system is ready to accept connections' || true)
  if [ "${ready_count}" -ge 2 ]; then
    ready=1
    break
  fi
  sleep 0.1
done
if [ "${ready}" -ne 1 ]; then
  docker logs "${container}" >&2
  exit 1
fi

port=$(docker port "${container}" 5432/tcp | sed -n 's/.*://p')
HQ_TEST_POSTGRES_URL="postgres://postgres:test@127.0.0.1:${port}/saveweb_hq_test?sslmode=disable" \
  go test -count=1 -v ./internal/tracker/postgres
