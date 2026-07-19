#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
run_dir=$(mktemp -d)
container="saveweb-hq-e2e-pg-$$"
tracker_pid=""
cleanup() {
  status=$?
  trap - EXIT
  if [ -n "${tracker_pid}" ]; then kill -TERM "${tracker_pid}" >/dev/null 2>&1 || true; wait "${tracker_pid}" >/dev/null 2>&1 || true; fi
  docker rm -f "${container}" >/dev/null 2>&1 || true
  if [ "${status}" -ne 0 ] && [ -s "${run_dir}/tracker.log" ]; then tail -n 200 "${run_dir}/tracker.log" >&2; fi
  rm -rf "${run_dir}"
  exit "${status}"
}
trap cleanup EXIT
cd "${root}"

go build -o "${run_dir}/tracker" ./cmd/tracker
go build -o "${run_dir}/source" ./cmd/source
docker run --rm -d --name "${container}" -e POSTGRES_PASSWORD=test -e POSTGRES_DB=hq -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
for _ in $(seq 1 300); do
  if [ "$(docker logs "${container}" 2>&1 | grep -c 'database system is ready to accept connections' || true)" -ge 2 ]; then break; fi
  sleep 0.1
done
pg_port=$(docker port "${container}" 5432/tcp | sed -n 's/.*://p')
database_url="postgres://postgres:test@127.0.0.1:${pg_port}/hq?sslmode=disable"

umask 077
printf '%s\n' 'e2e-worker-token-0123456789abcdef' >"${run_dir}/worker.token"
printf '%s\n' 'https://example.test/a' >"${run_dir}/jobs.txt"
"${run_dir}/source" pack --input "${run_dir}/jobs.txt" --output "${run_dir}/jobs.zst"
"${run_dir}/tracker" migrate --database-url "${database_url}"
"${run_dir}/tracker" bootstrap-user --database-url "${database_url}" --user-id worker-e2e --roles worker --machine-token-file "${run_dir}/worker.token"
"${run_dir}/tracker" put-project --database-url "${database_url}" --project-id project-e2e
"${run_dir}/tracker" enqueue-source --database-url "${database_url}" --project-id project-e2e --input "${run_dir}/jobs.zst"

port=$((20000 + RANDOM % 20000))
base="http://127.0.0.1:${port}"
"${run_dir}/tracker" serve --listen "127.0.0.1:${port}" --database-url "${database_url}" >"${run_dir}/tracker.log" 2>&1 &
tracker_pid=$!
for _ in $(seq 1 200); do curl --fail --silent "${base}/healthz" >/dev/null 2>&1 && break; sleep 0.1; done

claim=$(curl --fail --silent --show-error -H 'Authorization: Bearer e2e-worker-token-0123456789abcdef' -H 'Content-Type: application/json' -d '{"worker_id":"worker-e2e","max_jobs":1,"lease_seconds":300,"accept_types":[]}' "${base}/api/v1/projects/project-e2e/jobs/claim")
job_id=$(printf '%s' "${claim}" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
attempt_id=$(printf '%s' "${claim}" | sed -n 's/.*"attempt_id":"\([^"]*\)".*/\1/p')
test -n "${job_id}" && test -n "${attempt_id}"
accepted_at=$(date +%s)
complete=$(curl --fail --silent --show-error -H 'Authorization: Bearer e2e-worker-token-0123456789abcdef' -H 'Content-Type: application/json' -d "{\"worker_id\":\"worker-e2e\",\"items\":[{\"job_id\":\"${job_id}\",\"attempt_id\":\"${attempt_id}\",\"outcome\":{\"kind\":\"success\",\"code\":200,\"uri\":null,\"meta\":{}},\"warc_receipts\":[{\"id\":\"receipt-e2e\",\"issuer\":\"https://warc.example\",\"object_id\":\"warc-e2e\",\"sha256\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"size_bytes\":123,\"accepted_at\":${accepted_at},\"signature\":\"test\"}]}]}" "${base}/api/v1/projects/project-e2e/jobs/complete")
printf '%s' "${complete}" | grep -q '"status":"applied"'
count=$(docker exec "${container}" psql -U postgres -d hq -Atc "SELECT count(*) FROM tracker_jobs WHERE project_id='project-e2e' AND status='done' AND jsonb_array_length(warc_receipts)=1")
test "${count}" = 1
printf 'SavewebHQ E2E passed\n'
