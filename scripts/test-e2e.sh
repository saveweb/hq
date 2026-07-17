#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
run_dir=$(mktemp -d)
postgres_container="saveweb-hq-e2e-pg-$$"
minio_container="saveweb-hq-e2e-s3-$$"
tracker_pid=""
shard_pid=""

cleanup() {
  status=$?
  trap - EXIT
  if [ -n "${shard_pid}" ]; then
    kill -TERM "${shard_pid}" >/dev/null 2>&1 || true
    wait "${shard_pid}" >/dev/null 2>&1 || true
  fi
  if [ -n "${tracker_pid}" ]; then
    kill -TERM "${tracker_pid}" >/dev/null 2>&1 || true
    wait "${tracker_pid}" >/dev/null 2>&1 || true
  fi
  docker rm -f "${postgres_container}" "${minio_container}" >/dev/null 2>&1 || true
  if [ "${status}" -ne 0 ]; then
    for log in tracker shard takeover recovery-shard; do
      if [ -s "${run_dir}/${log}.log" ]; then
        printf '\n=== %s log ===\n' "${log}" >&2
        tail -n 200 "${run_dir}/${log}.log" >&2
      fi
    done
  fi
  rm -rf "${run_dir}"
  exit "${status}"
}
trap cleanup EXIT

wait_url() {
  url=$1
  for _ in $(seq 1 200); do
    if curl --insecure --fail --silent --show-error "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

wait_file() {
  path=$1
  for _ in $(seq 1 200); do
    if [ -s "${path}" ]; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

choose_port() {
  while true; do
    candidate=$((20000 + RANDOM % 20000))
    if ! (exec 3<>"/dev/tcp/127.0.0.1/${candidate}") 2>/dev/null; then
      printf '%s\n' "${candidate}"
      return
    fi
  done
}

stop_shard() {
  if [ -n "${shard_pid}" ]; then
    kill -TERM "${shard_pid}"
    wait "${shard_pid}"
    shard_pid=""
  fi
}

wait_shard_status() {
  project=$1
  shard=$2
  expected=$3
  for _ in $(seq 1 200); do
    actual=$(docker exec "${postgres_container}" psql -U postgres -d saveweb_hq_e2e -Atc \
      "SELECT status FROM tracker_shards WHERE project_id='${project}' AND id='${shard}'" 2>/dev/null || true)
    if [ "${actual}" = "${expected}" ]; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

cd "${root}"
for command in docker curl go uv; do
  command -v "${command}" >/dev/null
done

printf 'Building E2E binaries...\n'
go build -o "${run_dir}/tracker" ./cmd/tracker
go build -o "${run_dir}/shard" ./cmd/shard
go build -o "${run_dir}/source" ./cmd/source
go build -o "${run_dir}/queue-tool" ./e2e/cmd/queue-tool
go build -o "${run_dir}/go-worker" ./e2e/cmd/go-worker

docker run --rm -d --name "${postgres_container}" \
  -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=saveweb_hq_e2e \
  -p 127.0.0.1::5432 \
  postgres:17-alpine >/dev/null

ready=0
for _ in $(seq 1 300); do
  ready_count=$(docker logs "${postgres_container}" 2>&1 | grep -c 'database system is ready to accept connections' || true)
  if [ "${ready_count}" -ge 2 ]; then
    ready=1
    break
  fi
  sleep 0.1
done
if [ "${ready}" -ne 1 ]; then
  docker logs "${postgres_container}" >&2
  exit 1
fi

pg_port=$(docker port "${postgres_container}" 5432/tcp | sed -n 's/.*://p')
database_url="postgres://postgres:test@127.0.0.1:${pg_port}/saveweb_hq_e2e?sslmode=disable"

minio_access_key="saveweb-e2e-access"
minio_secret_key="saveweb-e2e-secret-0123456789"
docker run --rm -d --name "${minio_container}" \
  -e "MINIO_ROOT_USER=${minio_access_key}" \
  -e "MINIO_ROOT_PASSWORD=${minio_secret_key}" \
  -p 127.0.0.1::9000 \
  quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z \
  server /data --address :9000 >/dev/null
minio_port=$(docker port "${minio_container}" 9000/tcp | sed -n 's/.*://p')
minio_url="http://127.0.0.1:${minio_port}"
wait_url "${minio_url}/minio/health/ready"

tracker_port=$(choose_port)
shard_port=$(choose_port)
admin_port=$(choose_port)
tracker_url="http://127.0.0.1:${tracker_port}"
shard_url="https://127.0.0.1:${shard_port}"

umask 077
printf '%s\n' 'e2e-owner-token-0123456789abcdef0123456789abcdef' >"${run_dir}/owner.token"
printf '%s\n' 'e2e-worker-token-0123456789abcdef0123456789abcdef' >"${run_dir}/worker.token"
printf '%s\n' "${minio_access_key}" >"${run_dir}/s3-access-key"
printf '%s\n' "${minio_secret_key}" >"${run_dir}/s3-secret-key"
printf '%s\n' 'https://source.example/a' 'https://source.example/b' >"${run_dir}/source-jobs.txt"
"${run_dir}/source" pack --input "${run_dir}/source-jobs.txt" --output "${run_dir}/source.jobs.jsonl.zst"
mc_host="${minio_url/http:\/\//http:\/\/${minio_access_key}:${minio_secret_key}@}"
docker run --rm --network host -e "MC_HOST_hq=${mc_host}" \
  quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z mb --ignore-existing hq/source
docker run --rm --network host -e "MC_HOST_hq=${mc_host}" -v "${run_dir}:/work:ro" \
  quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z \
  cp /work/source.jobs.jsonl.zst hq/source/source.jobs.jsonl.zst
source_etag=$(md5sum "${run_dir}/source.jobs.jsonl.zst" | awk '{print $1}')
"${run_dir}/tracker" keygen --out "${run_dir}/signing.key" --key-id e2e-key
"${run_dir}/tracker" migrate --database-url "${database_url}"
"${run_dir}/tracker" bootstrap-user --database-url "${database_url}" --user-id owner-e2e \
  --roles shard_owner --machine-token-file "${run_dir}/owner.token"
"${run_dir}/tracker" bootstrap-user --database-url "${database_url}" --user-id worker-e2e \
  --roles worker --machine-token-file "${run_dir}/worker.token"
"${run_dir}/tracker" put-project --database-url "${database_url}" --project-id project-e2e
"${run_dir}/tracker" put-receiver --database-url "${database_url}" --project-id project-e2e \
  --receiver-id receiver-e2e --sink-uri s3://source/receiver-output
shard_id=$("${run_dir}/shard" init --out "${run_dir}/shard.identity")
tls_pin=$("${run_dir}/shard" tls-init --key-out "${run_dir}/shard.key" \
  --cert-out "${run_dir}/shard.pem" --server-name 127.0.0.1)

"${run_dir}/tracker" serve --listen "127.0.0.1:${tracker_port}" \
  --database-url "${database_url}" --public-url "${tracker_url}" \
  --signing-key-file "${run_dir}/signing.key" --allow-insecure-public-url \
  --allow-private-shard-endpoints --agent-heartbeat-seconds 1 --owner-lease-seconds 30 \
  --session-heartbeat-seconds 1 --session-lease-seconds 30 \
  --s3-endpoint "${minio_url}" --s3-region us-east-1 --s3-path-style --allow-http-s3 \
  --checkpoint-prefix-uri s3://source/checkpoints \
  --s3-access-key-id-file "${run_dir}/s3-access-key" \
  --s3-secret-access-key-file "${run_dir}/s3-secret-key" >"${run_dir}/tracker.log" 2>&1 &
tracker_pid=$!
wait_url "${tracker_url}/healthz"

start_shard() {
  "${run_dir}/shard" serve --listen "127.0.0.1:${shard_port}" \
    --admin-listen "127.0.0.1:${admin_port}" \
    --tracker-url "${tracker_url}" --tracker-issuer "${tracker_url}" --allow-http-tracker \
    --allow-http-object-download \
    --checkpoint-interval-seconds 2 \
    --machine-token-file "${run_dir}/owner.token" --identity-file "${run_dir}/shard.identity" \
    --data-dir "${run_dir}/shard-data" --endpoint "${shard_url}" --endpoint-version 1 \
    --tls-spki-sha256 "${tls_pin}" --tls-cert-file "${run_dir}/shard.pem" \
    --tls-key-file "${run_dir}/shard.key" >"${run_dir}/shard.log" 2>&1 &
  shard_pid=$!
  wait_url "${shard_url}/healthz"
  wait_url "http://127.0.0.1:${admin_port}/"
  sleep 0.5
  kill -0 "${shard_pid}"
  local_admin_token=$(tr -d '\r\n' <"${run_dir}/shard-data/runtime/local-admin.token")
  curl --fail --silent --show-error \
    -H "Authorization: Bearer ${local_admin_token}" \
    "http://127.0.0.1:${admin_port}/api/v1/status" | grep -q "${shard_id}"
}

printf 'Registering HTTPS shard endpoint...\n'
start_shard
stop_shard
"${run_dir}/tracker" put-shard --database-url "${database_url}" --project-id project-e2e \
  --shard-id shard-e2e --owner-agent-id "${shard_id}" --generation 1
"${run_dir}/queue-tool" --mode seed --data-dir "${run_dir}/shard-data" \
  --project-id project-e2e --shard-id shard-e2e --generation 1
start_shard

printf 'Running Go and Python workers through tracker routing...\n'
"${run_dir}/go-worker" --phase initial --tracker-url "${tracker_url}" \
  --machine-token-file "${run_dir}/worker.token"
uv run --project sdk/python python e2e/python_worker.py --tracker-url "${tracker_url}" \
  --machine-token-file "${run_dir}/worker.token"
"${run_dir}/go-worker" --phase receiver --tracker-url "${tracker_url}" \
  --machine-token-file "${run_dir}/worker.token"
"${run_dir}/go-worker" --phase verify --tracker-url "${tracker_url}" \
  --machine-token-file "${run_dir}/worker.token"

receiver_objects=$(docker run --rm --network host -e "MC_HOST_hq=${mc_host}" \
  quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z \
  find hq/source/receiver-output --name '*.jobs.jsonl.zst' | wc -l)
if [ "${receiver_objects}" -ne 2 ]; then
  printf 'expected two immutable receiver objects, got %s\n' "${receiver_objects}" >&2
  exit 1
fi

printf 'Merging receiver objects into an explicit Stage 2 source...\n'
docker run --rm --network host --user "$(id -u):$(id -g)" \
  -e HOME=/tmp -e "MC_HOST_hq=${mc_host}" -v "${run_dir}:/work" \
  quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z \
  cp --recursive hq/source/receiver-output /work/
mapfile -t receiver_inputs < <(find "${run_dir}/receiver-output" -type f -name '*.jobs.jsonl.zst' | sort)
if [ "${#receiver_inputs[@]}" -ne 2 ]; then
  printf 'expected two downloaded receiver objects, got %s\n' "${#receiver_inputs[@]}" >&2
  exit 1
fi
merge_args=()
for input in "${receiver_inputs[@]}"; do
  merge_args+=(--input "${input}")
done
"${run_dir}/source" merge "${merge_args[@]}" \
  --output-prefix "${run_dir}/stage-2" --jobs-per-file 100
docker run --rm --network host -e "MC_HOST_hq=${mc_host}" -v "${run_dir}:/work:ro" \
  quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z \
  cp /work/stage-2-000001.jobs.jsonl.zst hq/source/stage-2.jobs.jsonl.zst
stage2_etag=$(md5sum "${run_dir}/stage-2-000001.jobs.jsonl.zst" | awk '{print $1}')
"${run_dir}/tracker" put-project --database-url "${database_url}" --project-id project-stage2-e2e
"${run_dir}/tracker" put-shard --database-url "${database_url}" --project-id project-stage2-e2e \
  --shard-id shard-stage2-e2e --owner-agent-id "${shard_id}" --generation 1 --status loading \
  --source-uri s3://source/stage-2.jobs.jsonl.zst --source-format jobs-jsonl-zstd-v1 \
  --source-etag "${stage2_etag}"
sleep 3
"${run_dir}/go-worker" --phase stage2 --project-id project-stage2-e2e \
  --tracker-url "${tracker_url}" --machine-token-file "${run_dir}/worker.token"

printf 'Fencing an in-flight generation and recovering its job...\n'
"${run_dir}/go-worker" --phase takeover --tracker-url "${tracker_url}" \
  --machine-token-file "${run_dir}/worker.token" --ready-file "${run_dir}/takeover.ready" \
  --continue-file "${run_dir}/takeover.continue" >"${run_dir}/takeover.log" 2>&1 &
takeover_pid=$!
wait_file "${run_dir}/takeover.ready"
"${run_dir}/tracker" put-shard --database-url "${database_url}" --project-id project-e2e \
  --shard-id shard-e2e --owner-agent-id "${shard_id}" --generation 2
sleep 3
printf 'continue\n' >"${run_dir}/takeover.continue"
wait "${takeover_pid}"
"${run_dir}/go-worker" --phase recover --tracker-url "${tracker_url}" \
  --machine-token-file "${run_dir}/worker.token"

printf 'Loading an immutable S3-compatible source through a presigned URL...\n'
"${run_dir}/tracker" put-project --database-url "${database_url}" --project-id project-source-e2e
"${run_dir}/tracker" put-shard --database-url "${database_url}" --project-id project-source-e2e \
  --shard-id shard-source-e2e --owner-agent-id "${shard_id}" --generation 1 --status loading \
  --source-uri s3://source/source.jobs.jsonl.zst --source-format jobs-jsonl-zstd-v1 \
  --source-etag "${source_etag}"
sleep 3
"${run_dir}/go-worker" --phase source --project-id project-source-e2e --tracker-url "${tracker_url}" \
  --machine-token-file "${run_dir}/worker.token"
sleep 3

checkpoint_rows=$(docker exec "${postgres_container}" psql -U postgres -d saveweb_hq_e2e -Atc \
  "SELECT count(*) FROM tracker_shards WHERE checkpoint_uri IS NOT NULL AND checkpoint_format='sqlite-zstd-v1'")
if [ "${checkpoint_rows}" -lt 2 ]; then
  printf 'expected published checkpoint pointers, got %s\n' "${checkpoint_rows}" >&2
  exit 1
fi
checkpoint_objects=$(docker run --rm --network host -e "MC_HOST_hq=${mc_host}" \
  quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z \
  find hq/source/checkpoints --name '*.sqlite.zst' | wc -l)
if [ "${checkpoint_objects}" -lt 2 ]; then
  printf 'expected checkpoint objects, got %s\n' "${checkpoint_objects}" >&2
  exit 1
fi

printf 'Restoring a published checkpoint onto a blank replacement shard...\n'
"${run_dir}/tracker" put-project --database-url "${database_url}" --project-id project-recovery-e2e
"${run_dir}/queue-tool" --mode seed-recovery --data-dir "${run_dir}/shard-data" \
  --project-id project-recovery-e2e --shard-id shard-recovery-e2e --generation 1
"${run_dir}/tracker" put-shard --database-url "${database_url}" --project-id project-recovery-e2e \
  --shard-id shard-recovery-e2e --owner-agent-id "${shard_id}" --generation 1
checkpoint_ready=0
for _ in $(seq 1 100); do
  checkpoint_ready=$(docker exec "${postgres_container}" psql -U postgres -d saveweb_hq_e2e -Atc \
    "SELECT count(*) FROM tracker_shards WHERE project_id='project-recovery-e2e' AND id='shard-recovery-e2e' AND checkpoint_uri IS NOT NULL")
  if [ "${checkpoint_ready}" -eq 1 ]; then
    break
  fi
  sleep 0.2
done
if [ "${checkpoint_ready}" -ne 1 ]; then
  printf 'recovery checkpoint was not published\n' >&2
  exit 1
fi
stop_shard

recovery_shard_id=$("${run_dir}/shard" init --out "${run_dir}/recovery-shard.identity")
recovery_shard_port=$(choose_port)
recovery_admin_port=$(choose_port)
recovery_shard_url="https://127.0.0.1:${recovery_shard_port}"
recovery_tls_pin=$("${run_dir}/shard" tls-init --key-out "${run_dir}/recovery-shard.key" \
  --cert-out "${run_dir}/recovery-shard.pem" --server-name 127.0.0.1)

start_recovery_shard() {
  "${run_dir}/shard" serve --listen "127.0.0.1:${recovery_shard_port}" \
    --admin-listen "127.0.0.1:${recovery_admin_port}" \
    --tracker-url "${tracker_url}" --tracker-issuer "${tracker_url}" --allow-http-tracker \
    --allow-http-object-download --checkpoint-interval-seconds 2 \
    --machine-token-file "${run_dir}/owner.token" --identity-file "${run_dir}/recovery-shard.identity" \
    --data-dir "${run_dir}/recovery-shard-data" --endpoint "${recovery_shard_url}" --endpoint-version 1 \
    --tls-spki-sha256 "${recovery_tls_pin}" --tls-cert-file "${run_dir}/recovery-shard.pem" \
    --tls-key-file "${run_dir}/recovery-shard.key" >"${run_dir}/recovery-shard.log" 2>&1 &
  shard_pid=$!
  wait_url "${recovery_shard_url}/healthz"
  wait_url "http://127.0.0.1:${recovery_admin_port}/"
  sleep 0.5
  kill -0 "${shard_pid}"
  recovery_admin_token=$(tr -d '\r\n' <"${run_dir}/recovery-shard-data/runtime/local-admin.token")
}

# Register and health-check the new public endpoint before assigning ownership.
start_recovery_shard
stop_shard
"${run_dir}/tracker" put-shard --database-url "${database_url}" --project-id project-recovery-e2e \
  --shard-id shard-recovery-e2e --owner-agent-id "${recovery_shard_id}" --generation 2 --status recovering
start_recovery_shard
wait_shard_status project-recovery-e2e shard-recovery-e2e active
local_active=0
for _ in $(seq 1 100); do
  if curl --fail --silent --show-error \
    -H "Authorization: Bearer ${recovery_admin_token}" \
    "http://127.0.0.1:${recovery_admin_port}/api/v1/status" | grep -q '"status":"active"'; then
    local_active=1
    break
  fi
  sleep 0.1
done
if [ "${local_active}" -ne 1 ]; then
  printf 'replacement shard did not apply active assignment\n' >&2
  exit 1
fi
"${run_dir}/go-worker" --phase checkpoint-recovery --project-id project-recovery-e2e \
  --tracker-url "${tracker_url}" --machine-token-file "${run_dir}/worker.token"
stop_shard
"${run_dir}/queue-tool" --mode check-recovery --data-dir "${run_dir}/recovery-shard-data" \
  --project-id project-recovery-e2e --shard-id shard-recovery-e2e --generation 2

"${run_dir}/queue-tool" --mode check --data-dir "${run_dir}/shard-data" \
  --project-id project-e2e --shard-id shard-e2e --generation 2
"${run_dir}/queue-tool" --mode check-source --data-dir "${run_dir}/shard-data" \
  --project-id project-source-e2e --shard-id shard-source-e2e --generation 1
"${run_dir}/queue-tool" --mode check-source --data-dir "${run_dir}/shard-data" \
  --project-id project-stage2-e2e --shard-id shard-stage2-e2e --generation 1
printf 'SavewebHQ cross-process E2E passed.\n'
