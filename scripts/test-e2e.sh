#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
run_dir=$(mktemp -d)
container="saveweb-hq-e2e-pg-$$"
tracker_pid=""

cleanup() {
  status=$?
  trap - EXIT
  if [ -n "${tracker_pid}" ]; then
    kill -TERM "${tracker_pid}" >/dev/null 2>&1 || true
    wait "${tracker_pid}" >/dev/null 2>&1 || true
  fi
  docker rm -f "${container}" >/dev/null 2>&1 || true
  if [ "${status}" -ne 0 ] && [ -s "${run_dir}/tracker.log" ]; then
    tail -n 200 "${run_dir}/tracker.log" >&2
  fi
  rm -rf "${run_dir}"
  exit "${status}"
}
trap cleanup EXIT
cd "${root}"

go build -o "${run_dir}/tracker" ./cmd/tracker
go build -o "${run_dir}/source" ./cmd/source
docker run --rm -d --name "${container}" \
  -e POSTGRES_PASSWORD=test -e POSTGRES_DB=hq \
  -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
for _ in $(seq 1 300); do
  ready=$(docker logs "${container}" 2>&1 | grep -c 'database system is ready to accept connections' || true)
  if [ "${ready}" -ge 2 ]; then break; fi
  sleep 0.1
done
pg_port=$(docker port "${container}" 5432/tcp | sed -n 's/.*://p')
database_url="postgres://postgres:test@127.0.0.1:${pg_port}/hq?sslmode=disable"

umask 077
admin_token='e2e-admin-token-0123456789abcdef'
worker_token='e2e-worker-token-0123456789abcdef'
printf '%s\n' "${admin_token}" >"${run_dir}/admin.token"
printf '%s\n' "${worker_token}" >"${run_dir}/worker.token"
"${run_dir}/tracker" migrate --database-url "${database_url}"
"${run_dir}/tracker" bootstrap-user --database-url "${database_url}" \
  --user-id admin-e2e --roles admin --machine-token-file "${run_dir}/admin.token"
"${run_dir}/tracker" bootstrap-user --database-url "${database_url}" \
  --user-id worker-e2e --roles worker --machine-token-file "${run_dir}/worker.token"

port=$((20000 + RANDOM % 20000))
base="http://127.0.0.1:${port}"
"${run_dir}/tracker" serve --listen "127.0.0.1:${port}" \
  --database-url "${database_url}" >"${run_dir}/tracker.log" 2>&1 &
tracker_pid=$!
for _ in $(seq 1 200); do
  if curl --fail --silent "${base}/healthz" >"${run_dir}/health.json" 2>/dev/null; then break; fi
  sleep 0.1
done
kill -0 "${tracker_pid}"
python3 -c 'import json,sys; assert json.load(open(sys.argv[1])) == {"status":"ok"}' "${run_dir}/health.json"

admin=(-H "Authorization: Bearer ${admin_token}")
worker=(-H "Authorization: Bearer ${worker_token}" -H 'X-SavewebHQ-Client-Version: e2e-v1')
json=(-H 'Content-Type: application/json')

# A worker credential must not cross the management boundary.
status=$(curl --silent --output "${run_dir}/forbidden.json" --write-out '%{http_code}' \
  "${worker[@]}" "${base}/api/v1/admin/projects")
test "${status}" = 403
python3 -c 'import json,sys; assert json.load(open(sys.argv[1]))["error"]["code"] == "permission_denied"' "${run_dir}/forbidden.json"

# Create a project and enqueue a mixed workload entirely through the admin API.
curl --fail --silent --show-error "${admin[@]}" "${json[@]}" \
  -X PUT -d '{"status":"active","identity_mode":"external_id","dispatch_qps":null,"worker_claim_qps":null,"max_jobs_per_claim":256,"max_resets":3,"client_versions":["e2e-v1"]}' \
  "${base}/api/v1/admin/projects/project-e2e" >"${run_dir}/project.json"
python3 -c 'import json,sys; p=json.load(open(sys.argv[1])); assert p["id"] == "project-e2e" and p["status"] == "active" and p["identity_mode"] == "external_id" and p["max_resets"] == 3 and p["recommended_lease_seconds"] == 300 and p["client_versions"] == ["e2e-v1"] and sum(p["job_counts"].values()) == 0' "${run_dir}/project.json"

# Every worker route requires an explicitly allowed client version.
status=$(curl --silent --output "${run_dir}/upgrade.json" --write-out '%{http_code}' \
  -H "Authorization: Bearer ${worker_token}" -H 'X-SavewebHQ-Client-Version: obsolete-v1' \
  "${base}/api/v1/projects/project-e2e")
test "${status}" = 426
python3 -c 'import json,sys; e=json.load(open(sys.argv[1]))["error"]; assert e["code"] == "client_upgrade_required" and e["details"]["client_versions"] == ["e2e-v1"]' "${run_dir}/upgrade.json"

curl --fail --silent --show-error "${worker[@]}" \
  "${base}/api/v1/projects/project-e2e" >"${run_dir}/policy.json"
python3 -c 'import json,sys; p=json.load(open(sys.argv[1])); assert p["recommended_lease_seconds"] == 300 and p["policy_version"] == 1' "${run_dir}/policy.json"

# Upload the packed source format through a separate project.
curl --fail --silent --show-error "${admin[@]}" "${json[@]}" \
  -X PUT -d '{"status":"active","identity_mode":"external_id","dispatch_qps":null,"worker_claim_qps":null,"max_jobs_per_claim":256,"max_resets":3,"client_versions":["e2e-v1"]}' \
  "${base}/api/v1/admin/projects/source-e2e" >"${run_dir}/source-project.json"
printf 'https://example.test/source\n' >"${run_dir}/source-values.txt"
"${run_dir}/source" pack --identity-mode external_id \
  --input "${run_dir}/source-values.txt" --output "${run_dir}/source.jobs.jsonl.zst"
curl --fail --silent --show-error "${admin[@]}" -H 'Content-Type: application/zstd' \
  --data-binary "@${run_dir}/source.jobs.jsonl.zst" \
  "${base}/api/v1/admin/projects/source-e2e/source" >"${run_dir}/source-import.json"
python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); assert r["jobs"] == 1 and r["inserted"] == 1 and r["uncompressed_bytes"] > 0' "${run_dir}/source-import.json"
status=$(curl --silent --output /dev/null --write-out '%{http_code}' "${admin[@]}" -X DELETE \
  "${base}/api/v1/admin/projects/source-e2e")
test "${status}" = 204

# Enqueue count is constrained by the JSON body limit, not the worker batch limit.
curl --fail --silent --show-error "${admin[@]}" "${json[@]}" \
  -X PUT -d '{"status":"active","identity_mode":"external_id","dispatch_qps":null,"worker_claim_qps":null,"max_jobs_per_claim":256,"max_resets":3,"client_versions":["e2e-v1"]}' \
  "${base}/api/v1/admin/projects/large-enqueue-e2e" >"${run_dir}/large-project.json"
python3 -c 'import json,sys; json.dump({"jobs":[{"id":f"large-{i}","value":str(i)} for i in range(300)]},open(sys.argv[1],"w"))' "${run_dir}/large-enqueue.json"
curl --fail --silent --show-error "${admin[@]}" "${json[@]}" \
  --data-binary "@${run_dir}/large-enqueue.json" \
  "${base}/api/v1/admin/projects/large-enqueue-e2e/jobs" >"${run_dir}/large-enqueue-result.json"
python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); assert r["submitted"] == 300 and r["inserted"] == 300' "${run_dir}/large-enqueue-result.json"
status=$(curl --silent --output /dev/null --write-out '%{http_code}' "${admin[@]}" -X DELETE \
  "${base}/api/v1/admin/projects/large-enqueue-e2e")
test "${status}" = 204

status=$(curl --silent --output /dev/null --write-out '%{http_code}' "${admin[@]}" "${json[@]}" \
  -X PUT -d '{"status":"active","roles":["worker"]}' "${base}/api/v1/admin/users/api-worker")
test "${status}" = 204
curl --fail --silent --show-error "${admin[@]}" "${json[@]}" -X POST \
  "${base}/api/v1/admin/users/api-worker/machine-token" >"${run_dir}/api-worker-token.json"
python3 -c 'import json,sys; t=json.load(open(sys.argv[1])); assert t["user_id"] == "api-worker" and t["token"].startswith("hq_")' "${run_dir}/api-worker-token.json"
curl --fail --silent --show-error "${admin[@]}" \
  "${base}/api/v1/admin/users/api-worker/machine-token" >"${run_dir}/api-worker-token-view.json"
python3 -c 'import json,sys; assert json.load(open(sys.argv[1])) == json.load(open(sys.argv[2]))' "${run_dir}/api-worker-token.json" "${run_dir}/api-worker-token-view.json"
worker_token=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["token"])' "${run_dir}/api-worker-token.json")
status=$(curl --silent --output /dev/null --write-out '%{http_code}' "${base}/api/v1/whoami")
test "${status}" = 401
curl --fail --silent --show-error -H "Authorization: Bearer ${worker_token}" \
  "${base}/api/v1/whoami" >"${run_dir}/whoami.json"
python3 -c 'import json,sys; assert json.load(open(sys.argv[1])) == {"user_id":"api-worker"}' "${run_dir}/whoami.json"
curl --fail --silent --show-error "${admin[@]}" "${base}/api/v1/admin/users" >"${run_dir}/users.json"
python3 -c 'import json,sys; assert any(u["id"] == "api-worker" and u["machine_token_active"] and u["machine_token_viewable"] for u in json.load(open(sys.argv[1]))["users"])' "${run_dir}/users.json"
status=$(curl --silent --output /dev/null --write-out '%{http_code}' "${admin[@]}" -X DELETE \
  "${base}/api/v1/admin/users/api-worker/machine-token")
test "${status}" = 204
status=$(curl --silent --output /dev/null --write-out '%{http_code}' "${admin[@]}" -X DELETE \
  "${base}/api/v1/admin/users/api-worker")
test "${status}" = 204
curl --fail --silent --show-error "${admin[@]}" "${base}/api/v1/admin/users" >"${run_dir}/users-after-delete.json"
python3 -c 'import json,sys; assert all(u["id"] != "api-worker" for u in json.load(open(sys.argv[1]))["users"])' "${run_dir}/users-after-delete.json"

jobs='{"jobs":[
  {"id":"seed-1","value":"https://example.test/seed/1","type":"seed","via":null,"attr":{"group":"seed"}},
  {"id":"seed-2","value":"https://example.test/seed/2","type":"seed","via":null,"attr":{"group":"seed"}},
  {"id":"asset-1","value":"https://example.test/asset/1","type":"asset","via":"https://example.test/seed/1","attr":{"group":"asset"}},
  {"id":"asset-2","value":"https://example.test/asset/2","type":"asset","via":"https://example.test/seed/1","attr":{"group":"asset"}}
]}'
curl --fail --silent --show-error "${admin[@]}" "${json[@]}" -d "${jobs}" \
  "${base}/api/v1/admin/projects/project-e2e/jobs" >"${run_dir}/enqueue.json"
python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); assert r["submitted"] == 4 and r["inserted"] == 4' "${run_dir}/enqueue.json"

# The same immutable batch is idempotent.
curl --fail --silent --show-error "${admin[@]}" "${json[@]}" -d "${jobs}" \
  "${base}/api/v1/admin/projects/project-e2e/jobs" >"${run_dir}/reenqueue.json"
python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); assert r["submitted"] == 4 and r["inserted"] == 0' "${run_dir}/reenqueue.json"

curl --fail --silent --show-error "${admin[@]}" \
  "${base}/api/v1/admin/projects" >"${run_dir}/projects.json"
python3 -c 'import json,sys; ps=json.load(open(sys.argv[1]))["projects"]; assert len(ps) == 1 and ps[0]["job_counts"]["todo"] == 4' "${run_dir}/projects.json"

# Claim only seeds, extend one lease, complete one, and retry the other.
curl --fail --silent --show-error "${worker[@]}" "${json[@]}" \
  -d '{"worker_id":"worker-e2e","max_jobs":2,"lease_seconds":300,"accept_types":["seed"],"policy_version":1}' \
  "${base}/api/v1/projects/project-e2e/jobs/claim" >"${run_dir}/seeds.json"
read -r seed1 attempt1 seed2 attempt2 < <(python3 -c 'import json,sys; j=json.load(open(sys.argv[1]))["jobs"]; assert len(j)==2 and all(x["type"]=="seed" for x in j); print(j[0]["job_id"],j[0]["attempt_id"],j[1]["job_id"],j[1]["attempt_id"])' "${run_dir}/seeds.json")

printf '{"worker_id":"worker-e2e","extend_seconds":120,"items":[{"job_id":%s,"attempt_id":"%s"}]}' \
  "${seed1}" "${attempt1}" >"${run_dir}/extend-request.json"
curl --fail --silent --show-error "${worker[@]}" "${json[@]}" \
  --data-binary "@${run_dir}/extend-request.json" \
  "${base}/api/v1/projects/project-e2e/jobs/extend-lease" >"${run_dir}/extend.json"
python3 -c 'import json,sys; r=json.load(open(sys.argv[1]))["results"][0]; assert r["status"] == "applied" and r["job_status"] == "wip" and r["lease_expires_at"] > 0' "${run_dir}/extend.json"

accepted_at=$(date +%s)
printf '{"worker_id":"worker-e2e","items":[{"job_id":%s,"attempt_id":"%s","outcome":{"kind":"success","code":200,"uri":null,"meta":{}},"warc_receipts":[{"id":"receipt-seed-1","issuer":"https://warc.example","object_id":"warc-seed-1","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size_bytes":123,"accepted_at":%s,"signature":"test-signature"}]}]}' \
  "${seed1}" "${attempt1}" "${accepted_at}" >"${run_dir}/complete-seed.json"
curl --fail --silent --show-error "${worker[@]}" "${json[@]}" \
  --data-binary "@${run_dir}/complete-seed.json" \
  "${base}/api/v1/projects/project-e2e/jobs/complete" >"${run_dir}/complete-seed-result.json"
python3 -c 'import json,sys; assert json.load(open(sys.argv[1]))["results"][0]["status"] == "applied"' "${run_dir}/complete-seed-result.json"

printf '{"worker_id":"worker-e2e","items":[{"job_id":%s,"attempt_id":"%s","retryable":true,"error":{"code":"temporary","message":"retry once","details":{}}}]}' \
  "${seed2}" "${attempt2}" >"${run_dir}/retry-seed.json"
curl --fail --silent --show-error "${worker[@]}" "${json[@]}" \
  --data-binary "@${run_dir}/retry-seed.json" \
  "${base}/api/v1/projects/project-e2e/jobs/fail" >"${run_dir}/retry-seed-result.json"
python3 -c 'import json,sys; r=json.load(open(sys.argv[1]))["results"][0]; assert r["status"] == "applied" and r["job_status"] == "todo"' "${run_dir}/retry-seed-result.json"

# A late completion from the old attempt must be rejected.
printf '{"worker_id":"worker-e2e","items":[{"job_id":%s,"attempt_id":"%s","outcome":{"kind":"success","code":200,"uri":null,"meta":{}},"warc_receipts":[]}]}' \
  "${seed2}" "${attempt2}" >"${run_dir}/stale.json"
curl --fail --silent --show-error "${worker[@]}" "${json[@]}" \
  --data-binary "@${run_dir}/stale.json" \
  "${base}/api/v1/projects/project-e2e/jobs/complete" >"${run_dir}/stale-result.json"
python3 -c 'import json,sys; r=json.load(open(sys.argv[1]))["results"][0]; assert r["status"] == "rejected" and r["error"]["code"] == "stale_attempt"' "${run_dir}/stale-result.json"

# Claim assets separately; one succeeds and one is permanently failed.
curl --fail --silent --show-error "${worker[@]}" "${json[@]}" \
  -d '{"worker_id":"worker-e2e","max_jobs":2,"lease_seconds":300,"accept_types":["asset"],"policy_version":1}' \
  "${base}/api/v1/projects/project-e2e/jobs/claim" >"${run_dir}/assets.json"
read -r asset1 asset_attempt1 asset2 asset_attempt2 < <(python3 -c 'import json,sys; j=json.load(open(sys.argv[1]))["jobs"]; assert len(j)==2 and all(x["type"]=="asset" for x in j); print(j[0]["job_id"],j[0]["attempt_id"],j[1]["job_id"],j[1]["attempt_id"])' "${run_dir}/assets.json")
printf '{"worker_id":"worker-e2e","items":[{"job_id":%s,"attempt_id":"%s","retryable":false,"error":{"code":"not_found","message":"permanent","details":{}}}]}' \
  "${asset1}" "${asset_attempt1}" >"${run_dir}/fail-asset.json"
curl --fail --silent --show-error "${worker[@]}" "${json[@]}" --data-binary "@${run_dir}/fail-asset.json" \
  "${base}/api/v1/projects/project-e2e/jobs/fail" >"${run_dir}/fail-asset-result.json"
python3 -c 'import json,sys; r=json.load(open(sys.argv[1]))["results"][0]; assert r["status"] == "applied" and r["job_status"] == "failed"' "${run_dir}/fail-asset-result.json"

printf '{"worker_id":"worker-e2e","items":[{"job_id":%s,"attempt_id":"%s","outcome":{"kind":"success","code":200,"uri":null,"meta":{}},"warc_receipts":[]}]}' \
  "${asset2}" "${asset_attempt2}" >"${run_dir}/complete-asset.json"
curl --fail --silent --show-error "${worker[@]}" "${json[@]}" --data-binary "@${run_dir}/complete-asset.json" \
  "${base}/api/v1/projects/project-e2e/jobs/complete" >"${run_dir}/complete-asset-result.json"

# Reclaim and finish the retryable seed.
curl --fail --silent --show-error "${worker[@]}" "${json[@]}" \
  -d '{"worker_id":"worker-e2e","max_jobs":10,"lease_seconds":300,"accept_types":[],"policy_version":1}' \
  "${base}/api/v1/projects/project-e2e/jobs/claim" >"${run_dir}/retry-claim.json"
read -r retry_job retry_attempt < <(python3 -c 'import json,sys; j=json.load(open(sys.argv[1]))["jobs"]; assert len(j)==1; print(j[0]["job_id"],j[0]["attempt_id"])' "${run_dir}/retry-claim.json")
test "${retry_job}" = "${seed2}"
printf '{"worker_id":"worker-e2e","items":[{"job_id":%s,"attempt_id":"%s","outcome":{"kind":"success","code":200,"uri":null,"meta":{}},"warc_receipts":[]}]}' \
  "${retry_job}" "${retry_attempt}" >"${run_dir}/complete-retry.json"
curl --fail --silent --show-error "${worker[@]}" "${json[@]}" --data-binary "@${run_dir}/complete-retry.json" \
  "${base}/api/v1/projects/project-e2e/jobs/complete" >"${run_dir}/complete-retry-result.json"

# Draining stops scheduling, and archived projects reject new jobs.
curl --fail --silent --show-error "${admin[@]}" "${json[@]}" -X PUT -d '{"status":"draining","dispatch_qps":null,"worker_claim_qps":null,"max_jobs_per_claim":256,"max_resets":3,"client_versions":["e2e-v1"]}' \
  "${base}/api/v1/admin/projects/project-e2e" >"${run_dir}/draining.json"
status=$(curl --silent --output "${run_dir}/draining-claim.json" --write-out '%{http_code}' \
  "${worker[@]}" "${json[@]}" -d '{"worker_id":"worker-e2e","max_jobs":1,"lease_seconds":30,"accept_types":[],"policy_version":1}' \
  "${base}/api/v1/projects/project-e2e/jobs/claim")
test "${status}" = 409
python3 -c 'import json,sys; assert json.load(open(sys.argv[1]))["error"]["code"] == "project_not_active"' "${run_dir}/draining-claim.json"

curl --fail --silent --show-error "${admin[@]}" "${json[@]}" -X PUT -d '{"status":"archived","dispatch_qps":null,"worker_claim_qps":null,"max_jobs_per_claim":256,"max_resets":3,"client_versions":["e2e-v1"]}' \
  "${base}/api/v1/admin/projects/project-e2e" >"${run_dir}/archived.json"
status=$(curl --silent --output "${run_dir}/archived-enqueue.json" --write-out '%{http_code}' \
  "${admin[@]}" "${json[@]}" -d '{"jobs":[{"id":"late-job","value":"https://example.test/late","type":"seed","via":null,"attr":{}}]}' \
  "${base}/api/v1/admin/projects/project-e2e/jobs")
test "${status}" = 409
python3 -c 'import json,sys; assert json.load(open(sys.argv[1]))["error"]["code"] == "project_not_active"' "${run_dir}/archived-enqueue.json"

curl --fail --silent --show-error "${admin[@]}" \
  "${base}/api/v1/admin/projects/project-e2e" >"${run_dir}/final-project.json"
python3 -c 'import json,sys; p=json.load(open(sys.argv[1])); assert p["status"] == "archived"; assert p["job_counts"] == {"todo":0,"wip":0,"done":3,"failed":1,"reset_exhausted":0}' "${run_dir}/final-project.json"

database_check=$(docker exec "${container}" psql -U postgres -d hq -Atc \
  "SELECT count(*) FILTER (WHERE status='done'), count(*) FILTER (WHERE status='failed'), count(*) FILTER (WHERE jsonb_array_length(warc_receipts)=1), count(*) FILTER (WHERE attempt_id IS NOT NULL) FROM tracker_jobs WHERE project_id='project-e2e'")
test "${database_check}" = '3|1|1|0'

curl --fail --silent --show-error "${admin[@]}" \
  "${base}/api/v1/admin/projects/project-e2e/jobs?status=failed&limit=10" >"${run_dir}/failed-jobs.json"
python3 -c 'import json,sys; j=json.load(open(sys.argv[1])); assert len(j["jobs"]) == 1 and j["jobs"][0]["status"] == "failed"' "${run_dir}/failed-jobs.json"
curl --fail --silent --show-error "${admin[@]}" \
  "${base}/api/v1/admin/projects/project-e2e/jobs/${asset1}" >"${run_dir}/failed-job.json"
python3 -c 'import json,sys; assert json.load(open(sys.argv[1]))["job_id"] > 0' "${run_dir}/failed-job.json"
status=$(curl --silent --output /dev/null --write-out '%{http_code}' "${admin[@]}" -X POST \
  "${base}/api/v1/admin/projects/project-e2e/jobs/${asset1}/requeue")
test "${status}" = 204
status=$(curl --silent --output /dev/null --write-out '%{http_code}' "${admin[@]}" -X DELETE \
  "${base}/api/v1/admin/projects/project-e2e/jobs/${asset1}")
test "${status}" = 204
status=$(curl --silent --output /dev/null --write-out '%{http_code}' "${admin[@]}" -X DELETE \
  "${base}/api/v1/admin/projects/project-e2e")
test "${status}" = 204
status=$(curl --silent --output /dev/null --write-out '%{http_code}' "${admin[@]}" -X DELETE \
  "${base}/api/v1/admin/users/admin-e2e")
test "${status}" = 204
status=$(curl --silent --output /dev/null --write-out '%{http_code}' "${admin[@]}" \
  "${base}/api/v1/admin/users")
test "${status}" = 401
printf 'SavewebHQ admin and scheduling E2E passed\n'
