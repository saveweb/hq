#!/usr/bin/env bash
set -Eeuo pipefail

HQ_URL="${HQ_URL:-https://hq.saveweb.org}"

fail() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

for command_name in curl python3 bash; do
    command -v "$command_name" >/dev/null 2>&1 || fail "$command_name is required"
done
[[ -r /dev/tty && -w /dev/tty ]] || fail "an interactive terminal is required"

authorization_json="$(curl --fail --silent --show-error --request POST "$HQ_URL/api/v1/device/authorizations")"
IFS=$'\t' read -r device_code verification_url expires_in interval < <(
    python3 -c 'import json,sys; value=json.load(sys.stdin); print(value["device_code"], value["verification_uri_complete"], value["expires_in"], value["interval"], sep="\t")' <<<"$authorization_json"
)
unset authorization_json

[[ ${#device_code} -eq 43 ]] || fail "HQ returned an invalid device code"
[[ "$expires_in" =~ ^[0-9]+$ && "$interval" =~ ^[0-9]+$ && "$interval" -gt 0 ]] || fail "HQ returned invalid polling settings"

printf '\nOpen this URL to authorize the shell:\n\n%s\n\n' "$verification_url"
printf 'Only an active account with the worker role and an existing machine token will be accepted.\n'

deadline=$(( $(date +%s) + expires_in ))
machine_token=""
while (( $(date +%s) < deadline )); do
    response_json="$(curl --silent --show-error --request POST --data-urlencode "device_code=$device_code" "$HQ_URL/api/v1/device/token")"
    status="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])' <<<"$response_json")"
    case "$status" in
        authorized)
            machine_token="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["machine_token"])' <<<"$response_json")"
            break
            ;;
        authorization_pending)
            sleep "$interval"
            ;;
        access_denied)
            fail "HQ denied authorization; verify the account status, worker role, and machine token"
            ;;
        *)
            fail "HQ returned an unknown authorization status"
            ;;
    esac
done
unset response_json device_code

[[ "$machine_token" == hq_* ]] || fail "Authorization expired or HQ returned an invalid machine token"
export HQ_MACHINE_TOKEN="$machine_token"
unset machine_token

printf '\nHQ_MACHINE_TOKEN is set in a new Bash shell. Exit that shell to remove it.\n\n'
exec bash -i </dev/tty >/dev/tty 2>&1
