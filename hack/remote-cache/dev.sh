#!/usr/bin/env bash
# Local MinIO for the remote cache service. See README.md in this directory.
#
#   dev.sh up      start MinIO, create the bucket, print the settings
#   dev.sh down    stop it and keep the data
#   dev.sh reset   stop it and delete the data
#   dev.sh status  show the containers
#   dev.sh env     print the settings as shell exports
#   dev.sh test    run the Go tests against it (starts it first)
#   dev.sh e2e     run the remote cache with real engines (cmd/cacheservice/e2e)
#   dev.sh e2e-down  remove the engines a run with E2E_KEEP=1 left
#
# Address, ports, credentials and bucket come from MINIO_BIND_ADDR,
# MINIO_API_PORT, MINIO_CONSOLE_PORT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD,
# MINIO_REGION and MINIO_BUCKET. MinIO listens on loopback unless
# MINIO_BIND_ADDR says otherwise.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"

: "${MINIO_BIND_ADDR:=127.0.0.1}"
: "${MINIO_API_PORT:=9000}"
: "${MINIO_CONSOLE_PORT:=9001}"
: "${MINIO_ROOT_USER:=dagger-dev}"
: "${MINIO_ROOT_PASSWORD:=dagger-dev-secret}"
: "${MINIO_REGION:=us-east-1}"
: "${MINIO_BUCKET:=dagger-cache}"
export MINIO_BIND_ADDR MINIO_API_PORT MINIO_CONSOLE_PORT MINIO_ROOT_USER MINIO_ROOT_PASSWORD MINIO_REGION MINIO_BUCKET

# The host the ports are published on, as clients should address it.
host="$MINIO_BIND_ADDR"
case "$host" in
127.0.0.1 | 0.0.0.0) host=localhost ;;
esac

compose() {
	docker compose -f "$here/compose.yaml" "$@"
}

# Values are quoted with %q, so the output is safe to eval whatever the
# settings contain.
print_env() {
	local name
	for name in ENDPOINT REGION BUCKET ACCESS_KEY_ID SECRET_ACCESS_KEY PATH_STYLE; do
		printf 'export CACHESERVICE_S3_%s=%q\n' "$name" "$(s3_setting "$name")"
	done
}

s3_setting() {
	case "$1" in
	ENDPOINT) echo "http://${host}:${MINIO_API_PORT}" ;;
	REGION) echo "$MINIO_REGION" ;;
	BUCKET) echo "$MINIO_BUCKET" ;;
	ACCESS_KEY_ID) echo "$MINIO_ROOT_USER" ;;
	SECRET_ACCESS_KEY) echo "$MINIO_ROOT_PASSWORD" ;;
	PATH_STYLE) echo true ;;
	esac
}

up() {
	compose up -d --wait minio
	compose run --rm create-bucket >/dev/null
	echo "MinIO is up with bucket '${MINIO_BUCKET}'."
	echo "  S3 API:  http://${host}:${MINIO_API_PORT}"
	echo "  Console: http://${host}:${MINIO_CONSOLE_PORT} (${MINIO_ROOT_USER} / ${MINIO_ROOT_PASSWORD})"
	echo
	echo "Settings for the service and its tests (eval \"\$($0 env)\"):"
	print_env
}

case "${1:-}" in
up) up ;;
down) compose down ;;
reset) compose down --volumes ;;
status) compose ps -a ;;
env) print_env ;;
test)
	up >/dev/null
	eval "$(print_env)"
	cd "$repo"
	go test -count=1 -run 'TestMinIO' -v ./cmd/cacheservice/...
	;;
e2e)
	# Extra arguments go to go test, such as -run 'TestRealEngines/^renew$'.
	# E2E_SCENARIOS picks the scenarios after the first, which always runs.
	scenarios="${E2E_SCENARIOS:-}"
	for sc in $scenarios; do
		case "$sc" in
		reexport | renew | prune | superseded | namespaces | disconnect | engine-restart | s3-faults | service-restart) ;;
		*)
			echo "dev.sh e2e: unknown scenario '$sc' in E2E_SCENARIOS (known: reexport renew prune superseded namespaces disconnect engine-restart s3-faults service-restart)" >&2
			exit 2
			;;
		esac
	done
	run=TestRealEngines
	if [[ -n "${scenarios// /}" ]]; then
		run="TestRealEngines/^($(tr -s ' ' '|' <<<"$scenarios" | sed 's/^|//; s/|$//'))\$"
	fi
	up >/dev/null
	eval "$(print_env)"
	cd "$repo"
	CACHESERVICE_E2E=1 go test -count=1 -v -timeout 60m -run "$run" ./cmd/cacheservice/e2e/ "${@:2}"
	;;
e2e-down)
	label=dagger-remote-cache-e2e
	docker ps -aq --filter "label=$label" | xargs -r docker rm -f >/dev/null
	docker volume ls -q --filter "label=$label" | xargs -r docker volume rm -f >/dev/null
	;;
*)
	sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
	;;
esac
