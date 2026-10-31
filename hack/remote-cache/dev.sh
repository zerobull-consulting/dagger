#!/usr/bin/env bash
# Local MinIO for the remote cache service. See README.md in this directory.
#
#   dev.sh up      start MinIO, create the bucket, print the settings
#   dev.sh down    stop it and keep the data
#   dev.sh reset   stop it and delete the data
#   dev.sh status  show the containers
#   dev.sh env     print the settings as shell exports
#   dev.sh test    run the Go test against it (starts it first)
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

print_env() {
	cat <<EOF
export CACHESERVICE_S3_ENDPOINT=http://${host}:${MINIO_API_PORT}
export CACHESERVICE_S3_REGION=${MINIO_REGION}
export CACHESERVICE_S3_BUCKET=${MINIO_BUCKET}
export CACHESERVICE_S3_ACCESS_KEY_ID=${MINIO_ROOT_USER}
export CACHESERVICE_S3_SECRET_ACCESS_KEY=${MINIO_ROOT_PASSWORD}
export CACHESERVICE_S3_PATH_STYLE=true
EOF
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
	go test -count=1 -run 'TestMinIO' -v ./cmd/cacheservice/
	;;
*)
	sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
	;;
esac
