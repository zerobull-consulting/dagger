#!/usr/bin/env bash
# Tests of sidecar.sh that need no Docker daemon: its validation, and its
# stop against a fake docker command. CI's sidecar demo job runs it; run it
# the same locally:
#
#   .github/actions/remote-cache-sidecar/sidecar_test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
failures=0
check() {
	local what="$1"
	shift
	if "$@"; then
		echo "ok    $what"
	else
		echo "FAIL  $what"
		failures=$((failures + 1))
	fi
}
not() { ! "$@"; }

# shellcheck source=sidecar.sh
source "$here/sidecar.sh"
# sidecar.sh sets -e; the tests run commands that are meant to fail.
set +e

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# --- validation ---
check "an empty prefix is the whole bucket" valid_prefix ""
check "a prefix of segments" valid_prefix "cache/team-a_1.v2"
for bad in "../cache" "cache/.." "cache/./x" "/cache" "cache/" "cache//x" "ca che" "cache;x" "$(printf 'a%.0s' {1..257})"; do
	check "prefix '${bad:0:40}' is refused" not valid_prefix "$bad"
done
check "namespace default" valid_namespace default
check "namespace with '..' is refused" not valid_namespace "a..b"
check "image by tag and digest" valid_image "ghcr.io/o/i:t@sha256:0123"
check "an image starting with '-' is refused" not valid_image "--privileged"
check "10m is 600s" [ "$(duration_seconds 10m)" = 600 ]
check "1h30m is 5400s" [ "$(duration_seconds 1h30m)" = 5400 ]
check "1.5h is refused" not duration_seconds 1.5h
check "an empty duration is refused" not duration_seconds ""

# start refuses bad inputs before it creates anything (its directory
# included) or touches Docker (a docker that always fails comes first on
# PATH; everything else is as usual).
mkdir -p "$tmp/nodocker"
printf '#!/bin/sh\nexit 1\n' >"$tmp/nodocker/docker"
chmod +x "$tmp/nodocker/docker"
start_refuses() {
	rm -rf "$tmp/refuse"
	! (
		PATH="$tmp/nodocker:$PATH"
		export SIDECAR_ROLE=writer SIDECAR_SERVICE_IMAGE=s SIDECAR_ENGINE_IMAGE=e SIDECAR_DIR="$tmp/refuse"
		export CACHESERVICE_S3_ENDPOINT=http://x CACHESERVICE_S3_REGION=r CACHESERVICE_S3_BUCKET=b
		export CACHESERVICE_S3_ACCESS_KEY_ID=a CACHESERVICE_S3_SECRET_ACCESS_KEY=s
		export "$@"
		start
	) >/dev/null 2>&1 && [[ ! -e "$tmp/refuse" ]]
}
check "start refuses a prefix with '..'" start_refuses CACHESERVICE_S3_PREFIX=cache/../other
check "start refuses s3-path-style 'yes'" start_refuses CACHESERVICE_S3_PATH_STYLE=yes
check "start refuses verbose '1'" start_refuses SIDECAR_VERBOSE=1
check "start refuses role 'admin'" start_refuses SIDECAR_ROLE=admin
check "start refuses a missing secret key" start_refuses CACHESERVICE_S3_SECRET_ACCESS_KEY=

# --- stop, against a fake docker ---
# FAKE_RUNNING_AFTER_STOP: whether the service is still running after
# docker stop. FAKE_EXIT: its exit code.
mkdir -p "$tmp/bin"
cat >"$tmp/bin/docker" <<'EOF'
#!/usr/bin/env bash
state="$FAKE_DIR/service.running"
case "$1 $2" in
"inspect -f")
	case "$3" in
	*Running*) [[ -f "$state" ]] && echo true || echo false ;;
	*ExitCode*) [[ -f "$state" ]] && echo 0 || echo "$FAKE_EXIT" ;;
	esac
	;;
"container inspect") exit 0 ;;
"stop --time")
	[[ "$4" == *-service && "$FAKE_RUNNING_AFTER_STOP" != true ]] && rm -f "$state"
	exit 0
	;;
"logs "*) echo 'time=x level=INFO msg="drain: done" outcome=complete' ;;
*) exit 0 ;;
esac
EOF
chmod +x "$tmp/bin/docker"
stop_with() {
	(
		export PATH="$tmp/bin:$PATH" FAKE_DIR="$tmp/fake" SIDECAR_DIR="$tmp/sc" SIDECAR_NAME=t
		export FAKE_RUNNING_AFTER_STOP="$1" FAKE_EXIT="$2"
		unset GITHUB_OUTPUT GITHUB_STEP_SUMMARY
		mkdir -p "$FAKE_DIR" "$SIDECAR_DIR/t/logs"
		: >"$FAKE_DIR/service.running"
		: >"$SIDECAR_DIR/t/started"
		printf '%s\n' service=t-service engine=t-engine volume=t-engine-state network=t-net \
			stop_seconds=20 "logs=$SIDECAR_DIR/t/logs" role=writer >"$SIDECAR_DIR/t/state"
		stop
	) >/dev/null 2>&1
}
check "a clean stop passes" stop_with false 0
check "a stop that exits 1 fails" not stop_with false 1
check "a stop that exits 137 fails" not stop_with false 137
check "a service still running after docker stop fails the stop" not stop_with true 0

# --- start refuses an engine-state volume that already exists ---
cat >"$tmp/bin/docker-volume" <<'EOF2'
#!/usr/bin/env bash
case "$1 $2" in
"volume inspect") exit 0 ;;               # it exists
"volume create") : >"$FAKE_DIR/volume.created" ;;
"network inspect") echo 10.9.0.1 ;;
"port "*) echo 10.9.0.1:30000 ;;
"inspect -f") echo true ;;
"logs "*) echo 'msg=listening addr=:8090' ;;
*) exit 0 ;;
esac
EOF2
mkdir -p "$tmp/volbin" "$tmp/volfake"
cp "$tmp/bin/docker-volume" "$tmp/volbin/docker"
chmod +x "$tmp/volbin/docker"
existing_volume_refused() {
	! (
		export PATH="$tmp/volbin:$PATH" FAKE_DIR="$tmp/volfake" SIDECAR_DIR="$tmp/vol"
		export SIDECAR_ROLE=writer SIDECAR_SERVICE_IMAGE=s SIDECAR_ENGINE_IMAGE=e
		export CACHESERVICE_S3_ENDPOINT=http://x CACHESERVICE_S3_REGION=r CACHESERVICE_S3_BUCKET=b
		export CACHESERVICE_S3_ACCESS_KEY_ID=a CACHESERVICE_S3_SECRET_ACCESS_KEY=s
		unset GITHUB_ACTIONS GITHUB_ENV GITHUB_OUTPUT
		start
	) >/dev/null 2>&1 && [[ ! -e "$tmp/volfake/volume.created" ]]
}
check "start refuses an engine-state volume that already exists" existing_volume_refused

# --- check.sh stored reports a writer that stored nothing ---
printf '%s\n' 'time=x level=INFO msg="drain: done" outcome=complete' >"$tmp/no-exports.log"
stored_out="$("$here/../../../hack/remote-cache/demo/check.sh" stored "$tmp/no-exports.log" "$tmp/stored" 2>&1)"
stored_rc=$?
check "check.sh stored fails on a log without exports" [ "$stored_rc" -ne 0 ]
check "... and says why" grep -q 'FAIL  the writer stored no export' <<<"$stored_out"

if ((failures > 0)); then
	echo "$failures test(s) failed"
	exit 1
fi
echo "all tests passed"
