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
		printf '%s\n' service=t-service engine=t-engine volume=t-engine-state network=t-net \
			stop_seconds=20 "logs=$SIDECAR_DIR/t/logs" role=writer \
			created=network created=service created=volume created=engine >"$SIDECAR_DIR/t/state"
		stop
	) >/dev/null 2>&1
}
check "a clean stop passes" stop_with false 0
check "a stop that exits 1 fails" not stop_with false 1
check "a stop that exits 137 fails" not stop_with false 137
check "a service still running after docker stop fails the stop" not stop_with true 0

# --- the stop removes only what the start created ---
# A docker that logs every removal, and finds every container stopped.
mkdir -p "$tmp/rmbin"
cat >"$tmp/rmbin/docker" <<'EOF2'
#!/usr/bin/env bash
case "$1" in
rm | volume | network) [[ "$1 $2" == "volume inspect" || "$1 $2" == "network inspect" ]] || echo "$*" >>"$FAKE_DIR/removed" ;;
esac
case "$1 $2" in
"inspect -f") echo false ;;
*) exit 0 ;;
esac
EOF2
chmod +x "$tmp/rmbin/docker"
removed_after_stop() {
	local created="$1"
	rm -rf "$tmp/rm" && mkdir -p "$tmp/rm/fake" "$tmp/rm/sc/t/logs"
	printf '%s\n' service=t-service engine=t-engine volume=t-engine-state network=t-net \
		stop_seconds=20 "logs=$tmp/rm/sc/t/logs" role=writer $created >"$tmp/rm/sc/t/state"
	(
		export PATH="$tmp/rmbin:$PATH" FAKE_DIR="$tmp/rm/fake" SIDECAR_DIR="$tmp/rm/sc" SIDECAR_NAME=t
		unset GITHUB_OUTPUT GITHUB_STEP_SUMMARY
		stop
	) >/dev/null 2>&1
	cat "$tmp/rm/fake/removed" 2>/dev/null | tr '\n' ';'
}
check "a start that created only the network: the stop removes only it" \
	[ "$(removed_after_stop created=network)" = "network rm t-net;" ]
check "a start that created nothing: the stop removes nothing" [ -z "$(removed_after_stop "")" ]

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
	) >/dev/null 2>&1 && [[ ! -e "$tmp/volfake/volume.created" && ! -e "$tmp/vol/remote-cache/state" ]]
}
check "start refuses an engine-state volume that already exists, and writes no state" existing_volume_refused

# --- start's Docker calls are bounded, and its failures show both logs ---
# A docker whose "image inspect" never answers, and whose pull fails.
mkdir -p "$tmp/hangbin"
cat >"$tmp/hangbin/docker" <<'EOF2'
#!/usr/bin/env bash
case "$1 $2" in
"image inspect") sleep 600 ;;
"pull "*) exit 1 ;;
"container inspect" | "volume inspect" | "network inspect") exit 1 ;;   # free names
*) exit 0 ;;
esac
EOF2
chmod +x "$tmp/hangbin/docker"
fake_start() {
	local bin="$1"
	shift
	(
		export PATH="$tmp/$bin:$PATH" FAKE_DIR="$tmp/$bin.fake" SIDECAR_DIR="$tmp/$bin.sc" SIDECAR_START_TIMEOUT=5
		export SIDECAR_ROLE=writer SIDECAR_SERVICE_IMAGE=s SIDECAR_ENGINE_IMAGE=e
		export CACHESERVICE_S3_ENDPOINT=http://x CACHESERVICE_S3_REGION=r CACHESERVICE_S3_BUCKET=b
		export CACHESERVICE_S3_ACCESS_KEY_ID=a CACHESERVICE_S3_SECRET_ACCESS_KEY=s
		(($#)) && export "$@"
		mkdir -p "$FAKE_DIR"
		unset GITHUB_ACTIONS GITHUB_ENV GITHUB_OUTPUT
		start
	) 2>&1
}
image_inspect_bounded() {
	local t=$SECONDS out
	out="$(timeout 60 bash -c "$(declare -f fake_start); tmp='$tmp'; source '$here/sidecar.sh'; set +e; fake_start hangbin SIDECAR_INSPECT_TIMEOUT=2")"
	[[ $((SECONDS - t)) -lt 30 && "$out" == *"pulling s failed"* ]]
}
check "start's image inspect is bounded: a daemon that doesn't answer fails the pull, not the job's timeout" image_inspect_bounded

# A docker whose service runs until the engine starts, then exits.
mkdir -p "$tmp/exitbin"
cat >"$tmp/exitbin/docker" <<'EOF2'
#!/usr/bin/env bash
[[ "$1 $2 $3" == "network inspect -f" ]] && { echo 10.9.0.1; exit 0; }
case "$1 $2" in
"container inspect" | "volume inspect" | "network inspect") exit 1 ;;
"network create" | "volume create") exit 0 ;;
"run -d")
	[[ " $* " == *" --privileged "* ]] && : >"$FAKE_DIR/engine.started"
	exit 0
	;;
"port "*) echo 10.9.0.1:30000 ;;
"inspect -f")
	case "$4" in
	*-service) [[ -f "$FAKE_DIR/engine.started" ]] && echo false || echo true ;;
	*) echo true ;;
	esac
	;;
"logs --tail") echo "LOG OF $4" ;;
"logs "*) echo 'msg=listening addr=:8090' ;;
*) exit 0 ;;
esac
EOF2
chmod +x "$tmp/exitbin/docker"
start_shows_both_logs() {
	local out
	out="$(fake_start exitbin)"
	[[ "$out" == *"the service exited while the engine started"* && "$out" == *"LOG OF remote-cache-engine"* && "$out" == *"LOG OF remote-cache-service"* ]]
}
check "a service that exits while the engine starts fails the start with both logs" start_shows_both_logs

# --- debug-port: a generated token, in a file mounted read-only ---
# A docker that records the service's docker run, then has it exit.
mkdir -p "$tmp/dbgbin"
cat >"$tmp/dbgbin/docker" <<'EOF2'
#!/usr/bin/env bash
[[ "$1 $2 $3" == "network inspect -f" ]] && { echo 10.9.0.1; exit 0; }
case "$1 $2" in
"container inspect" | "volume inspect" | "network inspect") exit 1 ;;
"run -d") printf '%s\n' "$@" >"$FAKE_DIR/service.args"; exit 0 ;;
"port "*) echo 10.9.0.1:30000 ;;
"inspect -f") echo false ;;
*) exit 0 ;;
esac
EOF2
chmod +x "$tmp/dbgbin/docker"
debug_out="$(fake_start dbgbin SIDECAR_DEBUG_PORT=8091)"
dbg_dir="$tmp/dbgbin.sc/remote-cache/secrets"
dbg_token="$(cat "$dbg_dir/debug-token" 2>/dev/null)"
dbg_args="$(cat "$tmp/dbgbin.fake/service.args" 2>/dev/null)"
check "debug-port: the token is 64 hex digits" grep -qxE "[0-9a-f]{64}" <<<"$dbg_token"
check "debug-port: its file is readable by the service's UID" [ "$(stat -c %a "$dbg_dir/debug-token")" = 644 ]
check "debug-port: its directory only by this user" [ "$(stat -c %a "$dbg_dir")" = 700 ]
check "debug-port: the service gets the file, read-only" grep -qxF -- "$dbg_dir/debug-token:/run/secrets/debug-token:ro" <<<"$dbg_args"
check "debug-port: and -debug-token-file" grep -qxF -- "-debug-token-file" <<<"$dbg_args"
check "debug-port: published on the host's loopback only" grep -qxF -- "127.0.0.1:8091:8091" <<<"$dbg_args"
check "debug-port: the token is on no command line" not grep -qF -- "$dbg_token" <<<"$dbg_args"
check "debug-port: nor in the start's output" not grep -qF -- "$dbg_token" <<<"$debug_out"
check "without debug-port: no token" bash -c "$(declare -f fake_start); tmp='$tmp'; source '$here/sidecar.sh'; set +e; fake_start dbgbin SIDECAR_NAME=nodebug >/dev/null; ! grep -q -- -debug-token-file '$tmp/dbgbin.fake/service.args' && [ ! -e '$tmp/dbgbin.sc/nodebug/secrets' ]"
# The stop removes the token.
mkdir -p "$tmp/sc/t/secrets" && echo x >"$tmp/sc/t/secrets/debug-token"
stop_with false 0
check "the stop removes the debug token" [ ! -e "$tmp/sc/t/secrets" ]

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
