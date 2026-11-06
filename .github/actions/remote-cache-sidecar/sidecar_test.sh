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
# The service and the engine run while $FAKE_DIR/<service|engine>.running
# exists. Every call is logged to $FAKE_DIR/calls, and the engine's stop
# records whether the service's drain was done by then. Settings:
#   FAKE_DRAIN    after: the service logs "drain: done" from the second
#                 look at its log after the SIGTERM; never: it never does
#   FAKE_SERVICE  after-engine: it exits once the engine stopped (its
#                 engine.stop arrived); by-itself: 1s after the SIGTERM,
#                 whatever the engine does (its linger's bound); stays:
#                 only on docker stop
#   FAKE_ENGINE   stops, or hangs: docker stop fails, and it keeps running
#   FAKE_EXIT     the service's exit code
mkdir -p "$tmp/bin"
cat >"$tmp/bin/docker" <<'FAKE'
#!/usr/bin/env bash
echo "$*" >>"$FAKE_DIR/calls"
svc="$FAKE_DIR/service.running" eng="$FAKE_DIR/engine.running"
case "$1 $2" in
"inspect -f")
	f="$eng"
	[[ "$4" == *-service ]] && f="$svc"
	case "$3" in
	*Running*) [[ -f "$f" ]] && echo true || echo false ;;
	*ExitCode*) [[ -f "$f" ]] && echo 0 || echo "$FAKE_EXIT" ;;
	esac
	;;
"container inspect") exit 0 ;;
"kill -s")
	if [[ "$FAKE_SERVICE" == by-itself ]]; then
		(sleep 1 && rm -f "$svc") >/dev/null 2>&1 &
	fi
	;;
"logs --since")
	looks=$(($(cat "$FAKE_DIR/looks" 2>/dev/null || echo 0) + 1))
	echo "$looks" >"$FAKE_DIR/looks"
	if [[ "$FAKE_DRAIN" == after && "$looks" -ge 2 ]]; then
		: >"$FAKE_DIR/drained"
		echo 'time=x level=INFO msg="drain: done" outcome=complete'
	fi
	;;
"stop --time")
	case "$4" in
	*-engine)
		if [[ -f "$FAKE_DIR/drained" ]]; then
			echo "ENGINE STOPPED AFTER THE DRAIN" >>"$FAKE_DIR/calls"
		else
			echo "ENGINE STOPPED BEFORE THE DRAIN" >>"$FAKE_DIR/calls"
		fi
		[[ "$FAKE_ENGINE" == hangs ]] && exit 1
		rm -f "$eng"
		[[ "$FAKE_SERVICE" == after-engine ]] && rm -f "$svc"
		;;
	*-service) rm -f "$svc" ;;
	esac
	exit 0
	;;
"logs "*) echo 'time=x level=INFO msg="drain: done" outcome=complete' ;;
*) exit 0 ;;
esac
FAKE
chmod +x "$tmp/bin/docker"
# stop_with <exit code> [FAKE_*=...]: a stop of a running sidecar whose
# state says the service drains for at most 3s and lingers for at most 1s,
# so the stop waits 3+5s for the drain and 3+1+10s in all. It leaves its
# output in $tmp/stop.out, the calls in $tmp/fake/calls, and how long it
# took, in seconds, in $tmp/stop.took.
stop_with() {
	local code="$1"
	shift
	rm -rf "$tmp/fake" "$tmp/sc"
	local t=$SECONDS rc
	(
		export PATH="$tmp/bin:$PATH" FAKE_DIR="$tmp/fake" SIDECAR_DIR="$tmp/sc" SIDECAR_NAME=t
		export FAKE_DRAIN=after FAKE_SERVICE=after-engine FAKE_ENGINE=stops FAKE_EXIT="$code"
		(($#)) && export "$@"
		unset GITHUB_OUTPUT GITHUB_STEP_SUMMARY
		mkdir -p "$FAKE_DIR" "$SIDECAR_DIR/t/logs"
		: >"$FAKE_DIR/service.running"
		: >"$FAKE_DIR/engine.running"
		printf '%s\n' service=t-service engine=t-engine volume=t-engine-state network=t-net \
			stop_seconds=14 linger_seconds=1 "logs=$SIDECAR_DIR/t/logs" role=writer \
			created=network created=service created=volume created=engine >"$SIDECAR_DIR/t/state"
		stop
	) >"$tmp/stop.out" 2>&1
	rc=$?
	echo $((SECONDS - t)) >"$tmp/stop.took"
	return $rc
}
calls() { cat "$tmp/fake/calls" 2>/dev/null; }
called() { calls | grep -qxF -- "$1"; }
# Whether there were calls matching each pattern, in this order.
in_order() {
	local prev=0 n pattern
	for pattern in "$@"; do
		n="$(calls | grep -n -F -- "$pattern" | cut -d: -f1 | awk -v p="$prev" '$1 > p { print; exit }')"
		[[ -n "$n" ]] || return 1
		prev="$n"
	done
}
cleaned_up() { called "rm -f t-engine" && called "volume rm -f t-engine-state" && called "network rm t-net" && [[ ! -e "$tmp/sc/t/state" ]]; }
took_at_most() { [[ "$(cat "$tmp/stop.took")" -le "$1" ]]; }

check "a clean stop passes" stop_with 0
check "... it signals the service, waits for its drain, stops the engine, then waits for the service" \
	in_order "kill -s TERM t-service" "logs --since" "stop --time 30 t-engine" "inspect -f {{.State.Running}} t-service"
check "... the engine is stopped only once the drain is done" called "ENGINE STOPPED AFTER THE DRAIN"
check "... the service gets no second signal" not called "stop --time 5 t-service"
check "... in seconds" took_at_most 3
check "... and keeps both logs" [ -f "$tmp/sc/t/logs/service.log" -a -f "$tmp/sc/t/logs/engine.log" ]
check "a stop that exits 1 fails" not stop_with 1
check "a stop that exits 137 fails" not stop_with 137

check "a service that never reports its drain done: the stop still passes" stop_with 0 FAKE_DRAIN=never
check "... it waits for the drain only up to its bound" took_at_most 10
check "... then stops the engine" called "ENGINE STOPPED BEFORE THE DRAIN"
check "... says so" grep -q "didn't report its drain done in time" "$tmp/stop.out"
check "... and cleans up" cleaned_up

check "a service that doesn't exit by the stop's bound fails the stop" not stop_with 0 FAKE_SERVICE=stays
check "... after waiting for it only up to the bound" took_at_most 18
check "... it was stopped" called "stop --time 5 t-service"
check "... says why" grep -q "past its own bound" "$tmp/stop.out"
check "... and cleans up" cleaned_up

check "an engine that won't stop: the stop still passes once the service exits" stop_with 0 FAKE_ENGINE=hangs FAKE_SERVICE=by-itself
check "... says so" grep -q "the engine was still running after docker stop" "$tmp/stop.out"
check "... and removes the engine" cleaned_up

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
		stop_seconds=20 linger_seconds=15 "logs=$tmp/rm/sc/t/logs" role=writer $created >"$tmp/rm/sc/t/state"
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
relative_dir_absolute() {
	(cd "$tmp" && fake_start dbgbin SIDECAR_NAME=rel SIDECAR_DIR=rel.sc SIDECAR_DEBUG_PORT=8091) >/dev/null
	grep -qxF -- "$tmp/rel.sc/rel/secrets/debug-token:/run/secrets/debug-token:ro" "$tmp/dbgbin.fake/service.args"
}
check "debug-port: a relative SIDECAR_DIR still mounts the token by an absolute path" relative_dir_absolute
check "debug-port: the state records the token, for the stop" grep -qx created=debug-token "$tmp/dbgbin.sc/remote-cache/state"
# A token file already there, not this start's: refused, and left alone.
existing_token_refused() {
	mkdir -p "$tmp/dbgbin.sc/pre/secrets" && echo theirs >"$tmp/dbgbin.sc/pre/secrets/debug-token"
	! (fake_start dbgbin SIDECAR_NAME=pre SIDECAR_DEBUG_PORT=8091 >/dev/null 2>&1) &&
		[ "$(cat "$tmp/dbgbin.sc/pre/secrets/debug-token")" = theirs ] &&
		! grep -qx created=debug-token "$tmp/dbgbin.sc/pre/state" 2>/dev/null
}
check "debug-port: a token file already there is refused, and left alone" existing_token_refused
existing_dir_refused() {
	mkdir -p "$tmp/dbgbin.sc/predir/secrets" && chmod 755 "$tmp/dbgbin.sc/predir/secrets"
	! (fake_start dbgbin SIDECAR_NAME=predir SIDECAR_DEBUG_PORT=8091 >/dev/null 2>&1) &&
		[ "$(stat -c %a "$tmp/dbgbin.sc/predir/secrets")" = 755 ] && [ -z "$(ls -A "$tmp/dbgbin.sc/predir/secrets")" ]
}
check "debug-port: a secrets directory already there is refused, and its mode left alone" existing_dir_refused
# The stop removes the token only when the state says this start wrote it,
# and nothing else in its directory.
stop_secrets() {
	local created="$1"
	rm -rf "$tmp/st" && mkdir -p "$tmp/st/fake" "$tmp/st/sc/t/logs" "$tmp/st/sc/t/secrets"
	echo x >"$tmp/st/sc/t/secrets/debug-token"
	[[ "$2" == other ]] && echo y >"$tmp/st/sc/t/secrets/other"
	printf '%s\n' service=t-service engine=t-engine volume=t-engine-state network=t-net \
		stop_seconds=20 linger_seconds=15 "logs=$tmp/st/sc/t/logs" role=writer $created >"$tmp/st/sc/t/state"
	(
		export PATH="$tmp/rmbin:$PATH" FAKE_DIR="$tmp/st/fake" SIDECAR_DIR="$tmp/st/sc" SIDECAR_NAME=t
		unset GITHUB_OUTPUT GITHUB_STEP_SUMMARY
		stop
	) >/dev/null 2>&1
}
stop_secrets created=debug-token none
check "the stop removes the token this start wrote, and its empty directory" [ ! -e "$tmp/st/sc/t/secrets" ]
stop_secrets created=debug-token other
check "... but not another file next to it" [ ! -e "$tmp/st/sc/t/secrets/debug-token" -a -e "$tmp/st/sc/t/secrets/other" ]
stop_secrets "" none
check "a start that wrote no token: the stop leaves the file" [ -e "$tmp/st/sc/t/secrets/debug-token" ]

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
