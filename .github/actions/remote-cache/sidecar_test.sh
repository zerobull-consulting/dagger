#!/usr/bin/env bash
# Tests of sidecar.sh that need no Docker daemon: its validation, the
# images its version selects, its start and its stop against a fake docker
# command. CI's sidecar demo job runs it; run it
# the same locally:
#
#   .github/actions/remote-cache/sidecar_test.sh
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
check "a docker call in a wait is bounded by what is left of it" [ "$(step_bound $((SECONDS + 3)))" -le 3 ]
check "... at most 10s" [ "$(step_bound $((SECONDS + 600)))" = 10 ]
check "... and at least 1s, past its end" [ "$(step_bound $((SECONDS - 5)))" = 1 ]
# zerobull-consulting/dagger#209: under start/run (SIDECAR_GROUP_KILLED=1), a
# bounded docker call stays in the script's process group, which start/run
# kills at its bound (GNU timeout alone moves to a group of its own). Run any
# other way, timeout is GNU's own, which kills its command's children too.
# The process group of a /proc/<pid>/stat line on stdin: the 3rd field after
# the command's name, which is in parentheses.
stat_pgid() {
	local s
	s="$(cat)"
	s="${s##*) }"
	read -r _ _ pgid _ <<<"$s"
	echo "${pgid:-}"
}
timeout_keeps_group() {
	local inner outer
	inner="$(timeout 5 sh -c 'cat /proc/$$/stat' | stat_pgid)"
	outer="$(stat_pgid </proc/$$/stat)"
	[[ -n "$inner" && "$inner" == "$outer" ]]
}
# A fresh bash that sourced sidecar.sh with the settings given, running CMD.
sourced() {
	local cmd="$1"
	shift
	env "$@" bash -c "source '$here/sidecar.sh'; set +e; $(declare -f stat_pgid timeout_keeps_group); $cmd"
}
which_timeout='if declare -F timeout >/dev/null; then echo wrapped; else echo plain; fi'
check "under start/run, a timeout call stays in the script's process group" \
	sourced timeout_keeps_group SIDECAR_GROUP_KILLED=1
check "... and run any other way, timeout is not wrapped" \
	[ "$(sourced "$which_timeout" SIDECAR_GROUP_KILLED=)" = plain ]
# A timeout with no --foreground (BusyBox's) is used as it is, not wrapped.
mkdir -p "$tmp/bbtimeout"
cat >"$tmp/bbtimeout/timeout" <<'FAKE'
#!/bin/sh
case "$1" in -*) echo "timeout: unrecognized option '$1'" >&2; exit 125 ;; esac
shift
exec "$@"
FAKE
chmod +x "$tmp/bbtimeout/timeout"
check "... and under start/run, a timeout with no --foreground still runs the command" \
	[ "$(sourced 'timeout 5 echo ran' SIDECAR_GROUP_KILLED=1 PATH="$tmp/bbtimeout:$PATH" 2>&1)" = ran ]
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
		export SIDECAR_ROLE=writer SIDECAR_DIR="$tmp/refuse"
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
# zerobull-consulting/dagger#104: the engine's config is checked with the
# other inputs, before the directory, the network or any container.
check "start refuses an engine-config that isn't a file, before it creates anything" start_refuses SIDECAR_ENGINE_CONFIG="$tmp/no-such-engine.json"
: >"$tmp/a:b.json" # it exists: only the ':' check can refuse it
check "start refuses an engine-config whose path has a ':', before it creates anything" start_refuses SIDECAR_ENGINE_CONFIG="$tmp/a:b.json"

# --- the images the version selects ---
engines="$here/../../../hack/remote-cache/engines.sh"
pin_of() { "$engines" pin ${1:+"$1"} | sed -n 's/^STOCK_ENGINE=//p'; }
first_version="$("$engines" pin | sed -n 's/^STOCK_ENGINE_TAG=//p')"
# Every accept line, the first and the rest.
mapfile -t accepted < <("$engines" versions | tr -d '[]"' | tr ',' '\n')
# selected [VAR=value...]: what select_images picks, as "version|service|engine".
selected() {
	(
		unset SIDECAR_VERSION SIDECAR_SERVICE_IMAGE SIDECAR_ENGINE_IMAGE GITHUB_ACTIONS
		(($#)) && export "$@"
		select_images && echo "$version|$service_image|$engine_image"
	) 2>"$tmp/selected.err"
}
check "no version: the manifest's first accept line selects both images" \
	[ "$(selected)" = "$first_version|ghcr.io/zerobull-consulting/dagger-remote-cache:$first_version|$(pin_of)" ]
for v in "${accepted[@]}"; do
	check "version $v selects its engine by the manifest's digest, and dagger-remote-cache:$v" \
		[ "$(selected SIDECAR_VERSION="$v")" = "$v|ghcr.io/zerobull-consulting/dagger-remote-cache:$v|$(pin_of "$v")" ]
done
check "... the engine image is pinned by digest" grep -qE '^registry\.dagger\.io/engine:v[^@]+@sha256:[0-9a-f]{64}$' <<<"$(pin_of)"
check "service-image overrides the service; the version still selects the engine" \
	[ "$(selected SIDECAR_VERSION="${accepted[-1]}" SIDECAR_SERVICE_IMAGE=cacheservice:ci)" = "${accepted[-1]}|cacheservice:ci|$(pin_of "${accepted[-1]}")" ]
check "an unknown version is refused" not selected SIDECAR_VERSION=v0.0.1
check "... and says which versions are accepted" grep -qF "it accepts only ${accepted[*]}" "$tmp/selected.err"
check "a prefix of an accepted version is refused" not selected SIDECAR_VERSION="${first_version%?}"
check "a version that isn't one is refused" not selected SIDECAR_VERSION=--privileged
check "... without repeating it" not grep -qF -- --privileged "$tmp/selected.err"
# zerobull-consulting/dagger#174: the start action checks the version with
# `sidecar.sh version`, the rule start runs (before, it ran engines.sh itself).
version_cmd() { (unset SIDECAR_VERSION SIDECAR_ENGINE_IMAGE SIDECAR_SERVICE_IMAGE; (($#)) && export "$@"; "$here/sidecar.sh" version 2>"$tmp/version.err"); }
check "sidecar.sh version: none is the manifest's first accept line" [ "$(version_cmd)" = "$first_version" ]
check "... an accepted version is itself" [ "$(version_cmd SIDECAR_VERSION="${accepted[-1]}")" = "${accepted[-1]}" ]
check "... an unknown version is refused, with the error start gives" not version_cmd SIDECAR_VERSION=v0.0.1
check "... which says what is accepted" grep -qF "it accepts only ${accepted[*]}" "$tmp/version.err"
check "... a service-image or engine-image in the environment changes nothing" \
	[ "$(version_cmd SIDECAR_ENGINE_IMAGE=e:1 SIDECAR_SERVICE_IMAGE=s:1)" = "$first_version" ]
check "the deprecated engine-image with service-image: both as given" \
	[ "$(selected SIDECAR_ENGINE_IMAGE=e:1 SIDECAR_SERVICE_IMAGE=s:1)" = "|s:1|e:1" ]
check "... with a warning" grep -q "warning: engine-image is deprecated" "$tmp/selected.err"
check "engine-image with version is refused" not selected SIDECAR_ENGINE_IMAGE=e:1 SIDECAR_SERVICE_IMAGE=s:1 SIDECAR_VERSION="$first_version"
check "engine-image without service-image is refused" not selected SIDECAR_ENGINE_IMAGE=e:1
check "a caller's ENGINES_FILE doesn't change the manifest read" \
	[ "$(selected ENGINES_FILE=/nonexistent)" = "$first_version|ghcr.io/zerobull-consulting/dagger-remote-cache:$first_version|$(pin_of)" ]
# Before anything is created or pulled.
check "start refuses an unknown version" start_refuses SIDECAR_VERSION=v0.0.1
check "start refuses engine-image with version" start_refuses SIDECAR_VERSION="$first_version" SIDECAR_ENGINE_IMAGE=e SIDECAR_SERVICE_IMAGE=s
check "start refuses engine-image without service-image" start_refuses SIDECAR_ENGINE_IMAGE=e

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
#   FAKE_EXITCODE hangs: docker inspect of an exit code sleeps 5s, then fails
#   FAKE_RM_FAIL  volume, network or both: that removal fails (exit 1)
#   FAKE_FLAKY    true: after the SIGTERM, docker inspect of the service
#                 fails its first 3 times; early: its first 3 inspects fail,
#                 before the SIGTERM
mkdir -p "$tmp/bin"
cat >"$tmp/bin/docker" <<'FAKE'
#!/usr/bin/env bash
echo "$*" >>"$FAKE_DIR/calls"
svc="$FAKE_DIR/service.running" eng="$FAKE_DIR/engine.running"
case "$1 $2" in
"inspect -f")
	f="$eng"
	[[ "$4" == *-service ]] && f="$svc"
	if [[ "$4" == *-service && ("$FAKE_FLAKY" == early || ("$FAKE_FLAKY" == true && -f "$FAKE_DIR/signaled")) ]]; then
		fails=$(($(cat "$FAKE_DIR/fails" 2>/dev/null || echo 0) + 1))
		echo "$fails" >"$FAKE_DIR/fails"
		# The sleep's output goes nowhere: a docker cut off by timeout
		# leaves no process holding the caller's pipe open.
		((fails <= 3)) && { [[ -n "${FAKE_PROBE_SLEEP:-}" && ! -f "$FAKE_DIR/signaled" ]] && sleep "$FAKE_PROBE_SLEEP" >/dev/null 2>&1 </dev/null; exit 1; }
	fi
	case "$3" in
	*ExitCode*) [[ "${FAKE_EXITCODE:-}" == hangs ]] && { sleep 5 >/dev/null 2>&1 </dev/null; exit 1; } ;;
	esac
	case "$3" in
	*Running*) [[ -f "$f" ]] && echo true || echo false ;;
	*ExitCode*) [[ -f "$f" ]] && echo 0 || echo "$FAKE_EXIT" ;;
	esac
	;;
"container inspect") exit 0 ;;
"volume rm") [[ "${FAKE_RM_FAIL:-}" == volume || "${FAKE_RM_FAIL:-}" == both ]] && exit 1; exit 0 ;;
"network rm") [[ "${FAKE_RM_FAIL:-}" == network || "${FAKE_RM_FAIL:-}" == both ]] && { echo "network has active endpoints" >&2; exit 1; }; exit 0 ;;
"kill -s")
	: >"$FAKE_DIR/signaled"
	if [[ "$FAKE_SERVICE" == by-itself ]]; then
		(sleep "${FAKE_EXIT_AFTER:-1}" && rm -f "$svc") >/dev/null 2>&1 &
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
	local code="$1" state_linger="linger_seconds=1" stop_seconds=14
	shift
	# A start from before the linger wrote no linger_seconds, and a
	# stop_seconds of the drain plus 10s.
	[[ "${1:-}" == old-state ]] && { state_linger="" stop_seconds=13; shift; }
	rm -rf "$tmp/fake" "$tmp/sc"
	local t=$SECONDS rc
	(
		export PATH="$tmp/bin:$PATH" FAKE_DIR="$tmp/fake" SIDECAR_DIR="$tmp/sc" SIDECAR_NAME=t
		export FAKE_DRAIN=after FAKE_SERVICE=after-engine FAKE_ENGINE=stops FAKE_EXIT="$code"
		(($#)) && export "$@"
		unset GITHUB_OUTPUT GITHUB_STEP_SUMMARY
		# T_SUMMARY: a step summary file; T_CREATED: what the state says the start created.
		[[ -n "${T_SUMMARY:-}" ]] && export GITHUB_STEP_SUMMARY="$T_SUMMARY"
		mkdir -p "$FAKE_DIR" "$SIDECAR_DIR/t/logs"
		: >"$FAKE_DIR/service.running"
		: >"$FAKE_DIR/engine.running"
		printf '%s\n' service=t-service engine=t-engine volume=t-engine-state network=t-net \
			"stop_seconds=$stop_seconds" $state_linger "logs=$SIDECAR_DIR/t/logs" role=writer \
			${T_CREATED:-created=network created=service created=volume created=engine} >"$SIDECAR_DIR/t/state"
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
took_hello_at_most() { [[ "$(cat "$tmp/hello.took")" -le "$1" ]]; }
took_hello_at_least() { [[ "$(cat "$tmp/hello.took")" -ge "$1" ]]; }
# in_file_order <file> <first> <second>: the first line matching first
# comes before the first matching second.
in_file_order() {
	local a b
	a="$(grep -nF -m1 -- "$2" "$1" | cut -d: -f1)"
	b="$(grep -nF -m1 -- "$3" "$1" | cut -d: -f1)"
	[[ -n "$a" && -n "$b" && "$a" -lt "$b" ]]
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
# zerobull-consulting/dagger#104: a failed removal is reported, with its
# name and kind; it doesn't change the stop's exit status.
check "a stop whose volume can't be removed still passes" stop_with 0 FAKE_RM_FAIL=volume
check "... and reports the volume by kind and name" grep -q "left behind: volume t-engine-state" "$tmp/stop.out"
check "... and the rest is still removed" called "network rm t-net"
check "a stop whose network can't be removed still passes" stop_with 0 FAKE_RM_FAIL=network
check "... and reports the network by kind and name" grep -q "left behind: network t-net" "$tmp/stop.out"
check "a clean stop reports nothing left behind" stop_with 0
check "... in its output" not grep -q "left behind" "$tmp/stop.out"
# A start that failed after the network, before the service: a failed removal
# still reaches the step summary.
rm -f "$tmp/summary"
check "a partial start whose network can't be removed still passes" stop_with 0 FAKE_RM_FAIL=network T_CREATED="created=network" T_SUMMARY="$tmp/summary"
check "... and its step summary names the network" grep -q "left behind.*network t-net" "$tmp/summary"
check "a stop that can't remove two things names both" stop_with 0 FAKE_RM_FAIL=both
check "... separated" grep -q "left behind: volume t-engine-state, network t-net;" "$tmp/stop.out"
# A docker that hangs on the exit code is cut off at the bound, not waited on.
exit_code_hang() {
	local t=$SECONDS out
	out="$(PATH="$tmp/bin:$PATH" FAKE_DIR="$tmp/fake" FAKE_EXITCODE=hangs FAKE_EXIT=0 exit_code t-service 1)"
	[[ "$out" == unknown && $((SECONDS - t)) -le 3 ]]
}
mkdir -p "$tmp/fake"
check "an exit code that docker doesn't answer in time is 'unknown', within the bound" exit_code_hang
check "a stop whose service exit code is unknown fails" not stop_with 0 FAKE_EXITCODE=hangs
check "... and says so" grep -q "exit code is unknown" "$tmp/stop.out"
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

check "a docker inspect that fails during the drain isn't taken for the service's exit" stop_with 0 FAKE_FLAKY=true
check "... the engine is still stopped only once the drain is done" called "ENGINE STOPPED AFTER THE DRAIN"
check "a docker inspect that fails before the stop: the service is still signaled, and the stop passes" stop_with 0 FAKE_FLAKY=early
check "... the engine is stopped only once the drain is done" called "ENGINE STOPPED AFTER THE DRAIN"
check "... says so" grep -q "docker didn't say whether the service is running" "$tmp/stop.out"
check "a slow probe before the SIGTERM doesn't eat the service's time: it isn't stopped by force" stop_with 0 FAKE_FLAKY=early FAKE_PROBE_SLEEP=5 FAKE_SERVICE=by-itself FAKE_EXIT_AFTER=3
check "... no second signal" not called "stop --time 5 t-service"
# zerobull-consulting/dagger#125: a daemon that hangs during the probe
# before the SIGTERM. Each of its three inspects would hang 60s; the probe is
# bounded at about 10s in all, so the whole stop, cleanup included, takes
# that plus its usual seconds, well inside its documented bound. A probe cut
# off by its bound is never taken for the service's exit: the service is
# still signaled, and the engine still waits for its drain.
check "a daemon that hangs during the probe: the stop passes (zerobull-consulting/dagger#125)" \
	stop_with 0 FAKE_FLAKY=early FAKE_PROBE_SLEEP=60
check "... its probe is bounded: the whole stop takes at most 10s plus its usual seconds" took_at_most 14
check "... the probe isn't taken for the service's exit: it's signaled, and its drain waited for" \
	in_order "kill -s TERM t-service" "logs --since" "stop --time 30 t-engine"
check "... the engine is stopped only once the drain is done" called "ENGINE STOPPED AFTER THE DRAIN"
check "... says so" grep -q "docker didn't say whether the service is running" "$tmp/stop.out"
check "... and cleans up" cleaned_up
# container_state alone, against a docker that never answers: its budget
# bounds the three tries and the pauses between them, and the answer is
# unknown, never stopped. SECONDS counts whole seconds, so allow 2s more.
mkdir -p "$tmp/probehang"
printf '#!/bin/sh\nexec sleep 60 >/dev/null 2>&1 </dev/null\n' >"$tmp/probehang/docker"
chmod +x "$tmp/probehang/docker"
probe_bounded() {
	local t=$SECONDS v
	v="$(PATH="$tmp/probehang:$PATH" container_state x-service "$1")"
	[[ "$v" == unknown && $((SECONDS - t)) -le $(($1 + 2)) ]]
}
check "the probe's bound: three hung tries take at most their budget, and say unknown" probe_bounded 6
check "... the stop's budget, 10s" probe_bounded 10
# A daemon that is slow but answers (4s) is still heard within the stop's
# budget: the first try gets most of it.
mkdir -p "$tmp/probeslow"
printf '#!/bin/sh\nsleep 4\necho false\n' >"$tmp/probeslow/docker"
chmod +x "$tmp/probeslow/docker"
check "... a daemon that answers in 4s is still heard: the service has stopped" \
	[ "$(PATH="$tmp/probeslow:$PATH" container_state x-service 10)" = stopped ]

check "a state from a start before the linger: the stop passes" stop_with 0 old-state
check "... with that start's bounds: its drain, no linger" grep -q "it drains for up to 3s, then answers the engine's last event for up to 0s; the stop waits 13s" "$tmp/stop.out"

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
		export SIDECAR_ROLE=writer SIDECAR_SERVICE_IMAGE=s
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
		export SIDECAR_ROLE=writer SIDECAR_SERVICE_IMAGE=s
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

# --- start waits for the welcome, and fails on a refusal (#146) ---
# A docker on which the start runs to the end. Once the engine has started,
# the service's log says, per FAKE_HELLO: welcome, it welcomed the engine
# and its first import sync is done; refused, it refused its version;
# silent, it connected and said no more; none, nothing. With welcome,
# FAKE_SYNC says how the first sync shows (zerobull-consulting/dagger#103):
# done (the default), at once; late, 3s after the engine started; failed,
# done with an error; partial, done with a bundle that failed to merge and
# no error; never, it isn't logged (an image from before the line). With
# FAKE_ENGINE_DIES, the engine is running at its first inspect only; with
# FAKE_INSPECT_FAILS, every inspect of it after the first fails. Every
# docker run is recorded in $FAKE_DIR/runs, and docker cp
# writes a fake CLI.
mkdir -p "$tmp/okbin"
cat >"$tmp/okbin/docker" <<'EOF2'
#!/usr/bin/env bash
[[ "$1 $2 $3" == "network inspect -f" ]] && { echo 10.9.0.1; exit 0; }
case "$1 $2" in
"container inspect" | "volume inspect" | "network inspect") exit 1 ;;
"image inspect" | "network create" | "volume create") exit 0 ;;
"pull -q") echo "$3" >>"$FAKE_DIR/pulls" ;;
"run -d")
	echo "$*" >>"$FAKE_DIR/runs"
	[[ " $* " == *" --privileged "* ]] && : >"$FAKE_DIR/engine.started"
	exit 0
	;;
"port "*) echo 10.9.0.1:30000 ;;
"inspect -f")
	if [[ -n "${FAKE_ENGINE_DIES:-}" && "$4" == *-engine && -f "$FAKE_DIR/engine.inspected" ]]; then
		echo false
	elif [[ -n "${FAKE_INSPECT_FAILS:-}" && "$4" == *-engine && -f "$FAKE_DIR/engine.inspected" ]]; then
		exit 1
	else
		[[ "$4" == *-engine && -f "$FAKE_DIR/engine.started" ]] && : >"$FAKE_DIR/engine.inspected"
		echo true
	fi
	;;
"logs --tail") echo "LOG OF $4" ;;
"logs "*)
	echo 'time=x level=INFO msg=listening addr=:8090'
	[[ -f "$FAKE_DIR/engine.started" ]] || exit 0
	case "$FAKE_HELLO" in
	welcome)
		echo 'time=x level=INFO msg="engine connected" conn=1'
		echo 'time=x level=INFO msg="engine welcomed" conn=1 engineVersion=v1.0.0-beta.25 versionAccepted=true'
		case "${FAKE_SYNC:-done}" in
		done) echo 'time=x level=INFO msg="import sync: first done" conn=1 namespace=default merged=2 failed=0' ;;
		late)
			(($(date +%s) - $(stat -c %Y "$FAKE_DIR/engine.started") >= 3)) &&
				echo 'time=x level=INFO msg="import sync: first done" conn=1 namespace=default merged=0 failed=0'
			;;
		failed) echo 'time=x level=INFO msg="import sync: first done" conn=1 namespace=default merged=0 failed=0 error=true' ;;
		partial) echo 'time=x level=INFO msg="import sync: first done" conn=1 namespace=default merged=1 failed=1' ;;
		esac
		;;
	refused)
		echo 'time=x level=INFO msg="engine connected" conn=1'
		echo 'time=x level=WARN msg="engine refused: its version is not accepted" conn=1 engineVersion=v1.0.0-beta.24 accept=[v1.0.0-beta.25] base=dev closeStatus=4002'
		;;
	silent) echo 'time=x level=INFO msg="engine connected" conn=1' ;;
	esac
	;;
"cp "*) echo "$*" >>"${FAKE_DIR:-/dev/null}/cps"; printf '#!/bin/sh\necho dagger fake\n' >"$3" ;;
*) exit 0 ;;
esac
EOF2
chmod +x "$tmp/okbin/docker"
# hello_start <FAKE_HELLO> [VAR=value...]: a start against okbin, its
# output in $tmp/hello.out, its time in $tmp/hello.took.
hello_start() {
	local hello="$1" t=$SECONDS rc
	shift
	rm -rf "$tmp/okbin.fake" "$tmp/okbin.sc"
	fake_start okbin FAKE_HELLO="$hello" "$@" >"$tmp/hello.out" 2>&1
	rc=$?
	echo $((SECONDS - t)) >"$tmp/hello.took"
	return $rc
}
runs() { cat "$tmp/okbin.fake/runs" 2>/dev/null; }
check "a welcomed engine: the start reports ready" hello_start welcome SIDECAR_SERVICE_IMAGE=
check "... after the welcome" grep -q "the service welcomed the engine ($first_version)" "$tmp/hello.out"
check "... ready" grep -q "remote-cache-sidecar: ready" "$tmp/hello.out"
check "... running the version's service image" grep -qF -- " ghcr.io/zerobull-consulting/dagger-remote-cache:$first_version -scope " <<<"$(runs)"
check "... and its engine, by digest" grep -qF -- " $(pin_of) --debug" <<<"$(runs)"
v_last="${accepted[-1]}"
hello_start welcome SIDECAR_SERVICE_IMAGE= SIDECAR_VERSION="$v_last" >/dev/null
check "version $v_last: the start runs its service image" grep -qF -- " ghcr.io/zerobull-consulting/dagger-remote-cache:$v_last -scope " <<<"$(runs)"
check "... and its engine, by digest" grep -qF -- " $(pin_of "$v_last") --debug" <<<"$(runs)"
pulls() { cat "$tmp/okbin.fake/pulls" 2>/dev/null; }
hello_start welcome SIDECAR_SERVICE_IMAGE= >/dev/null
check "the version's floating service tag is pulled even when it is here: it moves" \
	[ "$(pulls)" = "ghcr.io/zerobull-consulting/dagger-remote-cache:$first_version" ]
hello_start welcome >/dev/null
check "a service image given explicitly, and the engine by digest, aren't pulled when here" [ -z "$(pulls)" ]
check "a refused engine fails the start (zerobull-consulting/dagger#146)" not hello_start refused
check "... never ready" not grep -q "remote-cache-sidecar: ready" "$tmp/hello.out"
check "... says why, with the refusal" grep -q 'refused the engine .*msg="engine refused: its version is not accepted" conn=1 engineVersion=v1.0.0-beta.24 accept=\[v1.0.0-beta.25\] base=dev' "$tmp/hello.out"
check "a refused engine fails the start with the version's own service image too" not hello_start refused SIDECAR_SERVICE_IMAGE=
check "... at once" took_hello_at_most 3
check "... at once, not at the timeout" took_hello_at_most 3
check "... and shows both logs" grep -q "LOG OF remote-cache-engine" "$tmp/hello.out"
check "a service that never welcomes nor refuses: the start fails" not hello_start silent SIDECAR_SERVICE_IMAGE=
check "... within its timeout (5s) and a little" took_hello_at_most 9
check "... says so" grep -q "neither welcomed nor refused it within 5s" "$tmp/hello.out"
check "... never ready" not grep -q "remote-cache-sidecar: ready" "$tmp/hello.out"
check "... even with a long grace: the version's own image always logs the welcome" not hello_start silent SIDECAR_SERVICE_IMAGE= SIDECAR_WELCOME_GRACE=1
# A service image given explicitly may be from before the welcome line.
check "an explicitly given service that logs no welcome: ready after the grace" hello_start silent SIDECAR_WELCOME_GRACE=2
check "... with a warning that says why" grep -q "warning: the service (s) logged neither a welcome nor a refusal 2s after the engine connected" "$tmp/hello.out"
check "... ready" grep -q "remote-cache-sidecar: ready" "$tmp/hello.out"
check "... but not before the grace: a refusal comes first" not hello_start refused SIDECAR_WELCOME_GRACE=0
check "... and a grace longer than the start's timeout still fails at the timeout" not hello_start silent SIDECAR_WELCOME_GRACE=60

# --- start waits for the first import sync (#103) ---
check "a welcomed engine: the start waits for the first import sync" hello_start welcome
check "... and says what it merged" grep -q "the first import sync is done: it merged 2 stored bundle(s)" "$tmp/hello.out"
check "... before it is ready" in_file_order "$tmp/hello.out" "the first import sync is done" "remote-cache-sidecar: ready"
check "... without a warning" not grep -q "warning:" "$tmp/hello.out"
check "a first sync that comes late: the start waits for it" hello_start welcome FAKE_SYNC=late
check "... and is ready only after it" took_hello_at_least 3
check "... says so" grep -q "the first import sync is done: it merged 0 stored bundle(s)" "$tmp/hello.out"
check "... without a warning" not grep -q "warning:" "$tmp/hello.out"
check "a first sync never logged: the start goes on after its bound" hello_start welcome FAKE_SYNC=never SIDECAR_FIRST_SYNC_TIMEOUT=2
check "... with a warning that says why" grep -q "warning: the service (s) logged no first import sync within 2s of the welcome" "$tmp/hello.out"
check "... ready" grep -q "remote-cache-sidecar: ready" "$tmp/hello.out"
check "... after the bound, not at once" took_hello_at_least 2
check "... and not much later: a timeout, not a retry" took_hello_at_most 6
check "... telling a given image to use the version's" grep -q "never logs it: use the version input's service image" "$tmp/hello.out"
check "the version's own service image is waited for the same" hello_start welcome FAKE_SYNC=never SIDECAR_FIRST_SYNC_TIMEOUT=2 SIDECAR_SERVICE_IMAGE=
check "... with the warning" grep -q "logged no first import sync within 2s of the welcome" "$tmp/hello.out"
check "... which doesn't tell it to use the image it runs" not grep -q "use the version input's service image" "$tmp/hello.out"
check "... but says why it may be" grep -q "the version's service image is a release from before that line" "$tmp/hello.out"
check "a first sync that failed: the start goes on, with a warning" hello_start welcome FAKE_SYNC=failed
check "... that shows the line" grep -q 'warning: the service.s first import sync failed, .*msg="import sync: first done" conn=1 .*error=true' "$tmp/hello.out"
check "... ready" grep -q "remote-cache-sidecar: ready" "$tmp/hello.out"
check "a first sync with a bundle that failed to merge, and no error: a warning too" hello_start welcome FAKE_SYNC=partial
check "... that shows the line" grep -q 'warning: the service.s first import sync failed, in whole or in part, .*merged=1 failed=1' "$tmp/hello.out"
check "... not a plain success" not grep -q "the first import sync is done" "$tmp/hello.out"
check "... ready" grep -q "remote-cache-sidecar: ready" "$tmp/hello.out"
check "an engine that exits after the welcome fails the start" not hello_start welcome FAKE_SYNC=never FAKE_ENGINE_DIES=1 SIDECAR_FIRST_SYNC_TIMEOUT=30
check "... at once, not at the first sync's bound" took_hello_at_most 6
check "... says so" grep -q "the engine exited before the service's first import sync" "$tmp/hello.out"
check "... with the engine's log" grep -q "LOG OF remote-cache-engine" "$tmp/hello.out"
check "... never ready" not grep -q "remote-cache-sidecar: ready" "$tmp/hello.out"
check "a docker inspect that fails during the first-sync wait isn't an exit (zerobull-consulting/dagger#125)" hello_start welcome FAKE_INSPECT_FAILS=1
check "... the start waits for the sync" grep -q "the first import sync is done: it merged 2 stored bundle(s)" "$tmp/hello.out"
check "... ready" grep -q "remote-cache-sidecar: ready" "$tmp/hello.out"
# An image from before the welcome line predates the first sync's line.
check "an image from before the welcome line: no first-sync wait" hello_start silent SIDECAR_WELCOME_GRACE=1 SIDECAR_FIRST_SYNC_TIMEOUT=30
check "... nor its warning" not grep -q "first import sync" "$tmp/hello.out"
check "... ready well before the sync's bound" took_hello_at_most 6
check "a first-sync bound that isn't a number is refused" not hello_start welcome SIDECAR_FIRST_SYNC_TIMEOUT=30s
check "... says so" grep -q "SIDECAR_FIRST_SYNC_TIMEOUT '30s' isn't a number of seconds" "$tmp/hello.out"
check "an engine that never connects: the start fails" not hello_start none
check "... says so" grep -q "the engine didn't connect to the service within 5s" "$tmp/hello.out"

# --- SIDECAR_CLI: the job's installed CLI, which must be the engine's version ---
# A fake CLI: `version` says FAKE_CLI_VERSION; anything else prints the
# settings the wrapper left it.
mkdir -p "$tmp/clis"
fake_cli="$tmp/clis/dagger"
cat >"$fake_cli" <<'EOF2'
#!/bin/sh
case "$*" in
# As the real CLI: its version command takes no --silent.
*--silent*) echo 'Error: flag --silent is not supported by command "dagger version"' >&2; exit 1 ;;
*version*)
	[ -n "$FAKE_CLI_SILENT" ] && exit 0
	if [ -n "$FAKE_CLI_OLD" ]; then
		echo "dagger ${FAKE_CLI_VERSION} (image://registry.dagger.io/engine:x) linux/amd64"
	else
		# The CLI's own output.
		printf 'version:     %s\ncommit:      a45241b2\ndirty:       no\nplatform:    linux/amd64\nrunner-host: image://registry.dagger.io/engine:%s\n' "$FAKE_CLI_VERSION" "$FAKE_CLI_VERSION"
	fi
	;;
*) echo "TRACEPARENT=${TRACEPARENT-unset} DAGGER_ENGINE=${DAGGER_ENGINE-unset} OTEL_X=${OTEL_X-unset} RUNNER=$_EXPERIMENTAL_DAGGER_RUNNER_HOST CLOUD=$DAGGER_CLOUD_URL $DAGGER_CLOUD_TOKEN" ;;
esac
EOF2
chmod +x "$fake_cli"
cli_says() { (FAKE_CLI_VERSION="$1" cli_version "$fake_cli"); }
check "the CLI's version is read from the first line of dagger version" [ "$(cli_says v1.0.0-beta.25)" = v1.0.0-beta.25 ]
check "... a release's too" [ "$(cli_says v0.20.8)" = v0.20.8 ]
check "... the older 'dagger v1.0.0 (...)' form too" [ "$(FAKE_CLI_OLD=1 cli_says v0.20.8)" = v0.20.8 ]
check "... only on its version line, not in runner-host's tag" not bash -c "export FAKE_CLI_VERSION=devel; source '$here/sidecar.sh'; cli_version '$fake_cli'"
check "... a dev build isn't one" not cli_says devel
check "... nor is a version that is not semver" not cli_says v1.0
check "... a CLI that says nothing is refused" not bash -c "FAKE_CLI_SILENT=1 FAKE_CLI_VERSION=x; export FAKE_CLI_SILENT FAKE_CLI_VERSION; source '$here/sidecar.sh'; cli_version '$fake_cli'"
check "check_cli accepts the engine's version" bash -c "export FAKE_CLI_VERSION=$first_version; source '$here/sidecar.sh'; check_cli '$fake_cli' $first_version"
cli_check_msg() { (FAKE_CLI_VERSION="$1" check_cli "${3:-$fake_cli}" "$2") 2>&1; }
check "a CLI of another version is refused" not cli_check_msg v1.0.0-beta.24 "$first_version"
check "... saying both versions" grep -qF "is v1.0.0-beta.24 but the engine is $first_version" <<<"$(cli_check_msg v1.0.0-beta.24 "$first_version")"
check "a relative CLI path is refused" not cli_check_msg "$first_version" "$first_version" ./dagger
check "a CLI that isn't there is refused" not cli_check_msg "$first_version" "$first_version" "$tmp/clis/nope"
check "a CLI without a version to match (the deprecated engine-image) is refused" not cli_check_msg "$first_version" ""
check "start refuses a mismatched CLI before it creates anything" start_refuses SIDECAR_CLI="$fake_cli" FAKE_CLI_VERSION=v0.0.1
check "start refuses a CLI that isn't a file before it creates anything" start_refuses SIDECAR_CLI="$tmp/clis/nope"
own_wrapper_refused() {
	# The CLI given is the sidecar's own wrapper path, a script that records
	# being run: the start must refuse it without running it.
	local d="$tmp/own" out
	rm -rf "$d"
	mkdir -p "$d/remote-cache/bin"
	printf '#!/bin/sh\ntouch "%s/ran"\necho "version:     %s"\n' "$d" "$first_version" >"$d/remote-cache/bin/dagger"
	chmod +x "$d/remote-cache/bin/dagger"
	out="$(
		PATH="$tmp/nodocker:$PATH"
		export SIDECAR_ROLE=writer SIDECAR_DIR="$d" SIDECAR_CLI="$d/remote-cache/bin/dagger"
		export CACHESERVICE_S3_ENDPOINT=http://x CACHESERVICE_S3_REGION=r CACHESERVICE_S3_BUCKET=b
		export CACHESERVICE_S3_ACCESS_KEY_ID=a CACHESERVICE_S3_SECRET_ACCESS_KEY=s
		start 2>&1
	)"
	[[ "$out" == *"own wrapper"* && ! -e "$d/ran" ]]
}
check "a CLI that is the sidecar's own wrapper is refused before it is run" own_wrapper_refused
fake_start_cli() { hello_start welcome FAKE_CLI_VERSION="$first_version" SIDECAR_CLI="$fake_cli" "$@"; }
check "a CLI of the engine's version: the start is ready" fake_start_cli
check "... without copying the CLI out of the engine" [ ! -e "$tmp/okbin.fake/cps" ]
check "... and says which CLI it uses" grep -q "using the CLI $fake_cli ($first_version)" "$tmp/hello.out"
sidecar_wrapper="$tmp/okbin.sc/remote-cache/bin/dagger"
check "... the wrapper runs that CLI" grep -qF "$fake_cli" "$sidecar_wrapper"
# zerobull-consulting/dagger#209: the start runs the wrapper under start/run,
# where its timeout calls stay in the group too; the job's later steps run it
# with GNU's own. A timeout that records how it was called tells them apart.
mkdir -p "$tmp/rectimeout"
cat >"$tmp/rectimeout/timeout" <<'FAKE'
#!/bin/sh
echo "$*" >>"$TIMEOUT_LOG"
while [ "${1#-}" != "$1" ]; do shift; done
shift
exec "$@"
FAKE
chmod +x "$tmp/rectimeout/timeout"
wrapper_timeout() {
	rm -f "$tmp/timeout.log"
	TIMEOUT_LOG="$tmp/timeout.log" PATH="$tmp/rectimeout:$tmp/okbin:$PATH" "$@" "$sidecar_wrapper" call >/dev/null 2>&1
	if grep -q -- '^--foreground 30 docker inspect' "$tmp/timeout.log"; then
		echo wrapped
	elif grep -q -- '^30 docker inspect' "$tmp/timeout.log"; then
		echo plain
	fi
}
check "... and under start/run, its timeout calls keep to the caller's process group" \
	[ "$(wrapper_timeout env SIDECAR_GROUP_KILLED=1)" = wrapped ]
check "... but in the job's later steps, its timeout is GNU's own" \
	[ "$(wrapper_timeout env SIDECAR_GROUP_KILLED=)" = plain ]
# The guards survive: a job with a trace and settings of its own.
wrapped() { TRACEPARENT=00-aa-bb-00 DAGGER_ENGINE=other OTEL_X=1 PATH="$tmp/okbin:$PATH" "$sidecar_wrapper" call; }
check "... which drops an inherited TRACEPARENT, DAGGER_* and OTEL_*" grep -q "^TRACEPARENT=unset DAGGER_ENGINE=unset OTEL_X=unset" <<<"$(wrapped)"
check "... and sets this engine and this service" grep -q "RUNNER=docker-container://remote-cache-engine CLOUD=http://10.9.0.1:30000 sidecar-cli" <<<"$(wrapped)"
# zerobull-consulting/dagger#174: the wrapper and cli_run had a list each.
# Now one function is both, and the wrapper carries a copy of it.
scrubbed_names="TRACEPARENT TRACESTATE OTEL_X DAGGER_ENGINE _EXPERIMENTAL_DAGGER_RUNNER_HOST"
unscrubbed_by_wrapper() {
	local v n=0 out
	local -a assign=()
	for v in $scrubbed_names; do assign+=("$v=1"); done
	out="$(env "${assign[@]}" PATH="$tmp/okbin:$PATH" bash -c 'source "$1"; env' _ <(sed -n '/^scrub_env ()/,/^}/p' "$sidecar_wrapper"; echo 'scrub_env') 2>&1)"
	for v in $scrubbed_names; do grep -q "^$v=" <<<"$out" && n=$((n + 1)); done
	((n == 0))
}
check "... the wrapper carries the scrub_env function of sidecar.sh" [ "$(declare -f scrub_env)" = "$(sed -n '/^scrub_env ()/,/^}/p' "$sidecar_wrapper")" ]
check "... which scrubs every name of the list" unscrubbed_by_wrapper
check "... and cli_run scrubs the same names" bash -c 'source "$1"; set +e; export TRACEPARENT=1 TRACESTATE=1 OTEL_X=1 DAGGER_ENGINE=1 _EXPERIMENTAL_DAGGER_RUNNER_HOST=1; cli_run env | grep -qE "^(TRACEPARENT|TRACESTATE|OTEL_X|DAGGER_ENGINE|_EXPERIMENTAL_DAGGER_RUNNER_HOST)="; [[ $? -ne 0 ]]' _ "$here/sidecar.sh"
mismatch_start() {
	hello_start welcome FAKE_CLI_VERSION=v1.0.0-beta.1 SIDECAR_CLI="$fake_cli" SIDECAR_SERVICE_IMAGE=
	local rc=$?
	[[ $rc -ne 0 ]] && [[ ! -e "$tmp/okbin.fake/pulls" && ! -e "$tmp/okbin.fake/runs" ]] && grep -q "is v1.0.0-beta.1 but the engine is" "$tmp/hello.out"
}
check "... a mismatch pulls nothing and runs nothing" mismatch_start
hello_start welcome >/dev/null
check "... docker cp copies the CLI out of the engine" grep -q "^cp remote-cache-engine:/usr/local/bin/dagger " "$tmp/okbin.fake/cps"

# --- the pulls run at the same time, and each failure fails the start ---
# Both images are missing. Each pull records that it started, then waits (at
# most 5s) for the other to have started: sequential pulls never see it.
mkdir -p "$tmp/pullbin"
cat >"$tmp/pullbin/docker" <<'EOF2'
#!/usr/bin/env bash
[[ "$1 $2 $3" == "network inspect -f" ]] && { echo 10.9.0.1; exit 0; }
case "$1 $2" in
"container inspect" | "volume inspect" | "network inspect" | "image inspect") exit 1 ;;
"pull -q")
	img="${3//[^A-Za-z0-9]/_}"
	: >"$FAKE_DIR/started.$img"
	for _ in $(seq 50); do
		[[ "$(ls "$FAKE_DIR" | grep -c '^started\.')" -ge 2 ]] && { echo "$3" >>"$FAKE_DIR/overlapped"; break; }
		sleep 0.1
	done
	[[ -n "$FAKE_PULL_FAIL" && "$3" == *"$FAKE_PULL_FAIL"* ]] && exit 1
	exit 0
	;;
*) exit 1 ;;
esac
EOF2
chmod +x "$tmp/pullbin/docker"
pull_start() { rm -rf "$tmp/pullbin.fake" "$tmp/pullbin.sc"; fake_start pullbin SIDECAR_START_TIMEOUT=2 "$@" >"$tmp/pull.out" 2>&1; }
pull_start SIDECAR_SERVICE_IMAGE=svc:1
check "the start's pulls overlap: each saw the other begin" [ "$(wc -l <"$tmp/pullbin.fake/overlapped")" = 2 ]
pull_start FAKE_PULL_FAIL=svc SIDECAR_SERVICE_IMAGE=svc:1
check "a failed service pull fails the start" grep -q "pulling svc:1 failed or took over 15m" "$tmp/pull.out"
check "... naming only that image" not grep -q "pulling registry.dagger.io[^ ]* failed" "$tmp/pull.out"
check "... before it creates a network" not grep -q "starting the .* service" "$tmp/pull.out"
pull_start FAKE_PULL_FAIL=registry.dagger.io SIDECAR_SERVICE_IMAGE=svc:1
check "a failed engine pull fails the start" grep -q "pulling registry.dagger.io/engine:.* failed or took over 15m" "$tmp/pull.out"
check "... naming only that image" not grep -q "pulling svc:1 failed" "$tmp/pull.out"
pull_start FAKE_PULL_FAIL=: SIDECAR_SERVICE_IMAGE=svc:1
check "both failing: both are named" bash -c "grep -q 'pulling svc:1 failed' '$tmp/pull.out' && grep -q 'pulling registry.dagger.io/engine' '$tmp/pull.out'"

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
# The args files are there (a flag that is absent proves nothing when no
# service was started): each has the flags every start passes.
check "writer: the service was started, with its -scope" grep -qxF -- "-scope" <<<"$dbg_args"
check "writer: the service gets -gc-mode=batch" grep -qxF -- "-gc-mode=batch" <<<"$dbg_args"
check "writer: and no -gc-interval" not grep -q -- "-gc-interval" <<<"$dbg_args"
# Its own bin directory (a copy of the same fake docker), so it leaves the
# debug-port start's state alone.
mkdir -p "$tmp/robin"
cp "$tmp/dbgbin/docker" "$tmp/robin/docker"
fake_start robin SIDECAR_ROLE=read-only >/dev/null
ro_args="$(cat "$tmp/robin.fake/service.args" 2>/dev/null)"
check "read-only: the service was started, with its -scope" grep -qxF -- "-scope" <<<"$ro_args"
check "read-only: the service gets -export=false" grep -qxF -- "-export=false" <<<"$ro_args"
check "read-only: and neither -gc-mode nor -gc-interval" not grep -qE -- "^-gc-(mode|interval)" <<<"$ro_args"
check "start passes the linger the stop's bounds assume" grep -qx -- "-drain-linger" <<<"$dbg_args"
check "... 15s" bash -c 'grep -A1 -x -- "-drain-linger" <<<"$1" | tail -1 | grep -qx 15s' _ "$dbg_args"
check "... and records it for the stop" grep -qx linger_seconds=15 "$tmp/dbgbin.sc/remote-cache/state"
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

# zerobull-consulting/dagger#174: a second start with a name in use fails
# before it writes or creates anything, so the first sidecar's state is as it
# was. The second start's post step then runs the stop for that name, which
# is the first sidecar's: both stops come at the job's end, after every step
# that uses the first, so the first is stopped when it would have been.
second_start_refused() {
	hello_start welcome >/dev/null || return 1
	local state="$tmp/okbin.sc/remote-cache/state" before after calls_before out
	before="$(cat "$state")" calls_before="$(cat "$tmp/okbin.fake/calls")"
	out="$(fake_start okbin SIDECAR_SERVICE_IMAGE= 2>&1)" && return 1
	after="$(cat "$state")"
	grep -q "already set up" <<<"$out" && [ "$before" = "$after" ] && [ "$calls_before" = "$(cat "$tmp/okbin.fake/calls")" ]
}
check "a second start with a name in use is refused, and touches nothing of the first" second_start_refused

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
