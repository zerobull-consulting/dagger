#!/usr/bin/env bash
# Runs the demo (demo.sh) through a running sidecar and checks what it did,
# from the cache service's log: the cache spans it received, its merges and
# its exports. CI's sidecar job and run.sh use it. Needs the service's -v
# (the action's verbose: true), which logs every cache span.
#
#   check.sh writer    <service container> <dir>
#   check.sh stored    <service log file>  <dir>
#   check.sh read-only <service container> <dir> <the writer's dir>
#
# writer: the cacheable step executes and the cache-volume step is warm (the
# engine has the cache volume the cacheable step filled). It saves the
# cacheable step's stamp and the engine's cache ID in <dir>.
#
# stored: after the writer's stop, its log shows exports stored, and a
# drain that completed.
#
# read-only: on a fresh engine with a read-only service, the service merged
# the writer's entries; the cacheable step is a hit, with 0 executed and the
# writer's stamp; the cache-volume step reruns, cold; and nothing is
# exported.
#
# Hits are counted from cache spans (Container.withExec, outcome hit or
# executed), as cmd/cacheservice/e2e does, never from wall time. Every wait
# is bounded (CHECK_TIMEOUT, default 120s).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
timeout_s="${CHECK_TIMEOUT:-120}"
failures=0

ok() { echo "ok    $*"; }
fail() {
	echo "FAIL  $*"
	[[ -n "${GITHUB_ACTIONS:-}" ]] && echo "::error::remote cache demo: $*"
	failures=$((failures + 1))
}
finish() {
	if ((failures > 0)); then
		echo "$failures check(s) failed" >&2
		exit 1
	fi
	echo "all checks passed"
}

# The service's log, whole, then from line $2 on (1-based). Read whole
# before grep: grep -q stopping early would fail the pipeline (pipefail).
# Each read is bounded, so CHECK_TIMEOUT bounds the waits.
service_log() { timeout 30 docker logs "$1" 2>&1 || true; }
log_from() { service_log "$1" | tail -n +"$2"; }
log_lines() { service_log "$1" | wc -l; }

# withExec cache spans in a piece of the log: "<hit> <executed> <cache IDs>".
exec_spans() {
	awk '
		/msg="cache span"/ && / name=Container\.withExec / {
			if ($0 ~ / outcome=hit /) hit++
			if ($0 ~ / outcome=executed /) executed++
			if (match($0, / cache=[^ \/]+/)) ids[substr($0, RSTART + 7, RLENGTH - 7)] = 1
		}
		END {
			n = ""
			for (id in ids) n = n (n == "" ? "" : ",") id
			printf "%d %d %s\n", hit, executed, (n == "" ? "-" : n)
		}'
}

# Runs one demo step, and waits until the service has its withExec spans
# (at least $3 with an outcome). Sets out, hit, executed and caches.
step() {
	local service="$1" what="$2" want="$3"
	local mark=$(($(log_lines "$service") + 1))
	out="$(DEMO_LOG_DIR="$dir" "$here/demo.sh" "$what")" || { fail "the $what step failed: see $dir/$what.log"; out=""; }
	echo "$what: $out"
	local deadline=$((SECONDS + timeout_s)) spans
	while :; do
		spans="$(log_from "$service" "$mark" | exec_spans)"
		read -r hit executed caches <<<"$spans"
		((hit + executed >= want)) && break
		if ((SECONDS >= deadline)); then
			fail "the $what step's withExec cache spans didn't reach the service within ${timeout_s}s ($hit hit, $executed executed): does the CLI inherit an unsampled TRACEPARENT, or lack DAGGER_CLOUD_URL and DAGGER_CLOUD_TOKEN?"
			break
		fi
		sleep 1
	done
	# Late spans of the same session, if any.
	sleep 2
	read -r hit executed caches <<<"$(log_from "$service" "$mark" | exec_spans)"
	echo "$what: Container.withExec spans: $hit hit, $executed executed (engine cache $caches)"
}

writer() {
	local service="$1"
	step "$service" build 1
	local stamp="$out"
	[[ "$stamp" =~ ^build\ [0-9a-f-]{36}\ [0-9a-f]{16}$ ]] && ok "the cacheable step printed a stamp ($stamp)" ||
		fail "the cacheable step's output isn't a stamp: '$stamp'"
	((executed >= 1)) && ok "the cacheable step executed on the writer ($executed executed)" ||
		fail "the cacheable step didn't execute on the writer's engine ($hit hit, $executed executed): is it fresh?"
	echo "$stamp" >"$dir/stamp"
	echo "$caches" >"$dir/writer-cache"

	step "$service" app 2
	[[ "$out" == *"cache volume warm"* ]] && ok "the cache-volume step is warm on the engine that filled the volume" ||
		fail "the cache-volume step isn't warm on the writer's engine: '$out'"
	((executed >= 1)) && ok "the cache-volume step executed ($executed executed)" ||
		fail "the cache-volume step didn't execute on the writer ($hit hit, $executed executed)"
	finish
}

stored() {
	local log="$1"
	[[ -f "$log" ]] || { fail "no service log at $log"; finish; }
	local exports roots
	# No export line is a failure to report, not a grep to exit on.
	exports="$(grep -c 'msg="export stored"' "$log" || true)"
	roots="$({ grep 'msg="export stored"' "$log" || true; } | sed -nE 's/.* roots=([0-9]+).*/\1/p' | awk '{n += $1} END {print n + 0}')"
	((exports >= 1 && roots >= 1)) && ok "the writer stored $exports export(s), $roots root(s)" ||
		fail "the writer stored no export ($exports exports, $roots roots)"
	grep -q 'msg="drain: done" outcome=complete' "$log" && ok "the writer's drain completed" ||
		fail "the writer's log has no completed drain (was the stop skipped, or cut off?)"
	if grep -q 'msg="drain: left unexported"' "$log"; then
		fail "the writer's drain left roots unexported: $(grep 'msg="drain: left unexported"' "$log" | head -n 3 || true)"
	fi
	finish
}

read_only() {
	local service="$1"
	local wdir="$2"
	[[ -f "$wdir/stamp" && -f "$wdir/writer-cache" ]] || { fail "no stamp from the writer phase in $wdir"; finish; }
	local stamp writer_cache
	stamp="$(cat "$wdir/stamp")"
	writer_cache="$(cat "$wdir/writer-cache")"

	grep -q 'msg=mode export=off import=on gc=off readOnly=true' <<<"$(service_log "$service")" &&
		ok "the service is read-only (-export=false)" || fail "the service isn't read-only: no 'readOnly=true' mode line"
	# The service merges what is stored as the engine connects; wait for it,
	# so the demo doesn't race it.
	local deadline=$((SECONDS + timeout_s)) merged=0
	while :; do
		merged="$(service_log "$service" | sed -nE 's/.*msg="import sync" .* merged=([0-9]+).*/\1/p' | awk '{n += $1} END {print n + 0}')"
		((merged >= 1)) && break
		if ((SECONDS >= deadline)); then
			break
		fi
		sleep 1
	done
	((merged >= 1)) && ok "the service merged $merged stored bundle(s) into the engine" ||
		fail "the service merged nothing into the engine within ${timeout_s}s: did the writer store an export, under the same prefix and namespace?"

	step "$service" build 1
	[[ "$caches" != - && ",$caches," != *",$writer_cache,"* ]] && ok "the engine is fresh: cache $caches, the writer's was $writer_cache" ||
		fail "the engine isn't fresh: it has the writer's cache ($caches), so its hits aren't the remote cache's"
	((executed == 0 && hit >= 1)) && ok "the cacheable step is a hit: $hit hit, 0 executed" ||
		fail "the cacheable step isn't a hit: $hit hit, $executed executed"
	[[ "$out" == "$stamp" ]] && ok "the cacheable step returned the writer's stamp" ||
		fail "the cacheable step's stamp isn't the writer's: '$out', the writer's '$stamp'"

	step "$service" app 2
	[[ "$out" == *"cache volume cold"* ]] && ok "the cache-volume step reran its work, cold: cache volumes stay local" ||
		fail "the cache-volume step isn't cold on a fresh engine: '$out'"
	((executed >= 1)) && ok "the cache-volume step executed ($executed executed)" ||
		fail "the cache-volume step didn't execute ($hit hit, $executed executed): was it a cache hit?"

	if grep -q 'msg="export stored"' <<<"$(service_log "$service")"; then
		fail "the read-only service exported something"
	else
		ok "the read-only service exported nothing"
	fi
	finish
}

case "${1:-}" in
writer | stored)
	[[ $# -eq 3 ]] || { sed -n '7,9p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
	dir="$3"
	mkdir -p "$dir"
	if [[ "$1" == writer ]]; then writer "$2"; else stored "$2"; fi
	;;
read-only)
	[[ $# -eq 4 ]] || { sed -n '7,9p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
	dir="$3"
	mkdir -p "$dir"
	read_only "$2" "$4"
	;;
*)
	sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
	;;
esac
