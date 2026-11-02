#!/usr/bin/env bash
# End-to-end run of the remote cache with real engines (issue #29). Run it
# through `dev.sh e2e`, which sets the MinIO settings. See README.md in this
# directory and cmd/cacheservice/README.md, "Real-engine run".
#
# It starts from nothing each time: MinIO's data is deleted, and engines A and
# B get new, empty cache volumes. Then:
#
#   1. builds dagger-engine, dagger-init-session and the dagger CLI from this
#      repo, and an engine image that copies them over a release image;
#   2. starts MinIO, published on the Docker bridge gateway so the engine
#      containers reach it at the address the service signs URLs for;
#   3. starts cacheservice on the host, on the bridge gateway only (it takes
#      any token), with the S3 settings from `dev.sh env`;
#   4. starts engines A and B, each pointed at the service;
#   5. runs the pipeline on A, waits until the index in MinIO holds its
#      entries and the service has merged them into B, then runs the same
#      pipeline on B;
#   6. checks that B's result is A's (the exec didn't run again on B), that B
#      read the layers from MinIO, that the index holds A's entries plus only
#      what B built itself, each once (#35), and prints both runs'
#      wall-clock times.
#
# Then the scenarios in E2E_SCENARIOS, in this order (all by default):
#
#   reexport    (#30) A's stored entries exported again upload nothing, keep
#               their numbers and write no bundle; a second pipeline over
#               alpine uploads only its own layers.
#   renew       (#31) a second service, a replica on the same scope with
#               presigned GETs that last E2E_GET_EXPIRY seconds, merges A's
#               entries into new engines C and D. Once the GETs expired, C
#               reads the layers through renew; then exec2's layer is
#               deleted from MinIO, and D's read degrades to a cache miss.
#   prune       (#32) C prunes its cache; the replica, polling every
#               E2E_IMPORT_POLL seconds, doesn't merge the entries into it
#               again until C restarts. Starts the replica and C if renew
#               didn't.
#   superseded  (#32, #27) a Dang module function with a cache TTL of
#               E2E_TTL seconds runs on A, expires, and runs again, which
#               replaces its value; the service stores both. A new engine F
#               then imports, and reads the value: which record it got is
#               reported.
#
# Each prints its checks as it ends; the run fails if any check failed.
# Everything is left running afterwards so it can be inspected: `dev.sh
# e2e-down` stops it. Logs and results go to E2E_DIR.
#
# Settings: E2E_DIR (default: $TMPDIR/dagger-remote-cache-e2e), E2E_BASE_IMAGE
# (the release image the binaries are copied over), E2E_SKIP_BUILD=1 (reuse
# the binaries in E2E_DIR/bin), CACHESERVICE_PORT (8090),
# CACHESERVICE_DEBUG_PORT (8091), REPLICA_PORT (8092), REPLICA_DEBUG_PORT
# (8093), E2E_TIMEOUT (seconds to wait for the export and the merge, 180),
# E2E_CA_FILE (a CA bundle the engines should trust, for hosts behind a
# TLS-intercepting proxy), E2E_SCENARIOS ("reexport renew prune
# superseded"), E2E_GET_EXPIRY (30), E2E_IMPORT_POLL (2), E2E_TTL (30).
set -eEuo pipefail
# A command that fails where nothing expected it to says where, rather than
# ending the run silently.
trap 'echo "e2e: line ${LINENO}: \"${BASH_COMMAND}\" failed (exit $?)" >&2' ERR

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"

: "${E2E_DIR:=${TMPDIR:-/tmp}/dagger-remote-cache-e2e}"
: "${E2E_BASE_IMAGE:=registry.dagger.io/engine:v1.0.0-beta.22}"
: "${E2E_IMAGE:=dagger-remote-cache-e2e-engine}"
: "${CACHESERVICE_PORT:=8090}"
: "${CACHESERVICE_DEBUG_PORT:=8091}"
: "${E2E_TIMEOUT:=180}"
: "${E2E_SCOPE:=default}"
: "${REPLICA_PORT:=8092}"
: "${REPLICA_DEBUG_PORT:=8093}"
: "${E2E_SCENARIOS:=reexport renew prune superseded}"
: "${E2E_GET_EXPIRY:=30}"
: "${E2E_IMPORT_POLL:=2}"
: "${E2E_TTL:=30}"

all_scenarios=(reexport renew prune superseded)
for sc in $E2E_SCENARIOS; do
	[[ " ${all_scenarios[*]} " == *" $sc "* ]] || {
		echo "e2e: unknown scenario '$sc' in E2E_SCENARIOS (known: ${all_scenarios[*]})" >&2
		exit 2
	}
done
wanted() { [[ " $E2E_SCENARIOS " == *" $1 "* ]]; }

engines=(a b c d f)
container() { echo "dagger-remote-cache-e2e-$1"; }
volume() { echo "dagger-remote-cache-e2e-$1"; }

# The engines reach the service and MinIO on the Docker bridge gateway. So
# does the CLI: the engine sends session spans to the CLI's DAGGER_CLOUD_URL.
gateway="$(docker network inspect bridge -f '{{(index .IPAM.Config 0).Gateway}}')"
service_url="http://${gateway}:${CACHESERVICE_PORT}"
debug_url="http://127.0.0.1:${CACHESERVICE_DEBUG_PORT}"
replica_url="http://${gateway}:${REPLICA_PORT}"
replica_debug_url="http://127.0.0.1:${REPLICA_DEBUG_PORT}"

# svc_url and svc_debug SERVICE: the addresses of the main service or the
# replica.
svc_url() { if [[ "$1" == replica ]]; then echo "$replica_url"; else echo "$service_url"; fi; }
svc_debug() { if [[ "$1" == replica ]]; then echo "$replica_debug_url"; else echo "$debug_url"; fi; }
svc_log() { if [[ "$1" == replica ]]; then echo "$E2E_DIR/replica.log"; else echo "$E2E_DIR/cacheservice.log"; fi; }

dev() { MINIO_BIND_ADDR="$gateway" "$here/dev.sh" "$@"; }

log() { printf '\n==> %s\n' "$*" >&2; }
die() {
	printf 'e2e: %s\n' "$*" >&2
	exit 1
}

now() { date +%s.%N; }
elapsed() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%.2f", b - a }'; }

# wait_for SECONDS DESCRIPTION COMMAND...: runs COMMAND every second until it
# succeeds.
wait_for() {
	local timeout="$1" what="$2" deadline
	shift 2
	deadline=$((SECONDS + timeout))
	until "$@" >/dev/null 2>&1; do
		if ((SECONDS >= deadline)); then
			die "timed out after ${timeout}s waiting for ${what}"
		fi
		sleep 1
	done
}

# stop_service PID: stops a cacheservice and waits until it has exited. On
# SIGTERM it drains, finishing exports and imports in flight (up to 5s), and
# may still write to MinIO meanwhile: a reset before it exits would not be
# from empty.
stop_service() {
	local pid="$1" i
	kill "$pid" 2>/dev/null || return 0
	for i in $(seq 30); do
		kill -0 "$pid" 2>/dev/null || return 0
		sleep 0.5
	done
	kill -9 "$pid" 2>/dev/null || true
	for i in $(seq 10); do
		kill -0 "$pid" 2>/dev/null || return 0
		sleep 0.5
	done
	die "cacheservice (pid $pid) did not exit"
}

stop_all() {
	local e
	for e in "${engines[@]}"; do
		docker rm -f "$(container "$e")" >/dev/null 2>&1 || true
		docker volume rm -f "$(volume "$e")" >/dev/null 2>&1 || true
	done
	for pidfile in cacheservice.pid replica.pid; do
		if [[ -f "$E2E_DIR/$pidfile" ]]; then
			stop_service "$(cat "$E2E_DIR/$pidfile")"
			rm -f "$E2E_DIR/$pidfile"
		fi
	done
	stop_trace
}

build() {
	mkdir -p "$E2E_DIR/bin"
	if [[ "${E2E_SKIP_BUILD:-}" != 1 ]]; then
		log "Building the engine, init-session, the CLI and cacheservice"
		(
			cd "$repo"
			export CGO_ENABLED=0
			go build -tags dfexcludepatterns,dfparents -o "$E2E_DIR/bin/dagger-engine" ./cmd/engine
			go build -o "$E2E_DIR/bin/dagger-init-session" ./cmd/init-session
			go build -o "$E2E_DIR/bin/dagger" ./cmd/dagger
			go build -o "$E2E_DIR/bin/cacheservice" ./cmd/cacheservice
		)
	fi
	log "Building ${E2E_IMAGE} from ${E2E_BASE_IMAGE}"
	# GC is off: on small disks the engine prunes every entry right after the
	# session. docker.io pulls go through mirror.gcr.io, as in the release
	# image's own engine.json.
	cat >"$E2E_DIR/bin/engine.json" <<-'EOF'
		{
		  "gc": {"enabled": false},
		  "registries": {"docker.io": {"mirrors": ["mirror.gcr.io"]}}
		}
	EOF
	cat >"$E2E_DIR/bin/Dockerfile" <<-EOF
		FROM ${E2E_BASE_IMAGE}
		COPY dagger-engine dagger-init-session dagger /usr/local/bin/
		COPY engine.json /etc/dagger/engine.json
	EOF
	# Behind a TLS-intercepting proxy, the engine has to trust its CA to pull
	# images.
	if [[ -n "${E2E_CA_FILE:-}" ]]; then
		cp "$E2E_CA_FILE" "$E2E_DIR/bin/extra-ca.crt"
		cat >>"$E2E_DIR/bin/Dockerfile" <<-'EOF'
			COPY extra-ca.crt /usr/local/share/ca-certificates/extra-ca.crt
			RUN cat /usr/local/share/ca-certificates/extra-ca.crt >>/etc/ssl/certs/ca-certificates.crt
		EOF
	fi
	docker build -q -t "$E2E_IMAGE" "$E2E_DIR/bin" >/dev/null
}

start_minio() {
	log "Starting MinIO on ${gateway}, from empty"
	# The run is only from empty if this deletes the old data.
	dev reset >/dev/null || die "could not delete MinIO's data, so the run would not start from empty"
	dev up >/dev/null 2>&1 || dev up
	eval "$(dev env)"
}

start_service() {
	log "Starting cacheservice on ${gateway}:${CACHESERVICE_PORT}"
	"$E2E_DIR/bin/cacheservice" -v \
		-listen "${gateway}:${CACHESERVICE_PORT}" \
		-debug-listen "127.0.0.1:${CACHESERVICE_DEBUG_PORT}" \
		-scope "$E2E_SCOPE" \
		>"$E2E_DIR/cacheservice.log" 2>&1 &
	echo $! >"$E2E_DIR/cacheservice.pid"
	wait_for 30 "cacheservice" curl -fsS "$debug_url/debug/engines"
	kill -0 "$(cat "$E2E_DIR/cacheservice.pid")" 2>/dev/null ||
		die "cacheservice exited: $E2E_DIR/cacheservice.log"
}

# start_replica: a second cacheservice on the same bucket and scope, as
# another replica of the service would be, with presigned GETs that last
# E2E_GET_EXPIRY seconds and an import poll of E2E_IMPORT_POLL seconds. It
# learns of the main service's exports by polling the index.
start_replica() {
	if [[ -f "$E2E_DIR/replica.pid" ]] && kill -0 "$(cat "$E2E_DIR/replica.pid")" 2>/dev/null; then
		return
	fi
	log "Starting a replica of cacheservice on ${gateway}:${REPLICA_PORT}: GETs last ${E2E_GET_EXPIRY}s, import poll ${E2E_IMPORT_POLL}s"
	CACHESERVICE_S3_GET_EXPIRY="${E2E_GET_EXPIRY}s" "$E2E_DIR/bin/cacheservice" -v \
		-listen "${gateway}:${REPLICA_PORT}" \
		-debug-listen "127.0.0.1:${REPLICA_DEBUG_PORT}" \
		-scope "$E2E_SCOPE" \
		-import-poll "${E2E_IMPORT_POLL}s" \
		>"$E2E_DIR/replica.log" 2>&1 &
	echo $! >"$E2E_DIR/replica.pid"
	wait_for 30 "the replica" curl -fsS "$replica_debug_url/debug/engines"
	kill -0 "$(cat "$E2E_DIR/replica.pid")" 2>/dev/null ||
		die "the replica exited: $E2E_DIR/replica.log"
}

# start_engine NAME [SERVICE]: starts an engine with a new, empty cache
# volume, pointed at the main service or the replica, and prints its
# connection number there.
start_engine() {
	local name="$1" svc="${2:-main}" url dbg before
	url="$(svc_url "$svc")"
	dbg="$(svc_debug "$svc")"
	before="$(curl -fsS "$dbg/debug/engines" | jq -r '[.[].conn] | max // 0')"
	docker rm -f "$(container "$name")" >/dev/null 2>&1 || true
	docker volume rm -f "$(volume "$name")" >/dev/null 2>&1 || true
	docker run -d --privileged \
		--name "$(container "$name")" \
		--hostname "e2e-engine-${name}" \
		-v "$(volume "$name"):/var/lib/dagger" \
		-e _EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL="$url" \
		-e DAGGER_CLOUD_TOKEN="e2e-engine-${name}" \
		-e DAGGER_CLOUD_URL="$url" \
		"$E2E_IMAGE" --debug >/dev/null
	wait_conn "$name" "$svc" "$before"
}

# wait_conn NAME SERVICE AFTER: waits for engine NAME to connect to SERVICE
# with a connection number above AFTER, and prints it.
wait_conn() {
	local name="$1" dbg sel
	dbg="$(svc_debug "$2")"
	sel="[.[] | select(.conn > $3 and .engineName == \"e2e-engine-${name}\")]"
	wait_for 120 "engine ${name} to connect to the ${2} service" \
		sh -c "curl -fsS '$dbg/debug/engines' | jq -e '$sel | length > 0'"
	curl -fsS "$dbg/debug/engines" | jq -r "${sel}[-1].conn"
}

# engine_field SERVICE CONN FIELD: a field of a connection, as the service
# lists it.
engine_field() {
	curl -fsS "$(svc_debug "$1")/debug/engines" | jq -r ".[] | select(.conn == $2) | .$3"
}

# The pipeline: alpine, then two execs that each write 4MB of random data and
# a line with a random nonce to /stamp. Random output makes a re-run visible:
# an exec that ran again would write other stamps.
execs='withExec(args: ["sh", "-c", "head -c 4194304 /dev/urandom > /layer1.bin && echo \"exec1 $(cat /proc/sys/kernel/random/uuid) $(sha256sum /layer1.bin | cut -c1-16)\" > /stamp"]) {
        withExec(args: ["sh", "-c", "head -c 4194304 /dev/urandom > /layer2.bin && echo \"exec2 $(cat /proc/sys/kernel/random/uuid) $(sha256sum /layer2.bin | cut -c1-16)\" >> /stamp"]) {'
pipeline="{ container { from(address: \"alpine:3.22\") {
      ${execs}
          file(path: \"/stamp\") { contents }
} } } } }"

# The verify query, run on B after the pipeline: one more exec, with a nonce
# so it is never cached, over the pipeline's container. It has to read
# both layer files, so B needs every layer of the chain on disk, while the
# pipeline itself, cached down to its result string, needs none.
verify() {
	cat <<-EOF
		{ container { from(address: "alpine:3.22") {
		      ${execs}
		          withExec(args: ["sh", "-c", "echo verify-$1 >/dev/null; cat /stamp; sha256sum /layer1.bin /layer2.bin | cut -c1-16"]) { stdout }
		} } } } }
	EOF
}

# run_cli NAME OUT SERVICE ARGS...: runs the dagger CLI with ARGS, and stdin,
# on an engine, with the CLI's telemetry pointed at SERVICE (main or
# replica), and writes its stdout to OUT.json and its stderr to OUT.log.
# Prints the elapsed seconds.
run_cli() {
	local name="$1" out="$2" svc="$3" start end
	shift 3
	start="$(now)"
	# An inherited TRACEPARENT (some CI and sandboxes set one, unsampled)
	# makes the CLI's trace unsampled, and then the engine records no session
	# spans: the service never hears of the entries.
	env -u TRACEPARENT -u TRACESTATE -u OTEL_EXPORTER_OTLP_ENDPOINT \
		-u OTEL_EXPORTER_OTLP_TRACES_ENDPOINT -u OTEL_EXPORTER_OTLP_LOGS_ENDPOINT \
		_EXPERIMENTAL_DAGGER_RUNNER_HOST="docker-container://$(container "$name")" \
		DAGGER_CLOUD_TOKEN="e2e-cli" \
		DAGGER_CLOUD_URL="$(svc_url "$svc")" \
		DAGGER_NO_NAG=1 \
		"$E2E_DIR/bin/dagger" --progress=plain "$@" \
		>"$E2E_DIR/${out}.json" 2>"$E2E_DIR/${out}.log" ||
		die "the CLI on ${name} failed: $E2E_DIR/${out}.log"
	end="$(now)"
	elapsed "$start" "$end"
}

# run_query NAME OUT [SERVICE]: runs the query on stdin on an engine.
run_query() { run_cli "$1" "$2" "${3:-main}" query; }

stamp() { jq -r '.container.from.withExec.withExec.file.contents' "$E2E_DIR/$1.json"; }

mc_minio() {
	docker compose -f "$here/compose.yaml" exec -T minio sh -c \
		"mc alias set local http://localhost:9000 \"\$MINIO_ROOT_USER\" \"\$MINIO_ROOT_PASSWORD\" >/dev/null && $*"
}

index_json() { mc_minio "mc cat local/${CACHESERVICE_S3_BUCKET}/${CACHESERVICE_S3_PREFIX:+${CACHESERVICE_S3_PREFIX}/}${E2E_SCOPE}/index.json"; }

# MinIO's request trace, from start_trace until stop_trace, in the MinIO
# container's /tmp/e2e-trace.log. stop_trace OUT also copies it to
# E2E_DIR/OUT.
start_trace() {
	docker compose -f "$here/compose.yaml" exec -d minio sh -c \
		'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && echo $$ >/tmp/e2e-trace.pid && exec mc admin trace local >/tmp/e2e-trace.log 2>&1'
	sleep 2
}
stop_trace() {
	docker compose -f "$here/compose.yaml" exec -T minio sh -c \
		'[ -f /tmp/e2e-trace.pid ] && kill $(cat /tmp/e2e-trace.pid); rm -f /tmp/e2e-trace.pid' >/dev/null 2>&1 || true
	if [[ -n "${1:-}" ]]; then
		docker compose -f "$here/compose.yaml" exec -T minio cat /tmp/e2e-trace.log >"$E2E_DIR/$1" || true
	fi
}

scope_key() { echo "${CACHESERVICE_S3_PREFIX:+${CACHESERVICE_S3_PREFIX}/}${E2E_SCOPE}/$1"; }
bundle_json() { mc_minio "mc cat local/${CACHESERVICE_S3_BUCKET}/$(scope_key "bundles/$1.json")"; }
# bundle_objects: the stored bundles' keys, one per line.
bundle_objects() { mc_minio "mc ls local/${CACHESERVICE_S3_BUCKET}/$(scope_key bundles/)" | awk '{print $NF}' | sort; }

# log_lines FILE: the number of lines in a log, to read what comes after.
log_lines() { wc -l <"$1"; }
# log_since FILE LINE: a log from line LINE on.
log_since() { tail -n +"$(($2 + 1))" "$1"; }

# Each scenario's checks: check OK TEXT prints TEXT, prefixed with FAIL when
# OK isn't 1, and counts the failures.
failures=0
check() {
	if [[ "$1" == 1 ]]; then
		printf '  ok    %s\n' "$2"
	else
		printf '  FAIL  %s\n' "$2"
		failures=$((failures + 1))
	fi
}
# ok_if CONDITION...: 1 if the command succeeds, else 0, for check.
ok_if() { if "$@" >/dev/null 2>&1; then echo 1; else echo 0; fi; }

# cache_spans CACHEID FROM-LINE: the service's cache span log lines for a
# cache, from a line of its log on.
cache_spans() {
	tail -n +"$2" "$E2E_DIR/cacheservice.log" | grep 'msg="cache span"' | grep -F "cache=$1/" || true
}

case "${1:-run}" in
down)
	stop_all
	exit 0
	;;
run) ;;
*) die "usage: e2e.sh [run|down]" ;;
esac

# Before anything is deleted: a service answering that this E2E_DIR didn't
# start belongs to another run, and stopping its engines or resetting MinIO
# would wipe that run's caches and data.
own_service=0
if [[ -f "$E2E_DIR/cacheservice.pid" ]] && kill -0 "$(cat "$E2E_DIR/cacheservice.pid")" 2>/dev/null; then
	own_service=1
fi
if ((!own_service)) && curl -fsS -o /dev/null "$debug_url/debug/engines" 2>/dev/null; then
	die "a cacheservice already answers on ${debug_url}: stop it first (dev.sh e2e-down, with the E2E_DIR it was started with)"
fi
stop_all
mkdir -p "$E2E_DIR"
find "$E2E_DIR" -mindepth 1 -maxdepth 1 ! -name bin -exec rm -rf {} +
build
start_minio
start_service

log "Starting engines A and B, each with an empty cache"
conn_a="$(start_engine a)"
conn_b="$(start_engine b)"
cache_b="$(curl -fsS "$debug_url/debug/engines" | jq -r ".[] | select(.conn == $conn_b) | .cacheID")"
echo "engine A is connection ${conn_a}, engine B is connection ${conn_b}" >&2

log "Running the pipeline on A"
time_a="$(run_query a run-a <<<"$pipeline")"
stamp_a="$(stamp run-a)"
printf 'A took %ss. Stamp:\n%s\n' "$time_a" "$stamp_a" >&2

log "Waiting for the service to store A's entries in MinIO"
# Every retained entry, not just the first: a session can be exported in
# several batches, and the index read below must hold all of them.
wait_for "$E2E_TIMEOUT" "all of A's retained entries to reach the index" \
	sh -c "curl -fsS '$debug_url/debug/engines/$conn_a/entries' | jq -e '[.[] | select(.retained)] | length > 0 and all(.stored)'"
index_json >"$E2E_DIR/index-after-a.json"
jq -e '(.entries // {}) | length > 0' "$E2E_DIR/index-after-a.json" >/dev/null ||
	die "the index in MinIO has no entries: $E2E_DIR/index-after-a.json"
curl -fsS "$debug_url/debug/engines/$conn_a/entries" >"$E2E_DIR/entries-a.json"
# The layers B must read from MinIO: the longest fs chain in A's bundles,
# the last exec's container (alpine's layer and both execs').
for bundle in $(jq -r '[.entries[].bundle] | unique[]' "$E2E_DIR/index-after-a.json"); do
	mc_minio "mc cat local/${CACHESERVICE_S3_BUCKET}/${CACHESERVICE_S3_PREFIX:+${CACHESERVICE_S3_PREFIX}/}${E2E_SCOPE}/bundles/${bundle}.json"
done | jq -rs '[.[].outputs[]? | select(.address.part == "fs")
	| [.. | objects | select(has("digest") and has("mediaType")) | .digest]]
	| max_by(length) // [] | .[]' >"$E2E_DIR/layers-a.txt"
[[ -s "$E2E_DIR/layers-a.txt" ]] || die "A's bundles name no fs layers: $E2E_DIR/index-after-a.json"
echo "index: $(jq '.entries | length' "$E2E_DIR/index-after-a.json") entries, $(jq '.roots | length' "$E2E_DIR/index-after-a.json") roots" >&2

log "Waiting for the service to merge them into B"
wait_for "$E2E_TIMEOUT" "a merge into B" \
	sh -c "curl -fsS '$debug_url/debug/engines/$conn_b/imports' | jq -e '.merged | length > 0'"
curl -fsS "$debug_url/debug/engines/$conn_b/imports" >"$E2E_DIR/imports-b-merged.json"
jq -c '.merged[] | {bundle: .bundle[0:12], roots, retained, expired, values}' "$E2E_DIR/imports-b-merged.json" >&2

start_trace

log "Running the same pipeline on B"
log_line=$(($(wc -l <"$E2E_DIR/cacheservice.log") + 1))
time_b="$(run_query b run-b <<<"$pipeline")"
stamp_b="$(stamp run-b)"
printf 'B took %ss. Stamp:\n%s\n' "$time_b" "$stamp_b" >&2
wait_for 30 "B's cache spans" sh -c "tail -n +$log_line '$E2E_DIR/cacheservice.log' | grep 'msg=\"cache span\"' | grep -qF 'cache=$cache_b/'"
cache_spans "$cache_b" "$log_line" >"$E2E_DIR/spans-b.log"
execs_run_b="$(grep 'name=Container.withExec' "$E2E_DIR/spans-b.log" | grep -c 'outcome=executed' || true)"
execs_hit_b="$(grep 'name=Container.withExec' "$E2E_DIR/spans-b.log" | grep -c 'outcome=hit' || true)"

log "Reading the layers on B: one more exec over the pipeline's container"
time_verify="$(run_query b verify-b < <(verify "$(date +%s%N)"))"
verified="$(jq -r '.container.from.withExec.withExec.withExec.stdout' "$E2E_DIR/verify-b.json")"
printf 'took %ss. Output:\n%s\n' "$time_verify" "$verified" >&2

stop_trace
docker compose -f "$here/compose.yaml" exec -T minio cat /tmp/e2e-trace.log >"$E2E_DIR/minio-trace-b.log" || true

log "Waiting for the service to store B's new entries, and merge them into A"
wait_for "$E2E_TIMEOUT" "all of B's retained entries to be stored" \
	sh -c "curl -fsS '$debug_url/debug/engines/$conn_b/entries' | jq -e '[.[] | select(.retained)] | length > 0 and all(.stored)'"
wait_for "$E2E_TIMEOUT" "a merge into A" \
	sh -c "curl -fsS '$debug_url/debug/engines/$conn_a/imports' | jq -e '.merged | length > 0'"
curl -fsS "$debug_url/debug/engines/$conn_b/entries" >"$E2E_DIR/entries-b.json"
curl -fsS "$debug_url/debug/engines/$conn_b/imports" >"$E2E_DIR/imports-b.json"
curl -fsS "$debug_url/debug/engines/$conn_a/imports" >"$E2E_DIR/imports-a.json"
index_json >"$E2E_DIR/index-final.json"
# The index must hold A's entries, unchanged, and only the entries B built
# itself (the verify exec's), each once (#35): B's imports keep A's numbers,
# so no number is handed out for them, and none of B's entries is one it
# imported. Nothing expires during a run, so every number handed out is an
# entry.
entries_after_a="$(jq '.entries | length' "$E2E_DIR/index-after-a.json")"
entries_final="$(jq '.entries | length' "$E2E_DIR/index-final.json")"
entries_b="$(jq --arg b "$cache_b" '[.entries[] | select(.source.cacheID == $b)] | length' "$E2E_DIR/index-final.json")"
numbers_final="$(jq '.nextNumber - 1' "$E2E_DIR/index-final.json")"
a_unchanged="$(jq -n --slurpfile before "$E2E_DIR/index-after-a.json" --slurpfile after "$E2E_DIR/index-final.json" \
	'$before[0].entries | to_entries | all(.key as $k | .value.source == $after[0].entries[$k].source)')"
reexported="$(jq -n --arg b "$cache_b" --slurpfile idx "$E2E_DIR/index-final.json" --slurpfile im "$E2E_DIR/imports-b.json" \
	'[$im[0].values[].number] as $imported | [$idx[0].entries[] | select(.source.cacheID == $b and (.source.number | IN($imported[])))] | length')"
# The merges into A carry only B's new entries, not A's own back.
echoed="$(jq -n --slurpfile before "$E2E_DIR/index-after-a.json" --slurpfile im "$E2E_DIR/imports-a.json" \
	'[$im[0].merged[].roots[] | tostring | select(IN($before[0].entries | keys[]))] | length')"
grep 'GetObject' "$E2E_DIR/minio-trace-b.log" | grep '/blobs/' >"$E2E_DIR/minio-blob-gets-b.log" || true
blob_gets="$(grep -c . "$E2E_DIR/minio-blob-gets-b.log" || true)"
# Every layer of A's chain must be among them, not just some.
missing_layers=()
while read -r digest; do
	grep -qF "/blobs/${digest/://}" "$E2E_DIR/minio-blob-gets-b.log" || missing_layers+=("$digest")
done <"$E2E_DIR/layers-a.txt"
expected_layers="$(grep -c . "$E2E_DIR/layers-a.txt")"

# The layer files B read must be A's: their checksums are in A's stamps.
sums_a="$(awk '{print $3}' <<<"$stamp_a")"
sums_b="$(tail -n 2 <<<"$verified")"

log "Result"
ok=1
if [[ -n "$stamp_a" && "$stamp_a" == "$stamp_b" ]]; then
	echo "cache hit:   B's stamps are A's, so neither exec ran on B"
else
	echo "MISS:        B's stamps differ from A's, so B ran the execs itself"
	ok=0
fi
echo "B's spans:   Container.withExec ${execs_hit_b} hit, ${execs_run_b} executed"
# The pipeline has two withExec calls: both must be hits.
((execs_run_b == 0 && execs_hit_b == 2)) || ok=0
if [[ "$sums_a" == "$sums_b" ]]; then
	echo "layers:      the layer files on B are A's (checksums match A's stamps)"
else
	echo "WRONG LAYERS: the layer files on B are not A's"
	ok=0
fi
if ((entries_b > 0 && entries_final == entries_after_a + entries_b && numbers_final == entries_final && reexported == 0 && echoed == 0)) && [[ "$a_unchanged" == true ]]; then
	echo "index:       ${entries_final} entries: A's ${entries_after_a}, unchanged, and ${entries_b} new from B's verify exec; no import stored again, nothing echoed to A"
else
	echo "DUPLICATES:  the index has ${entries_final} entries (A's ${entries_after_a}, ${entries_b} from B, ${reexported} of them B's imports), ${numbers_final} numbers handed out, A's unchanged: ${a_unchanged}, ${echoed} of A's own merged back into A"
	ok=0
fi
if ((blob_gets > 0 && ${#missing_layers[@]} == 0)); then
	echo "from S3:     B's engine read all ${expected_layers} layers of A's chain from MinIO (${blob_gets} blob GETs):"
	sed -E 's/^.*(GetObject [^ ]*\/blobs\/[a-z0-9]+\/[0-9a-f]{12})[^ ]*.*$/               \1.../' "$E2E_DIR/minio-blob-gets-b.log"
else
	echo "NOT FROM S3: MinIO didn't serve B these layers of A's chain: ${missing_layers[*]:-all of them}"
	ok=0
fi
echo "timings:     A ${time_a}s (empty cache, builds)"
echo "             B ${time_b}s (empty cache, entries merged from S3)"
echo "             B verify ${time_verify}s (one new exec, layers pulled from MinIO)"
echo "files:       $E2E_DIR"
((ok)) || failures=$((failures + 1))

# The checks for the verify exec, against A's: which lines of its output are
# A's. verify_lines OUT: the verify output in OUT.json.
verify_output() { jq -r '.container.from.withExec.withExec.withExec.stdout' "$E2E_DIR/$1.json"; }
cache_of() { engine_field "$1" "$2" cacheID; }

# ---------------------------------------------------------------------------
# #30: a repeated export uploads nothing new.
scenario_reexport() {
	log "#30: exporting A's stored entries again (POST /debug/engines/$conn_a/export)"
	local req res t0 t1 alpine line puts uploaded present new_bundles
	curl -fsS "$debug_url/debug/engines/$conn_a/entries" >"$E2E_DIR/reexport-entries-a.json"
	# Every stored root, with every part the tracker knows of, as the
	# tracker's own export sends them.
	req="$(jq -c '[.[] | select(.retained and .stored)]
		| {roots: [.[].number], outputs: [.[] as $e | ($e.parts // [])[] | {number: $e.number, address: fromjson}]}' \
		"$E2E_DIR/reexport-entries-a.json")"
	echo "$req" >"$E2E_DIR/reexport-request.json"
	index_json >"$E2E_DIR/reexport-index-before.json"
	bundle_objects >"$E2E_DIR/reexport-bundles-before.txt"
	t0="$(now)"
	curl -fsS -XPOST "$debug_url/debug/engines/$conn_a/export" -d "$req" >"$E2E_DIR/reexport-result.json" ||
		die "the export hook failed: $E2E_DIR/reexport-result.json"
	t1="$(now)"
	index_json >"$E2E_DIR/reexport-index-after.json"
	bundle_objects >"$E2E_DIR/reexport-bundles-after.txt"
	res="$E2E_DIR/reexport-result.json"
	jq -c '{stored, uploaded: (.uploaded // []), present: (.present // []), bundles}' "$res" >&2

	echo "#30, repeated export ($(elapsed "$t0" "$t1")s):"
	check "$(ok_if jq -e --argjson req "$req" '(.stored // [] | sort) == ($req.roots | sort)' "$res")" \
		"every root stored again: $(jq -c '.stored' "$res")"
	check "$(ok_if jq -e '(.uploaded // []) | length == 0' "$res")" \
		"no blob uploaded (uploaded: $(jq -c '.uploaded // []' "$res"))"
	local missing=0
	while read -r d; do
		jq -e --arg d "$d" '(.present // []) | index($d)' "$res" >/dev/null || missing=$((missing + 1))
	done <"$E2E_DIR/layers-a.txt"
	check "$(ok_if test "$missing" = 0)" \
		"every blob present: $(jq '.present // [] | length' "$res") blobs, all $(grep -c . "$E2E_DIR/layers-a.txt") fs layers of A's chain among them"
	check "$(ok_if jq -e -n --slurpfile b "$E2E_DIR/reexport-index-before.json" --slurpfile a "$E2E_DIR/reexport-index-after.json" \
		'$b[0].entries == $a[0].entries and $b[0].nextNumber == $a[0].nextNumber')" \
		"the index is unchanged: the entries keep their service numbers and bundles ($(jq '.entries | length' "$E2E_DIR/reexport-index-after.json") entries, next number $(jq '.nextNumber' "$E2E_DIR/reexport-index-after.json"))"
	check "$(ok_if cmp -s "$E2E_DIR/reexport-bundles-before.txt" "$E2E_DIR/reexport-bundles-after.txt")" \
		"no new bundle: $(grep -c . "$E2E_DIR/reexport-bundles-after.txt") stored, as before; the export stored $(jq -r '.bundles // [] | map(.[0:12]) | join(", ")' "$res")"

	log "#30: a second pipeline on A over the same alpine"
	alpine="$(head -n 1 "$E2E_DIR/layers-a.txt")"
	line="$(log_lines "$E2E_DIR/cacheservice.log")"
	start_trace
	run_query a reexport-run-a2 <<<'{ container { from(address: "alpine:3.22") {
		withExec(args: ["sh", "-c", "head -c 2097152 /dev/urandom > /other.bin && sha256sum /other.bin | cut -c1-16 > /other"]) {
			file(path: "/other") { contents } } } } }' >/dev/null
	wait_for "$E2E_TIMEOUT" "the second pipeline's export" \
		sh -c "tail -n +$((line + 1)) '$E2E_DIR/cacheservice.log' | grep 'msg=\"export stored\"' | grep -qF 'cacheID=$(cache_of main "$conn_a") '"
	wait_for "$E2E_TIMEOUT" "all of A's retained entries to be stored" \
		sh -c "curl -fsS '$debug_url/debug/engines/$conn_a/entries' | jq -e '[.[] | select(.retained)] | all(.stored)'"
	stop_trace reexport-trace-a2.log
	log_since "$E2E_DIR/cacheservice.log" "$line" | grep 'msg="export stored"' >"$E2E_DIR/reexport-exports-a2.log"
	uploaded="$(grep -o 'uploaded=[0-9]*' "$E2E_DIR/reexport-exports-a2.log" | cut -d= -f2 | awk '{ n += $1 } END { print n + 0 }')"
	present="$(grep -o 'present=[0-9]*' "$E2E_DIR/reexport-exports-a2.log" | cut -d= -f2 | awk '{ n += $1 } END { print n + 0 }')"
	new_bundles="$(grep -o 'bundle=[0-9a-f]*' "$E2E_DIR/reexport-exports-a2.log" | cut -d= -f2)"
	grep 'PutObject' "$E2E_DIR/reexport-trace-a2.log" | grep '/blobs/' >"$E2E_DIR/reexport-puts-a2.log" || true
	puts="$(grep -c . "$E2E_DIR/reexport-puts-a2.log" || true)"
	local in_bundle=0
	for b in $new_bundles; do
		bundle_json "$b" | jq -e --arg d "$alpine" '[.outputs[]?.chain.layers[]?.Descriptor.digest] | index($d)' >/dev/null && in_bundle=1
	done

	echo "#30, a second pipeline sharing alpine's layer:"
	check "$(ok_if test "${uploaded:-0}" -gt 0 -a "$puts" = "${uploaded:-0}")" \
		"it uploaded only its own layers: ${uploaded:-0} blobs, ${puts} PUTs in MinIO's trace"
	check "$(ok_if test "${present:-0}" -gt 0 -a "$in_bundle" = 1)" \
		"alpine's layer is in its bundle, and was present (${present:-0} blobs present)"
	check "$(ok_if bash -c "! grep -qF '/blobs/${alpine/://}' '$E2E_DIR/reexport-puts-a2.log'")" \
		"alpine's layer (${alpine:7:12}...) was not uploaded again"
}

# ---------------------------------------------------------------------------
# #31: a pull survives presigned GET expiry through renew; a missing layer
# degrades to a cache miss.
renew_started=0
scenario_renew() {
	start_replica
	log "#31: starting engines C and D on the replica, each with an empty cache"
	conn_c="$(start_engine c replica)"
	conn_d="$(start_engine d replica)"
	local roots merged_at expires_in line t_c t_d out_c out_d top
	roots="$(jq -c '.roots | keys | map(tonumber)' "$E2E_DIR/index-after-a.json")"
	for e in c d; do
		local conn
		conn="$([[ $e == c ]] && echo "$conn_c" || echo "$conn_d")"
		wait_for "$E2E_TIMEOUT" "the replica to merge A's entries into ${e^^}" \
			sh -c "curl -fsS '$replica_debug_url/debug/engines/$conn/imports' | jq -e --argjson r '$roots' '[.merged[].roots[]] as \$m | \$r - \$m | length == 0'"
		curl -fsS "$replica_debug_url/debug/engines/$conn/imports" >"$E2E_DIR/renew-imports-$e-merged.json"
	done
	merged_at="$(date +%s)"
	renew_started=1
	expires_in=$((E2E_GET_EXPIRY + 2))
	log "#31: waiting ${expires_in}s for the merged GETs to expire"
	sleep "$expires_in"

	log "#31: reading the layers on C, after the GETs expired"
	line="$(log_lines "$E2E_DIR/replica.log")"
	start_trace
	t_c="$(run_query c renew-verify-c replica < <(verify "$(date +%s%N)"))"
	stop_trace renew-trace-c.log
	log_since "$E2E_DIR/replica.log" "$line" | grep -E "conn=$conn_c dir=(in|out) type=renew" >"$E2E_DIR/renew-envelopes-c.log" || true
	out_c="$(verify_output renew-verify-c)"
	printf 'took %ss, %ss after the merge. Output:\n%s\n' "$t_c" "$(($(date +%s) - merged_at))" "$out_c" >&2
	grep 'GetObject' "$E2E_DIR/renew-trace-c.log" | grep '/blobs/' >"$E2E_DIR/renew-gets-c.log" || true
	local missing=()
	while read -r d; do
		grep -F "/blobs/${d/://}" "$E2E_DIR/renew-gets-c.log" | grep -q '20[06]' || missing+=("${d:7:12}")
	done <"$E2E_DIR/layers-a.txt"

	echo "#31, C reads after the GETs expired (${E2E_GET_EXPIRY}s):"
	check "$(ok_if grep -q 'type=renew ' "$E2E_DIR/renew-envelopes-c.log")" \
		"C sent renew: $(grep -c 'type=renew ' "$E2E_DIR/renew-envelopes-c.log" || true) renew, for $(grep -o 'layers=[0-9]*' "$E2E_DIR/renew-envelopes-c.log" | paste -sd, -) layers"
	check "$(ok_if bash -c "grep 'type=renewed' '$E2E_DIR/renew-envelopes-c.log' | grep -q 'unavailable=false' && ! grep -q 'unavailable=true' '$E2E_DIR/renew-envelopes-c.log'")" \
		"the replica answered with fresh GETs: $(grep 'type=renewed' "$E2E_DIR/renew-envelopes-c.log" | grep -o 'addresses=[0-9]*' | paste -sd, -)"
	check "$(ok_if test "${#missing[@]}" = 0 -a -s "$E2E_DIR/renew-gets-c.log")" \
		"MinIO served C every layer of A's chain: $(grep -c . "$E2E_DIR/renew-gets-c.log" || true) blob GETs${missing:+, missing ${missing[*]}}"
	check "$(ok_if test "$out_c" = "$(printf '%s\n%s' "$stamp_a" "$sums_a")")" \
		"the files C read are A's: its stamps and checksums"

	# C's verify exec is exported next, and its upload would put back a
	# layer deleted before it: the service asks only for the blobs the store
	# lacks, and C has them all on disk now.
	wait_for "$E2E_TIMEOUT" "C's verify exec to be stored" \
		sh -c "curl -fsS '$replica_debug_url/debug/engines/$conn_c/entries' | jq -e '[.[] | select(.retained)] | length > 0 and all(.stored)'"
	log "#31: deleting the last exec's layer from MinIO, and reading the layers on D"
	top="$(tail -n 1 "$E2E_DIR/layers-a.txt")"
	mc_minio "mc rm local/${CACHESERVICE_S3_BUCKET}/${CACHESERVICE_S3_PREFIX:+${CACHESERVICE_S3_PREFIX}/}blobs/${top/://}" >&2
	if mc_minio "mc stat local/${CACHESERVICE_S3_BUCKET}/${CACHESERVICE_S3_PREFIX:+${CACHESERVICE_S3_PREFIX}/}blobs/${top/://}" >/dev/null 2>&1; then
		die "exec2's layer ${top} is still in MinIO after mc rm"
	fi
	line="$(log_lines "$E2E_DIR/replica.log")"
	start_trace
	t_d="$(run_query d renew-verify-d replica < <(verify "$(date +%s%N)"))"
	stop_trace renew-trace-d.log
	log_since "$E2E_DIR/replica.log" "$line" | grep -E "conn=$conn_d dir=(in|out) type=renew|renew of a missing layer" >"$E2E_DIR/renew-envelopes-d.log" || true
	out_d="$(verify_output renew-verify-d)"
	printf 'took %ss. Output:\n%s\n' "$t_d" "$out_d" >&2
	local stamps_d sums_d
	stamps_d="$(head -n 2 <<<"$out_d")"
	sums_d="$(tail -n 2 <<<"$out_d")"

	echo "#31, D reads with a layer missing from MinIO:"
	check "$(ok_if bash -c "grep 'type=renewed' '$E2E_DIR/renew-envelopes-d.log' | grep -q 'unavailable=true' && grep -qF 'digest=$top' '$E2E_DIR/renew-envelopes-d.log'")" \
		"renew of the chain with the deleted layer answered unavailable ($(grep -c 'type=renew ' "$E2E_DIR/renew-envelopes-d.log" || true) renew, $(grep 'type=renewed' "$E2E_DIR/renew-envelopes-d.log" | grep -o 'unavailable=[a-z]*' | paste -sd, -))"
	check "$(ok_if test "$(head -n 1 <<<"$stamps_d")" = "$(head -n 1 <<<"$stamp_a")" -a "$(head -n 1 <<<"$sums_d")" = "$(head -n 1 <<<"$sums_a")")" \
		"exec1's files on D are A's"
	check "$(ok_if test "$(tail -n 1 <<<"$stamps_d")" != "$(tail -n 1 <<<"$stamp_a")" -a "$(tail -n 1 <<<"$sums_d")" != "$(tail -n 1 <<<"$sums_a")")" \
		"exec2 ran again on D, as for a cache miss: new stamp and checksum, and the query succeeded"
	echo "timings:     C ${t_c}s (layers through renewed GETs), D ${t_d}s (exec2 recomputed)"
}

# ---------------------------------------------------------------------------
# #32: entries an engine prunes are not merged back into it until it
# restarts.
scenario_prune() {
	local before after forced cache_c gen_c line merges new_conn polls wait_s
	if [[ -z "${conn_c:-}" ]]; then
		start_replica
		log "#32: starting engine C on the replica"
		conn_c="$(start_engine c replica)"
		local roots
		roots="$(jq -c '.roots | keys | map(tonumber)' "$E2E_DIR/index-after-a.json")"
		wait_for "$E2E_TIMEOUT" "the replica to merge A's entries into C" \
			sh -c "curl -fsS '$replica_debug_url/debug/engines/$conn_c/imports' | jq -e --argjson r '$roots' '[.merged[].roots[]] as \$m | \$r - \$m | length == 0'"
	fi
	cache_c="$(cache_of replica "$conn_c")"
	gen_c="$(engine_field replica "$conn_c" generation)"
	# Earlier scenarios' exports may still be on their way to C: wait until
	# every engine's entries are stored, and C's merges stop changing for
	# three polls, so that what is merged after the prune can only be the
	# pruned entries coming back.
	for conn in $(curl -fsS "$replica_debug_url/debug/engines" | jq -r '.[].conn'); do
		wait_for "$E2E_TIMEOUT" "the replica's exports of connection $conn" \
			sh -c "curl -fsS '$replica_debug_url/debug/engines/$conn/entries' | jq -e '[.[] | select(.retained)] | all(.stored)'"
	done
	local settled=0 prev cur
	prev="$(curl -fsS "$replica_debug_url/debug/engines/$conn_c/imports" | jq -c '[.merged[].bundle] | sort')"
	while ((settled < 3)); do
		sleep "$E2E_IMPORT_POLL"
		cur="$(curl -fsS "$replica_debug_url/debug/engines/$conn_c/imports" | jq -c '[.merged[].bundle] | sort')"
		if [[ "$cur" == "$prev" ]]; then settled=$((settled + 1)); else settled=0; prev="$cur"; fi
	done
	curl -fsS "$replica_debug_url/debug/engines/$conn_c/imports" >"$E2E_DIR/prune-imports-before.json"
	before="$(jq '.values | length' "$E2E_DIR/prune-imports-before.json")"
	merges="$(jq '.merged | length' "$E2E_DIR/prune-imports-before.json")"

	log "#32: pruning C's cache ({ engine { localCache { prune } } })"
	run_query c prune-entries-before replica <<<'{ engine { localCache { entrySet { entryCount diskSpaceBytes } } } }' >/dev/null
	line="$(log_lines "$E2E_DIR/replica.log")"
	run_query c prune-run replica <<<'{ engine { localCache { prune } } }' >/dev/null
	wait_for 60 "C's engine.prune event" \
		sh -c "tail -n +$((line + 1)) '$E2E_DIR/replica.log' | grep 'kind=engine.prune' | grep -qF 'cache=$cache_c/'"
	wait_for 30 "the importer to forget C's pruned entries" \
		sh -c "curl -fsS '$replica_debug_url/debug/engines/$conn_c/imports' | jq -e '.values | length < $before'"
	log_since "$E2E_DIR/replica.log" "$line" | grep 'kind=engine.prune' | grep -F "cache=$cache_c/" >"$E2E_DIR/prune-event.log"
	curl -fsS "$replica_debug_url/debug/engines/$conn_c/imports" >"$E2E_DIR/prune-imports-pruned.json"
	after="$(jq '.values | length' "$E2E_DIR/prune-imports-pruned.json")"
	run_query c prune-entries-after replica <<<'{ engine { localCache { entrySet { entryCount diskSpaceBytes } } } }' >/dev/null

	polls=5
	wait_s=$((E2E_IMPORT_POLL * polls + 1))
	log "#32: waiting ${wait_s}s (${polls} import polls), then syncing C 3 times by hand"
	sleep "$wait_s"
	for i in 1 2 3; do
		curl -fsS -XPOST "$replica_debug_url/debug/engines/$conn_c/import" >"$E2E_DIR/prune-sync-$i.json"
	done
	forced="$(jq -s '[.[].merged // [] | length] | add' "$E2E_DIR"/prune-sync-*.json)"
	curl -fsS "$replica_debug_url/debug/engines/$conn_c/imports" >"$E2E_DIR/prune-imports-polled.json"
	log_since "$E2E_DIR/replica.log" "$line" | grep -E "conn=$conn_c dir=out type=(merge|offer) " >"$E2E_DIR/prune-merges-after.log" || true
	# Bundles merged into C after the prune that it had merged before.
	local remerged
	remerged="$(log_since "$E2E_DIR/replica.log" "$line" | grep 'msg="import: merged"' | grep -F "cacheID=$cache_c " |
		grep -o 'bundle=[0-9a-f]*' | cut -d= -f2 | while read -r b; do
			if jq -e --arg b "$b" '[.merged[].bundle] | index($b)' "$E2E_DIR/prune-imports-before.json" >/dev/null; then echo "${b:0:12}"; fi
		done | paste -sd, -)" || true

	log "#32: restarting C"
	docker restart "$(container c)" >/dev/null
	new_conn="$(wait_conn c replica "$conn_c")"
	wait_for "$E2E_TIMEOUT" "the replica to merge into C again" \
		sh -c "curl -fsS '$replica_debug_url/debug/engines/$new_conn/imports' | jq -e '.merged | length >= $merges'"
	sleep 1
	curl -fsS "$replica_debug_url/debug/engines/$new_conn/imports" >"$E2E_DIR/prune-imports-restarted.json"

	echo "#32, pruned entries aren't merged back (engine.localCache.prune on C; import poll ${E2E_IMPORT_POLL}s):"
	check "$(ok_if test "$after" -lt "$before")" \
		"C pruned imported entries: its engine.prune event named $(grep -o 'resultId' "$E2E_DIR/prune-event.log" | wc -l) entries; the importer holds ${after} of its ${before} imported values; disk $(jq '.engine.localCache.entrySet.diskSpaceBytes' "$E2E_DIR/prune-entries-before.json") -> $(jq '.engine.localCache.entrySet.diskSpaceBytes' "$E2E_DIR/prune-entries-after.json") bytes"
	check "$(ok_if test -z "$remerged" -a ! -s "$E2E_DIR/prune-merges-after.log" -a "$forced" = 0)" \
		"no merge or offer to C over ${polls} polls and 3 syncs by hand ($(grep -c . "$E2E_DIR/prune-merges-after.log" || true) sent, ${forced} merged by the syncs${remerged:+, merged again: $remerged})"
	check "$(ok_if jq -e -n --slurpfile p "$E2E_DIR/prune-imports-pruned.json" --slurpfile q "$E2E_DIR/prune-imports-polled.json" \
		'($p[0].values | length) == ($q[0].values | length) and ($p[0].merged | length) == ($q[0].merged | length)')" \
		"the importer's state for C is as the prune left it"
	check "$(ok_if jq -e --argjson g "$gen_c" --argjson n "$before" '.generation > $g and (.values | length) >= $n' "$E2E_DIR/prune-imports-restarted.json")" \
		"after C restarted (generation $(jq '.generation' "$E2E_DIR/prune-imports-restarted.json")), the replica merged $(jq '.merged | length' "$E2E_DIR/prune-imports-restarted.json") bundles into it again, $(jq '.values | length' "$E2E_DIR/prune-imports-restarted.json") values"
}

# ---------------------------------------------------------------------------
# #32 (#27): which record of a replaced value a new engine ends up with.
scenario_superseded() {
	local mod="$E2E_DIR/ttlmod" version cache_a line n1 n2 s1 s2 e1 b1 b2 l1 l2 conn_f cache_f t_f read_f wait_s
	version="$(engine_field main "$conn_a" engineVersion)"
	cache_a="$(cache_of main "$conn_a")"
	mkdir -p "$mod"
	cat >"$mod/dagger.json" <<-EOF
		{"name": "ttlmod", "engineVersion": "${version}", "sdk": "dang"}
	EOF
	# Nothing depends on stamp2's result in A's cache, so when it has expired
	# the engine replaces its value in place (same entry, one more
	# replacement) rather than computing it into a new entry.
	cat >"$mod/main.dang" <<-EOF
		type Ttlmod {
		  pub stamp2(): Container! @cache(ttl: "${E2E_TTL}s") {
		    container.from("alpine:3.22").withExec(["sh", "-c", "cat /proc/sys/kernel/random/uuid > /stamp2"])
		  }
		}
	EOF

	# superseded_export N OUT: waits for A's entry N to be stored, and
	# prints its service number, bundle and fs layer as the index has it.
	superseded_export() {
		wait_for "$E2E_TIMEOUT" "A's entry $1 to be stored" \
			sh -c "curl -fsS '$debug_url/debug/engines/$conn_a/entries' | jq -e '.[] | select(.number == $1) | .stored'"
		index_json >"$E2E_DIR/$2-index.json"
		local s b o
		s="$(jq -r --arg c "$cache_a" --arg n "$1" '.caches[$c].numbers[$n].number' "$E2E_DIR/$2-index.json")"
		b="$(jq -r --arg s "$s" '.entries[$s].bundle' "$E2E_DIR/$2-index.json")"
		o="$(jq -r --arg s "$s" '.entries[$s].ordinal' "$E2E_DIR/$2-index.json")"
		bundle_json "$b" >"$E2E_DIR/$2-bundle.json"
		echo "$s $b $(jq -r --argjson o "$o" '[.outputs[] | select(.ordinal == $o and .address.part == "fs") | .chain.layers[-1].Descriptor.digest][0]' "$E2E_DIR/$2-bundle.json") $(jq -r --arg s "$s" '.entries[$s].expiresAtUnix // 0' "$E2E_DIR/$2-index.json") $(jq -r --arg s "$s" '.entries[$s].replacements // 0' "$E2E_DIR/$2-index.json")"
	}
	# stamp2_entry FROM-LINE CACHE: the engine entry of the latest
	# Ttlmod.stamp2 span of a cache, and its outcome.
	stamp2_span() {
		log_since "$E2E_DIR/cacheservice.log" "$1" | grep 'msg="cache span"' | grep -F "cache=$2/" | grep 'name=Ttlmod.stamp2 ' | tail -n 1
	}

	log "#32/#27: running a Dang function with a ${E2E_TTL}s cache TTL on A"
	line="$(log_lines "$E2E_DIR/cacheservice.log")"
	run_cli a superseded-run-1 main -m "$mod" call stamp-2 sync >/dev/null
	wait_for 30 "A's Ttlmod.stamp2 span" sh -c "tail -n +$((line + 1)) '$E2E_DIR/cacheservice.log' | grep 'name=Ttlmod.stamp2 ' | grep -qF 'cache=$cache_a/'"
	n1="$(stamp2_span "$line" "$cache_a" | grep -o 'result=[0-9]*' | cut -d= -f2)"
	read -r s1 b1 l1 e1 _ < <(superseded_export "$n1" superseded-1)
	echo "first value: engine entry ${n1}, service number ${s1}, bundle ${b1:0:12}, layer ${l1:7:12}, expires $(date -u -d "@$e1" +%T)" >&2

	wait_s=$((e1 - $(date +%s) + 2))
	log "#32/#27: waiting ${wait_s}s for it to expire, then running it again on A"
	((wait_s > 0)) && sleep "$wait_s"
	line="$(log_lines "$E2E_DIR/cacheservice.log")"
	run_cli a superseded-run-2 main -m "$mod" call stamp-2 sync >/dev/null
	wait_for 30 "A's Ttlmod.stamp2 span" sh -c "tail -n +$((line + 1)) '$E2E_DIR/cacheservice.log' | grep 'name=Ttlmod.stamp2 ' | grep -qF 'cache=$cache_a/'"
	n2="$(stamp2_span "$line" "$cache_a" | grep -o 'result=[0-9]*' | cut -d= -f2)"
	wait_for "$E2E_TIMEOUT" "the replaced value to be stored" \
		sh -c "curl -fsS '$debug_url/debug/engines/$conn_a/entries' | jq -e '.[] | select(.number == $n2) | .stored and (.replacements // 0) > 0'"
	local r2
	read -r s2 b2 l2 _ r2 < <(superseded_export "$n2" superseded-2)
	echo "second value: engine entry ${n2}, service number ${s2}, bundle ${b2:0:12}, layer ${l2:7:12}, replacements ${r2}" >&2

	log "#32/#27: starting engine F, with an empty cache, and reading the value on it"
	conn_f="$(start_engine f)"
	cache_f="$(cache_of main "$conn_f")"
	wait_for "$E2E_TIMEOUT" "a merge of the newer bundle into F" \
		sh -c "curl -fsS '$debug_url/debug/engines/$conn_f/imports' | jq -e --arg b '$b2' '[.merged[].bundle] | index(\$b)'"
	curl -fsS "$debug_url/debug/engines/$conn_f/imports" >"$E2E_DIR/superseded-imports-f.json"
	jq -c --arg b1 "$b1" --arg b2 "$b2" '.merged[] | select(.bundle == $b1 or .bundle == $b2) | {bundle: .bundle[0:12], roots, retained, expired}' "$E2E_DIR/superseded-imports-f.json" >&2
	line="$(log_lines "$E2E_DIR/cacheservice.log")"
	start_trace
	t_f="$(run_cli f superseded-read-f main -m "$mod" call stamp-2 file --path /stamp2 contents)"
	stop_trace superseded-trace-f.log
	wait_for 30 "F's Ttlmod.stamp2 span" sh -c "tail -n +$((line + 1)) '$E2E_DIR/cacheservice.log' | grep 'name=Ttlmod.stamp2 ' | grep -qF 'cache=$cache_f/'"
	read_f="$(stamp2_span "$line" "$cache_f")"
	echo "$read_f" >"$E2E_DIR/superseded-span-f.log"
	grep 'GetObject' "$E2E_DIR/superseded-trace-f.log" | grep '/blobs/' >"$E2E_DIR/superseded-gets-f.log" || true

	echo "#32 (#27), a value replaced after it expired (TTL ${E2E_TTL}s), then imported into an empty engine:"
	check "$(ok_if test "$n1" = "$n2" -a "${r2:-0}" -gt 0)" \
		"A replaced the value in place: engine entry ${n1}, then ${n2}, ${r2:-0} replacement(s)"
	check "$(ok_if test "$s1" = "$s2" -a "$b1" != "$b2" -a "$l1" != "$l2")" \
		"the index kept service number ${s1} and moved it to the newer bundle (${b1:0:12} -> ${b2:0:12}); the older bundle still holds the first record"
	# A function with an explicit cache TTL is evaluated again on every call
	# (core/sdk/dang/v2/entrypoint.go), so the hit is its body's exec, which
	# resolves to the imported entry.
	local imported_f exec_f
	imported_f="$(jq -r --arg s "$s2" '.values[$s].number // empty' "$E2E_DIR/superseded-imports-f.json")"
	exec_f="$(log_since "$E2E_DIR/cacheservice.log" "$line" | grep 'msg="cache span"' | grep -F "cache=$cache_f/" | grep 'name=Container.withExec ' | grep -F "result=${imported_f:-none} " | tail -n 1)"
	check "$(ok_if bash -c "grep -q 'outcome=hit' <<<'$exec_f' && grep -qF 'result=${imported_f:-none} ' <<<'$read_f'")" \
		"F hit the imported value: stamp2's exec hit F's entry ${imported_f:-?} (service number ${s2}), and stamp2 returned it ($(grep -o 'outcome=[a-z]*' <<<"$read_f") as a TTL function)"
	if grep -qF "/blobs/${l2/://}" "$E2E_DIR/superseded-gets-f.log" && ! grep -qF "/blobs/${l1/://}" "$E2E_DIR/superseded-gets-f.log"; then
		check 1 "F has the newer record: it read the second value's layer (${l2:7:12}...) from MinIO, not the first's (in ${t_f}s)"
	elif grep -qF "/blobs/${l1/://}" "$E2E_DIR/superseded-gets-f.log"; then
		# The outcome #27 is about; expected to fail until it is fixed.
		printf '  GAP   F has the older record: it read the first value'"'"'s layer (%s...), from a superseded record (known gap #27)\n' "${l1:7:12}"
	else
		check 0 "F read neither value's layer from MinIO: $E2E_DIR/superseded-gets-f.log"
	fi
}

for sc in "${all_scenarios[@]}"; do
	if wanted "$sc"; then
		echo
		"scenario_$sc"
	fi
done

echo
if ((failures > 0)); then
	echo "e2e: ${failures} check(s) failed. Files: $E2E_DIR"
	exit 1
fi
echo "e2e: all checks passed (scenarios: hit ${E2E_SCENARIOS}). Files: $E2E_DIR"
