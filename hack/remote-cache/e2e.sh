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
#      read the layers from MinIO, and prints both runs' wall-clock times.
#
# Everything is left running afterwards so it can be inspected: `dev.sh
# e2e-down` stops it. Logs and results go to E2E_DIR.
#
# Settings: E2E_DIR (default: $TMPDIR/dagger-remote-cache-e2e), E2E_BASE_IMAGE
# (the release image the binaries are copied over), E2E_SKIP_BUILD=1 (reuse
# the binaries in E2E_DIR/bin), CACHESERVICE_PORT (8090),
# CACHESERVICE_DEBUG_PORT (8091), E2E_TIMEOUT (seconds to wait for the
# export and the merge, 180), E2E_CA_FILE (a CA bundle the engines should
# trust, for hosts behind a TLS-intercepting proxy).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"

: "${E2E_DIR:=${TMPDIR:-/tmp}/dagger-remote-cache-e2e}"
: "${E2E_BASE_IMAGE:=registry.dagger.io/engine:v1.0.0-beta.22}"
: "${E2E_IMAGE:=dagger-remote-cache-e2e-engine}"
: "${CACHESERVICE_PORT:=8090}"
: "${CACHESERVICE_DEBUG_PORT:=8091}"
: "${E2E_TIMEOUT:=180}"
: "${E2E_SCOPE:=default}"

engines=(a b)
container() { echo "dagger-remote-cache-e2e-$1"; }
volume() { echo "dagger-remote-cache-e2e-$1"; }

# The engines reach the service and MinIO on the Docker bridge gateway. So
# does the CLI: the engine sends session spans to the CLI's DAGGER_CLOUD_URL.
gateway="$(docker network inspect bridge -f '{{(index .IPAM.Config 0).Gateway}}')"
service_url="http://${gateway}:${CACHESERVICE_PORT}"
debug_url="http://127.0.0.1:${CACHESERVICE_DEBUG_PORT}"

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

stop_all() {
	local e
	for e in "${engines[@]}"; do
		docker rm -f "$(container "$e")" >/dev/null 2>&1 || true
		docker volume rm -f "$(volume "$e")" >/dev/null 2>&1 || true
	done
	if [[ -f "$E2E_DIR/cacheservice.pid" ]]; then
		kill "$(cat "$E2E_DIR/cacheservice.pid")" 2>/dev/null || true
		rm -f "$E2E_DIR/cacheservice.pid"
	fi
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
	if curl -fsS -o /dev/null "$debug_url/debug/engines" 2>/dev/null; then
		die "a cacheservice already answers on ${debug_url}: stop it first (dev.sh e2e-down, with the E2E_DIR it was started with)"
	fi
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

# start_engine NAME: starts an engine with a new, empty cache volume and
# prints its connection number at the service.
start_engine() {
	local name="$1" before conn
	before="$(curl -fsS "$debug_url/debug/engines" | jq -r '[.[].conn] | max // 0')"
	docker run -d --privileged \
		--name "$(container "$name")" \
		--hostname "e2e-engine-${name}" \
		-v "$(volume "$name"):/var/lib/dagger" \
		-e _EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL="$service_url" \
		-e DAGGER_CLOUD_TOKEN="e2e-engine-${name}" \
		-e DAGGER_CLOUD_URL="$service_url" \
		"$E2E_IMAGE" --debug >/dev/null
	wait_for 120 "engine ${name} to connect to the service" \
		sh -c "curl -fsS '$debug_url/debug/engines' | jq -e '[.[] | select(.conn > $before and .engineName == \"e2e-engine-${name}\")] | length > 0'"
	conn="$(curl -fsS "$debug_url/debug/engines" |
		jq -r "[.[] | select(.conn > $before and .engineName == \"e2e-engine-${name}\")][0].conn")"
	echo "$conn"
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

# run_query NAME OUT: runs the query on stdin on an engine through the CLI,
# with the CLI's telemetry pointed at the service, and writes the result to
# OUT.json and the CLI's output to OUT.log. Prints the elapsed seconds.
run_query() {
	local name="$1" out="$2" start end
	start="$(now)"
	# An inherited TRACEPARENT (some CI and sandboxes set one, unsampled)
	# makes the CLI's trace unsampled, and then the engine records no session
	# spans: the service never hears of the entries.
	env -u TRACEPARENT -u TRACESTATE -u OTEL_EXPORTER_OTLP_ENDPOINT \
		-u OTEL_EXPORTER_OTLP_TRACES_ENDPOINT -u OTEL_EXPORTER_OTLP_LOGS_ENDPOINT \
		_EXPERIMENTAL_DAGGER_RUNNER_HOST="docker-container://$(container "$name")" \
		DAGGER_CLOUD_TOKEN="e2e-cli" \
		DAGGER_CLOUD_URL="$service_url" \
		DAGGER_NO_NAG=1 \
		"$E2E_DIR/bin/dagger" --progress=plain query \
		>"$E2E_DIR/${out}.json" 2>"$E2E_DIR/${out}.log" ||
		die "the query on ${name} failed: $E2E_DIR/${out}.log"
	end="$(now)"
	elapsed "$start" "$end"
}

stamp() { jq -r '.container.from.withExec.withExec.file.contents' "$E2E_DIR/$1.json"; }

mc_minio() {
	docker compose -f "$here/compose.yaml" exec -T minio sh -c \
		"mc alias set local http://localhost:9000 \"\$MINIO_ROOT_USER\" \"\$MINIO_ROOT_PASSWORD\" >/dev/null && $*"
}

index_json() { mc_minio "mc cat local/${CACHESERVICE_S3_BUCKET}/${CACHESERVICE_S3_PREFIX:+${CACHESERVICE_S3_PREFIX}/}${E2E_SCOPE}/index.json"; }

# MinIO's request trace, from start_trace until stop_trace, in the MinIO
# container's /tmp/e2e-trace.log.
start_trace() {
	docker compose -f "$here/compose.yaml" exec -d minio sh -c \
		'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && echo $$ >/tmp/e2e-trace.pid && exec mc admin trace local >/tmp/e2e-trace.log 2>&1'
	sleep 2
}
stop_trace() {
	docker compose -f "$here/compose.yaml" exec -T minio sh -c \
		'[ -f /tmp/e2e-trace.pid ] && kill $(cat /tmp/e2e-trace.pid); rm -f /tmp/e2e-trace.pid' >/dev/null 2>&1 || true
}

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

stop_all
rm -rf "$E2E_DIR/"{run-,verify-,minio-,imports-,entries-,index}* "$E2E_DIR/cacheservice.log"
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
wait_for "$E2E_TIMEOUT" "A's export to reach the index" \
	sh -c "curl -fsS '$debug_url/debug/engines/$conn_a/entries' | jq -e '[.[] | select(.stored)] | length > 0'"
index_json >"$E2E_DIR/index-after-a.json"
jq -e '(.entries // {}) | length > 0' "$E2E_DIR/index-after-a.json" >/dev/null ||
	die "the index in MinIO has no entries: $E2E_DIR/index-after-a.json"
curl -fsS "$debug_url/debug/engines/$conn_a/entries" >"$E2E_DIR/entries-a.json"
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
curl -fsS "$debug_url/debug/engines/$conn_b/imports" >"$E2E_DIR/imports-b.json"
index_json >"$E2E_DIR/index-final.json"
grep 'GetObject' "$E2E_DIR/minio-trace-b.log" | grep '/blobs/' >"$E2E_DIR/minio-blob-gets-b.log" || true
blob_gets="$(grep -c . "$E2E_DIR/minio-blob-gets-b.log" || true)"

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
if ((blob_gets > 0)); then
	echo "from S3:     B's engine read ${blob_gets} layer blob(s) from MinIO:"
	sed -E 's/^.*(GetObject [^ ]*\/blobs\/[a-z0-9]+\/[0-9a-f]{12})[^ ]*.*$/               \1.../' "$E2E_DIR/minio-blob-gets-b.log"
else
	echo "NOT FROM S3: MinIO saw no blob GETs while B ran"
	ok=0
fi
echo "timings:     A ${time_a}s (empty cache, builds)"
echo "             B ${time_b}s (empty cache, entries merged from S3)"
echo "             B verify ${time_verify}s (one new exec, layers pulled from MinIO)"
echo "files:       $E2E_DIR"
((ok)) || exit 1
