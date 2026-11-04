#!/usr/bin/env bash
# Smoke test of the cache service's image (cmd/cacheservice/Dockerfile).
#
#   image-smoke.sh <image>
#
# Checks, with no bucket and no secrets:
#   1. `-h` exits 0 and prints the flags;
#   2. started with -metrics-listen, it serves /metrics;
#   3. `docker stop` delivers SIGTERM to the binary as PID 1, and it exits 0
#      within STOP_LIMIT seconds (default 5): that is the stop's own timeout,
#      after which docker sends SIGKILL and the exit code is 137.
#
# Needs only docker, curl, grep and bash (no GNU date or bc).
# Used by .github/workflows/cacheservice-image.yml; runs the same locally.
set -euo pipefail

image="${1:?usage: image-smoke.sh <image>}"
: "${STOP_LIMIT:=5}"
name="cacheservice-smoke-$$"

fail() {
	echo "FAIL: $*" >&2
	if docker container inspect "$name" >/dev/null 2>&1; then
		echo "--- container logs ---" >&2
		docker logs "$name" >&2 || true
	fi
	exit 1
}

cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "== 1. -h exits 0"
if ! help="$(docker run --rm "$image" -h 2>&1)"; then
	echo "$help" >&2
	fail "-h exited non-zero"
fi
grep -q -- "-drain-timeout" <<<"$help" || { echo "$help" >&2; fail "-h didn't print the flags"; }
echo "ok"

echo "== 2. /metrics with no bucket"
docker run -d --name "$name" -p 127.0.0.1::9090 "$image" -metrics-listen :9090 >/dev/null ||
	fail "docker run failed"
port="$(docker port "$name" 9090/tcp | head -n1)"
[[ -n "$port" ]] || fail "no published metrics port"
metrics=""
for _ in $(seq 1 50); do
	if metrics="$(curl -fsS "http://$port/metrics" 2>/dev/null)"; then
		break
	fi
	[[ "$(docker inspect -f '{{.State.Running}}' "$name")" == true ]] || fail "the container exited"
	sleep 0.2
done
grep -q '^cacheservice_draining 0$' <<<"$metrics" || fail "/metrics at $port didn't serve cacheservice_draining"
echo "ok: $(grep -c '^cacheservice_' <<<"$metrics") cacheservice_ samples"

echo "== 3. docker stop: SIGTERM to PID 1, exit 0 within ${STOP_LIMIT}s"
docker stop --time "$STOP_LIMIT" "$name" >/dev/null
code="$(docker inspect -f '{{.State.ExitCode}}' "$name")"
logs="$(docker logs "$name" 2>&1)"
[[ "$code" == 0 ]] || fail "exit code $code, not 0 (137: still running after ${STOP_LIMIT}s, so docker sent SIGKILL)"
grep -q 'msg=stopping signal=terminated' <<<"$logs" || fail "the binary didn't log that it got SIGTERM"
echo "ok: exit 0 on SIGTERM"

echo "PASS: $image"
