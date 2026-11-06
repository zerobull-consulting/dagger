#!/usr/bin/env bash
# Smoke test of the remote cache engine image (hack/remote-cache/engine.Dockerfile).
#
#   engine-image-smoke.sh <engine image> [<cacheservice image>]
#
# Checks, with no bucket and no secrets:
#   1. the image's CLI reports this repo's version, and with REVISION set, its
#      commit; with RUNNER_HOST set, that it provisions that engine by default;
#   2. the engine, started next to the cache service (no engine tokens, no
#      store) with _EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL, connects to it: the
#      service's debug API lists it, at this repo's version. The debug API
#      is published, so it takes a token generated for the run, when the
#      service image knows -debug-token-file (zerobull-consulting/dagger#60);
#      an older image's is open;
#   3. the engine runs a container (alpine, through the release image's
#      runtime and CNI), so the binaries copied over the release work with the
#      rest of it.
#
# The cache service image defaults to the published moving tag, the one
# engines are run against. CA_FILE, if set, replaces the engine's CA bundle,
# for a host whose registry traffic goes through a TLS proxy.
#
# Needs docker, curl, grep and bash. Used by
# .github/workflows/engine-image.yml; runs the same locally.
set -euo pipefail

image="${1:?usage: engine-image-smoke.sh <engine image> [<cacheservice image>]}"
service_image="${2:-ghcr.io/zerobull-consulting/cacheservice:feature-remote-cache}"
: "${CONNECT_LIMIT:=60}"
: "${QUERY_LIMIT:=300}"
run="engine-smoke-$$"
net="$run"
engine="$run-engine"
service="$run-cacheservice"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
want_version="$(tr -d '[:space:]' <"$repo_root/internal/version/VERSION")"

fail() {
	echo "FAIL: $*" >&2
	for c in "$service" "$engine"; do
		if docker container inspect "$c" >/dev/null 2>&1; then
			echo "--- $c logs (last 50 lines) ---" >&2
			docker logs --tail 50 "$c" >&2 || true
		fi
	done
	exit 1
}

secrets="$(mktemp -d)"
cleanup() {
	docker rm -f "$engine" "$service" >/dev/null 2>&1 || true
	docker network rm "$net" >/dev/null 2>&1 || true
	rm -rf "$secrets"
}
trap cleanup EXIT

echo "== 1. the CLI's version, commit and default engine"
# The image sets _EXPERIMENTAL_DAGGER_RUNNER_HOST to the engine's own socket;
# without it, the CLI prints the engine it would provision.
out="$(docker run --rm --entrypoint sh "$image" -c 'unset _EXPERIMENTAL_DAGGER_RUNNER_HOST; dagger version')" ||
	fail "dagger version exited non-zero"
echo "$out"
grep -qx "version: *$want_version" <<<"$out" || fail "version isn't $want_version"
if [[ -n "${REVISION:-}" ]]; then
	grep -qx "commit: *${REVISION:0:8}" <<<"$out" || fail "commit isn't ${REVISION:0:8}"
fi
if [[ -n "${RUNNER_HOST:-}" ]]; then
	grep -qxF "runner-host: $RUNNER_HOST" <<<"$out" || fail "runner-host isn't $RUNNER_HOST"
fi
echo "ok"

echo "== 2. the engine connects to the cache service"
docker network create "$net" >/dev/null
# The debug API on the container's port, published to the host's loopback
# only. :8091 isn't loopback in the container, so an image that knows
# -debug-token-file requires one: a token for this run, in a file mounted
# read-only (readable by the image's UID, 65532), never on a command line.
token_args=() auth=()
if docker run --rm "$service_image" -h 2>&1 | grep -q -- -debug-token-file; then
	chmod 700 "$secrets"
	(umask 022 && od -An -N32 -tx1 /dev/urandom | tr -d ' \n' >"$secrets/debug-token")
	token_args=(-v "$secrets/debug-token:/run/secrets/debug-token:ro")
	auth=(-H "Authorization: Bearer $(cat "$secrets/debug-token")")
fi
docker run -d --name "$service" --network "$net" -p 127.0.0.1::8091 "${token_args[@]}" \
	"$service_image" -debug-listen :8091 ${token_args:+-debug-token-file /run/secrets/debug-token} >/dev/null ||
	fail "cache service: docker run failed"
debug="$(docker port "$service" 8091/tcp | head -n1)"
[[ -n "$debug" ]] || fail "no published debug port"
ca_mount=()
if [[ -n "${CA_FILE:-}" ]]; then
	ca_mount=(-v "$CA_FILE:/etc/ssl/certs/ca-certificates.crt:ro")
fi
docker run -d --name "$engine" --privileged --network "$net" "${ca_mount[@]}" \
	-e _EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL="http://$service:8090" \
	-e DAGGER_CLOUD_URL="http://$service:8090" \
	-e DAGGER_CLOUD_TOKEN=engine-smoke \
	"$image" >/dev/null || fail "engine: docker run failed"
engines=""
for _ in $(seq 1 "$CONNECT_LIMIT"); do
	engines="$(curl -fsS "${auth[@]}" "http://$debug/debug/engines" 2>/dev/null || true)"
	grep -q "\"engineVersion\": *\"$want_version\"" <<<"$engines" && break
	[[ "$(docker inspect -f '{{.State.Running}}' "$engine")" == true ]] || fail "the engine exited"
	sleep 1
done
grep -q "\"engineVersion\": *\"$want_version\"" <<<"$engines" ||
	fail "the service didn't list an engine at $want_version within ${CONNECT_LIMIT}s: $engines"
echo "ok: $(grep -o '"engineName": *"[^"]*"' <<<"$engines" | head -n1)"

echo "== 3. the engine runs a container"
# The CLI runs in the engine's container, on its socket (set here, not left
# to the image's ENV, since the CLI's compiled-in default is the image's
# registry tag). The Cloud variables
# are the engine's: they are unset for the CLI, which can't resolve the
# service's name from there, and isn't what this checks.
query='{ container { from(address: "alpine:3.22") { withExec(args: ["echo", "engine-smoke-ok"]) { stdout } } } }'
if ! out="$(timeout "$QUERY_LIMIT" docker exec -i \
	-e _EXPERIMENTAL_DAGGER_RUNNER_HOST=unix:///run/dagger/engine.sock \
	-e DAGGER_CLOUD_URL= -e DAGGER_CLOUD_TOKEN= -e DAGGER_NO_NAG=1 \
	"$engine" dagger api query <<<"$query" 2>&1)"; then
	echo "$out" >&2
	fail "the query failed or took over ${QUERY_LIMIT}s"
fi
grep -q 'engine-smoke-ok' <<<"$out" || { echo "$out" >&2; fail "the exec's output is missing"; }
echo "ok"

echo "PASS: $image"
