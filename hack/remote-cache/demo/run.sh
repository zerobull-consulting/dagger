#!/usr/bin/env bash
# Runs the demo locally, as CI's sidecar job does, against the compose MinIO:
#
#   1. writer phase: a writer sidecar (.github/actions/remote-cache-sidecar/
#      sidecar.sh), the demo, then the stop, which drains;
#   2. read-only phase: a fresh engine and a read-only sidecar (-export=false,
#      with MinIO's read-only user), the demo again.
#
# and checks each phase (check.sh). Run it with `hack/remote-cache/dev.sh
# demo`, which starts MinIO where containers reach it. See README.md in this
# directory.
#
# VERSION is the engine version (default: the first accept line of the
# service's engine manifest, cmd/cacheservice/compat/engines.txt, the one
# CI's sidecar job runs). It selects both images, as the action's version
# input does: the stock engine of that version, pinned by the manifest's
# digest, and the service image that accepts it,
# ghcr.io/zerobull-consulting/dagger-remote-cache:<version>; so the two
# can't disagree. SERVICE_IMAGE overrides the service image (such as one
# built from this checkout), and the start fails if it refuses the engine.
# DEMO_PREFIX is the bucket prefix (default: a new one under demo/), and
# DEMO_DIR where the logs go. CA_FILE, for a host behind a TLS-intercepting
# proxy, is given to the engine.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
sidecar="$repo/.github/actions/remote-cache-sidecar/sidecar.sh"

# One version picks both images (zerobull-consulting/dagger#147, item 2).
[[ -z "${ENGINE_IMAGE:-}" ]] ||
	{ echo "run.sh: ENGINE_IMAGE is gone: set VERSION, which selects the engine and the service that accepts it" >&2; exit 2; }
# VERSION is a common name: say which one this is when it isn't accepted.
# The sidecar reads this checkout's manifest, never ENGINES_FILE: so does
# this check.
if [[ -n "${VERSION:-}" ]] && ! env -u ENGINES_FILE "$here/../engines.sh" pin "$VERSION" >/dev/null; then
	echo "run.sh: VERSION is the demo's engine version, an accept line of cmd/cacheservice/compat/engines.txt; unset it for the first one" >&2
	exit 2
fi
run_id="$(date -u +%Y%m%d-%H%M%S)-$$"
: "${DEMO_PREFIX:=demo/$run_id}"
: "${DEMO_DIR:=${TMPDIR:-/tmp}/remote-cache-demo/$run_id}"
[[ "$DEMO_PREFIX" =~ ^demo/[A-Za-z0-9._-]+$ ]] ||
	{ echo "run.sh: DEMO_PREFIX must be demo/<name>, under the read-only user's prefix: '$DEMO_PREFIX'" >&2; exit 2; }
for var in CACHESERVICE_S3_ENDPOINT CACHESERVICE_S3_BUCKET CACHESERVICE_S3_ACCESS_KEY_ID CACHESERVICE_S3_SECRET_ACCESS_KEY \
	DEMO_READONLY_ACCESS_KEY_ID DEMO_READONLY_SECRET_ACCESS_KEY; do
	[[ -n "${!var:-}" ]] || { echo "run.sh: $var is not set: run it with hack/remote-cache/dev.sh demo" >&2; exit 2; }
done
mkdir -p "$DEMO_DIR"

export SIDECAR_VERSION="${VERSION:-}" SIDECAR_SERVICE_IMAGE="${SERVICE_IMAGE:-}"
unset SIDECAR_ENGINE_IMAGE
export SIDECAR_DIR="$DEMO_DIR/sidecar" SIDECAR_VERBOSE=true
export SIDECAR_ENGINE_CA_FILE="${CA_FILE:-}"
export CACHESERVICE_S3_PREFIX="$DEMO_PREFIX"

# Whatever fails, stop what is running.
trap 'for n in demo-writer demo-reader; do SIDECAR_NAME=$n "$sidecar" stop >/dev/null 2>&1 || true; done' EXIT

echo "=== writer phase (prefix $DEMO_PREFIX; logs in $DEMO_DIR)"
export SIDECAR_NAME=demo-writer DAGGER="$SIDECAR_DIR/demo-writer/bin/dagger"
SIDECAR_ROLE=writer "$sidecar" start
"$here/check.sh" writer demo-writer-service "$DEMO_DIR/writer"
"$sidecar" stop
"$here/check.sh" stored "$SIDECAR_DIR/demo-writer/logs/service.log" "$DEMO_DIR/writer"

echo "=== read-only phase: a fresh engine, -export=false, the read-only key"
export SIDECAR_NAME=demo-reader DAGGER="$SIDECAR_DIR/demo-reader/bin/dagger"
SIDECAR_ROLE=read-only \
	CACHESERVICE_S3_ACCESS_KEY_ID="$DEMO_READONLY_ACCESS_KEY_ID" \
	CACHESERVICE_S3_SECRET_ACCESS_KEY="$DEMO_READONLY_SECRET_ACCESS_KEY" \
	"$sidecar" start
"$here/check.sh" read-only demo-reader-service "$DEMO_DIR/read-only" "$DEMO_DIR/writer"
"$sidecar" stop
trap - EXIT
echo "=== PASS: the demo ran through both phases (logs in $DEMO_DIR; the sidecars' in $SIDECAR_DIR/*/logs)"
