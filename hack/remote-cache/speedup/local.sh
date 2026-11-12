#!/usr/bin/env bash
# Runs the speedup pipelines (run.sh) through a local sidecar, as CI does: a
# writer phase stores what they build in the bucket, then a read-only phase, on
# a fresh engine, fetches it. Each phase reports, per pipeline, how many of its
# withExec steps were cache hits and how many ran, and how long it took: how
# cacheable the pipeline is, against a real bucket.
#
#   local.sh [writer] [read-only]     (default: both, in that order)
#
# The store is the CACHESERVICE_S3_* variables (ENDPOINT, REGION, BUCKET,
# ACCESS_KEY_ID, SECRET_ACCESS_KEY; PREFIX, PATH_STYLE), as for sidecar.sh.
# `hack/remote-cache/dev.sh speedup` runs it against a local MinIO.
#
# Against the PR-checks bucket (hack/remote-cache/spaces), with the read key:
#
#   CACHESERVICE_S3_PREFIX=cache-beta.26 local.sh read-only
#
# reads what the update job (remote-cache-update.yml) stored, as a pull
# request's run does. With the write key, use a prefix of your own: the update
# job owns its prefix, and two writers on one race their exports.
#
#   READONLY_ACCESS_KEY_ID      the read-only phase's key (default: the
#   READONLY_SECRET_ACCESS_KEY  CACHESERVICE_S3_ ones)
#   PIPELINES                   run.sh's pipelines to run (default: build unit)
#   VERSION                     the engine version: an accept line of
#                               cmd/cacheservice/compat/engines.txt (default:
#                               its first)
#   SERVICE_IMAGE               the service image (default: the published one
#                               for VERSION)
#   CA_FILE                     a CA bundle the engine trusts, for a host behind
#                               a TLS-intercepting proxy; the pipelines'
#                               containers get it too (run.sh, SPEEDUP_CA_FILE),
#                               so their keys aren't CI's
#   SPEEDUP_DIR                 where the logs go (default: a new directory
#                               under ${TMPDIR:-/tmp}/remote-cache-speedup)
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
sidecar="$repo/.github/actions/remote-cache/sidecar.sh"

phases=("$@")
((${#phases[@]})) || phases=(writer read-only)
for p in "${phases[@]}"; do
	[[ "$p" == writer || "$p" == read-only ]] || { sed -n '8,8p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
done
read -ra pipelines <<<"${PIPELINES:-build unit}"
for var in CACHESERVICE_S3_ENDPOINT CACHESERVICE_S3_BUCKET CACHESERVICE_S3_ACCESS_KEY_ID CACHESERVICE_S3_SECRET_ACCESS_KEY; do
	[[ -n "${!var:-}" ]] || { echo "local.sh: $var is not set (see the top of this script)" >&2; exit 2; }
done
if [[ -n "${VERSION:-}" ]] && ! SIDECAR_VERSION="$VERSION" "$sidecar" version >/dev/null 2>&1; then
	echo "local.sh: VERSION must be an accept line of cmd/cacheservice/compat/engines.txt; unset it for the first one" >&2
	exit 2
fi
run_id="$(date -u +%Y%m%d-%H%M%S)-$$"
: "${SPEEDUP_DIR:=${TMPDIR:-/tmp}/remote-cache-speedup/$run_id}"
mkdir -p "$SPEEDUP_DIR"
export SIDECAR_VERSION="${VERSION:-}" SIDECAR_SERVICE_IMAGE="${SERVICE_IMAGE:-}"
unset SIDECAR_ENGINE_IMAGE
export SIDECAR_DIR="$SPEEDUP_DIR/sidecar" SIDECAR_VERBOSE=true
export SIDECAR_ENGINE_CA_FILE="${CA_FILE:-}" SPEEDUP_CA_FILE="${CA_FILE:-}"
# An unsampled trace from the caller's shell would keep the service from
# learning any entry.
unset TRACEPARENT

trap 'for n in speedup-writer speedup-read-only; do SIDECAR_NAME=$n "$sidecar" stop >/dev/null 2>&1 || true; done' EXIT
summary=()
for phase in "${phases[@]}"; do
	name="speedup-$phase"
	echo "=== $phase phase (bucket $CACHESERVICE_S3_BUCKET, prefix ${CACHESERVICE_S3_PREFIX:-none}; logs in $SPEEDUP_DIR)"
	export SIDECAR_NAME="$name" DAGGER="$SIDECAR_DIR/$name/bin/dagger"
	if [[ "$phase" == writer ]]; then
		SIDECAR_ROLE=writer "$sidecar" start
	else
		SIDECAR_ROLE=read-only \
			CACHESERVICE_S3_ACCESS_KEY_ID="${READONLY_ACCESS_KEY_ID:-$CACHESERVICE_S3_ACCESS_KEY_ID}" \
			CACHESERVICE_S3_SECRET_ACCESS_KEY="${READONLY_SECRET_ACCESS_KEY:-$CACHESERVICE_S3_SECRET_ACCESS_KEY}" \
			"$sidecar" start
	fi
	executed_total=0
	for p in "${pipelines[@]}"; do
		t0=$SECONDS
		"$here/run.sh" "$p" "$p" "$SPEEDUP_DIR/$phase" "$name-service"
		read -r hit executed <"$SPEEDUP_DIR/$phase/$p.spans"
		executed_total=$((executed_total + executed))
		summary+=("$(printf '%-10s %-6s %4d hit %4d executed %5ds' "$phase" "$p" "$hit" "$executed" $((SECONDS - t0)))")
	done
	# The writer's drain stores the exports: the stop waits for it.
	"$sidecar" stop
	if [[ "$phase" == writer ]] && ((executed_total > 0)); then
		"$repo/hack/remote-cache/demo/check.sh" stored "$SIDECAR_DIR/$name/logs/service.log" "$SPEEDUP_DIR/$phase"
	fi
done
trap - EXIT
echo "=== done (logs in $SPEEDUP_DIR; the sidecars' in $SIDECAR_DIR/*/logs)"
printf '%s\n' "${summary[@]}"
