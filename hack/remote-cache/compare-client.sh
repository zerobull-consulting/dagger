#!/usr/bin/env bash
# Compares this checkout's engine side of the remote cache with the stock
# engine release CI runs (STOCK_ENGINE, STOCK_ENGINE_TAG: the workflow's
# top-level env), for the real-engines e2e and sidecar demo jobs.
#
#   compare-client.sh <job>   <job> names what runs the release, for the
#                             warning ("the e2e", "the sidecar demo")
#
# A warning, not a failure: when this branch's engine side of the remote
# cache (merged from upstream main) differs from the stock engine's, those
# jobs don't test it, and STOCK_ENGINE should move to the release that has
# it once there is one. Only a pin that contradicts itself fails: a
# STOCK_ENGINE_TAG that isn't STOCK_ENGINE's own tag, or a digest that isn't
# the one the registry gives for that tag (the jobs run the digest, and this
# compares the code of the tag). A registry or upstream that can't be
# reached, or a compare that fails, is a warning, so a required check never
# fails on it. The lookup and the fetch are bounded (2 and 5 minutes), and
# the calling step bounds the whole.
set -euo pipefail

job="${1:?usage: compare-client.sh <job>}"
: "${STOCK_ENGINE:?STOCK_ENGINE is not set}" "${STOCK_ENGINE_TAG:?STOCK_ENGINE_TAG is not set}"

ref="${STOCK_ENGINE%@*}"
if [[ "${ref##*:}" != "$STOCK_ENGINE_TAG" ]]; then
  echo "::error::STOCK_ENGINE_TAG ($STOCK_ENGINE_TAG) isn't STOCK_ENGINE's tag (${ref##*:}); update them together"
  exit 1
fi
if digest="$(timeout 120 docker buildx imagetools inspect --format '{{.Manifest.Digest}}' "$ref")"; then
  if [[ "$digest" != "${STOCK_ENGINE##*@}" ]]; then
    echo "::error::$ref is $digest in the registry, not STOCK_ENGINE's ${STOCK_ENGINE##*@}; update the tag and the digest together"
    exit 1
  fi
else
  echo "::warning::couldn't resolve $ref in the registry; STOCK_ENGINE's digest wasn't checked against it"
fi
if ! timeout 300 git fetch -q --depth=1 https://github.com/dagger/dagger "refs/tags/$STOCK_ENGINE_TAG:refs/tags/$STOCK_ENGINE_TAG"; then
  echo "::warning::couldn't fetch $STOCK_ENGINE_TAG from dagger/dagger; the engine's client wasn't compared"
  exit 0
fi
# The client, its adapter in the engine, and the bundles it moves.
paths=(
  engine/remotecache cmd/engine/remotecache.go 'engine/server/remote_cache*.go'
  dagql/cache_transfer_types.go dagql/cache_value_capture.go dagql/cache_value_import.go dagql/cache_stored_bundle.go
  ':!**/*_test.go'
)
# Ignored: the fork's export of the bundle version
# (zerobull-consulting/dagger#116), and blank lines. The blank-line
# pattern can't match an empty string: one that could would ignore
# every change.
ignore='ValueBundleVersion|store of bundles checks it|^[[:space:]]+$'
rc=0
git diff --quiet -I"$ignore" "$STOCK_ENGINE_TAG" HEAD -- "${paths[@]}" || rc=$?
case "$rc" in
0) echo "the engine's side of the remote cache is $STOCK_ENGINE_TAG's" ;;
1)
  echo "::warning::the engine's side of the remote cache differs from $STOCK_ENGINE_TAG's, which $job runs:"
  git diff --stat -I"$ignore" "$STOCK_ENGINE_TAG" HEAD -- "${paths[@]}" || true
  ;;
*) echo "::warning::couldn't compare the engine's client with $STOCK_ENGINE_TAG's (git diff exited $rc)" ;;
esac
