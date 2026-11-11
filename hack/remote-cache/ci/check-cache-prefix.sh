#!/usr/bin/env bash
# Checks the remote cache's engine pin before a job starts a sidecar on it: one
# engine's entries are for that engine, so the writer (remote-cache-update.yml,
# VERSION), the readers (remote-cache-tests.yml, DAGGER_VERSION) and the
# environments' CACHE_S3_PREFIX (cache-<version without its "v1.0.0-">) move
# together. Fails, with an annotation, when they don't.
#
#   check-cache-prefix.sh <version> <prefix>
set -euo pipefail
[[ $# -eq 2 ]] || { sed -n '8p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
version="$1" prefix="$2"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
workflows="$root/.github/workflows"

fail=0
want="cache-${version#*-}"
if [[ "$prefix" != "$want" ]]; then
	echo "::error::CACHE_S3_PREFIX is '$prefix', but engine $version uses '$want': move the environments' prefix with the version"
	fail=1
fi
# The pins, as each workflow sets them in its env.
for pin in remote-cache-update.yml:VERSION remote-cache-tests.yml:DAGGER_VERSION; do
	file="${pin%%:*}" name="${pin#*:}"
	got="$(sed -nE "s/^ *${name}: *(v[0-9][^ ]*) *\$/\1/p" "$workflows/$file")"
	if [[ "$got" != "$version" ]]; then
		echo "::error::$file sets $name to '${got:-nothing}', not $version: the writer and the readers must pin one engine version"
		fail=1
	fi
done
exit "$fail"
