#!/usr/bin/env bash
# Reads the cache service's engine manifest (cmd/cacheservice/compat/
# engines.txt; ENGINES_FILE overrides it) for CI and the scripts, so that no
# engine pin is copied by hand (zerobull-consulting/dagger#117, D1):
#
#   engines.sh pin
#       Prints STOCK_ENGINE (the first accept line's stock engine image,
#       pinned by digest) and STOCK_ENGINE_TAG (its version) as NAME=value
#       lines, for "$GITHUB_ENV" or eval. The first accept line is the
#       engine CI's pull requests, the e2e harness and the demo run.
#
#   engines.sh candidate <version> <digest>
#       Verifying a version not yet accepted: adds it as the first accept
#       line, verified by this CI run (GITHUB_SERVER_URL, GITHUB_REPOSITORY,
#       GITHUB_RUN_ID), so the run's service accepts it and its jobs run
#       it. The edit is the run's own and is never committed by this
#       script: a green run is what a commit of that line then cites.
#
# Both fail on more than 100 accept lines, and on a line the service's
# parser (compat.Parse, which TestEnginesScriptMatchesParser keeps this in
# step with) would refuse in the fields they read.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
file="${ENGINES_FILE:-$here/../../cmd/cacheservice/compat/engines.txt}"
max_accept=100
version_re='^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+(\.[0-9A-Za-z]+)*)?$'
digest_re='^sha256:[0-9a-f]{64}$'

die() {
	echo "engines.sh: $*" >&2
	exit 1
}

# accepts prints the accept lines' "<version> <digest>", in order.
accepts() {
	local n=0 kw v d rest
	# "|| [[ -n $kw ]]": a last line without a newline is a line too, as it
	# is to compat.Parse.
	while read -r kw v d rest || [[ -n "$kw" ]]; do
		[[ "$kw" == accept ]] || continue
		n=$((n + 1))
		((n <= max_accept)) || die "$file: more than $max_accept accept lines"
		[[ "$v" =~ $version_re ]] || die "$file: accept version '$v' is not a release version"
		[[ "$d" =~ $digest_re ]] || die "$file: accept digest '$d' of $v is not a sha256 digest"
		echo "$v $d"
	done <"$file"
	((n > 0)) || die "$file: no accept line"
}

case "${1:-}" in
pin)
	[[ $# -eq 1 ]] || die "usage: engines.sh pin"
	list="$(accepts)"
	read -r v d <<<"$list"
	echo "STOCK_ENGINE=registry.dagger.io/engine:$v@$d"
	echo "STOCK_ENGINE_TAG=$v"
	;;
candidate)
	[[ $# -eq 3 ]] || die "usage: engines.sh candidate <version> <digest>"
	v="$2" d="$3"
	[[ "$v" =~ $version_re ]] || die "candidate version '$v' is not a release version such as v1.0.0-beta.25"
	[[ "$d" =~ $digest_re ]] || die "candidate digest '$d' is not a sha256 digest"
	for var in GITHUB_SERVER_URL GITHUB_REPOSITORY GITHUB_RUN_ID; do
		[[ -n "${!var:-}" ]] || die "$var is not set: a candidate is verified by a CI run"
	done
	accepts >/dev/null
	if awk -v v="$v" '($1 == "accept" || $1 == "reject") && $2 == v { found = 1 } END { exit !found }' "$file"; then
		die "$v is in $file already"
	fi
	run="$GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID"
	tmp="$(mktemp)"
	awk -v line="accept $v $d verified: $run" '!done && $1 == "accept" { print line; done = 1 } { print }' "$file" >"$tmp"
	cat "$tmp" >"$file"
	rm -f "$tmp"
	echo "::warning::verifying candidate engine $v ($d): this run's service accepts it as its first accept line; commit 'accept $v $d verified: $run' once the run is green"
	;;
*)
	die "usage: engines.sh pin | engines.sh candidate <version> <digest>"
	;;
esac
