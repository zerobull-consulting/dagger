#!/usr/bin/env bash
# Reads the cache service's engine manifest (cmd/cacheservice/compat/
# engines.txt; ENGINES_FILE overrides it) for CI and the scripts, so that no
# engine pin is copied by hand (zerobull-consulting/dagger#117, D1):
#
#   engines.sh pin [<version>]
#       Prints STOCK_ENGINE (the first accept line's stock engine image,
#       pinned by digest) and STOCK_ENGINE_TAG (its version) as NAME=value
#       lines, for "$GITHUB_ENV" or eval. The first accept line is the
#       engine CI's pull requests, the e2e harness and the demo run. With a
#       version, that version's accept line instead; a version that isn't
#       accepted (unlisted, rejected or malformed) fails.
#
#   engines.sh base
#       Prints the base line's upstream release: what a release image is
#       stamped as (the Dockerfile's RELEASE).
#
#   engines.sh versions
#       Prints the accepted versions, in order, as one JSON array of
#       strings: the release matrix (cacheservice-publish.yml).
#
#   engines.sh candidate <version> <digest>
#       Verifying a version not yet accepted: adds it as the first accept
#       line, verified by this CI run (GITHUB_SERVER_URL, GITHUB_REPOSITORY,
#       GITHUB_RUN_ID), so the run's service accepts it and its jobs run
#       it. The edit is the run's own and is never committed by this
#       script: a green run is what a commit of that line then cites.
#
# Both check the whole manifest as the service's parser does (compat.Parse;
# TestEnginesScriptMatchesParser keeps the two in step), and fail on
# anything it would refuse, more than 100 accept lines included.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
file="${ENGINES_FILE:-$here/../../cmd/cacheservice/compat/engines.txt}"
max_accept=100
version_re='^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+(\.[0-9A-Za-z]+)*)?$'
digest_re='^sha256:[0-9a-f]{64}$'
verified_re='^https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/actions/runs/[0-9]+(/attempts/[0-9]+)?$'
issue_re='^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+#[0-9]+$'

die() {
	echo "engines.sh: $*" >&2
	exit 1
}

# accepts checks the whole manifest as compat.Parse does (each line's
# keyword, fields and their shapes, one base line, no version twice, at
# least one and at most max_accept accept lines), and prints the accept
# lines' "<version> <digest>", in order, then, with an argument "base",
# a last line "base <tag>".
accepts() {
	local want_base="${1:-}" base=""
	local n=0 lineno=0 bases=0 line
	local -a f
	local -A seen=()
	# "|| [[ -n $line ]]": a last line without a newline is a line too, as
	# it is to compat.Parse.
	while IFS= read -r line || [[ -n "$line" ]]; do
		lineno=$((lineno + 1))
		read -r -a f <<<"$line" || true
		((${#f[@]} > 0)) || continue
		[[ "${f[0]}" == \#* ]] && continue
		local where="$file:$lineno"
		case "${f[0]}" in
		base)
			((${#f[@]} == 2)) || die "$where: want 'base <upstream tag>'"
			((bases == 0)) || die "$where: a second base line"
			[[ "${f[1]}" =~ $version_re ]] || die "$where: base '${f[1]}' is not a release version"
			bases=1
			base="${f[1]}"
			;;
		accept)
			((${#f[@]} == 5)) && [[ "${f[3]}" == verified: ]] || die "$where: want 'accept <version> <digest> verified: <CI run URL>'"
			[[ "${f[1]}" =~ $version_re ]] || die "$where: accept version '${f[1]}' is not a release version"
			[[ "${f[2]}" =~ $digest_re ]] || die "$where: accept digest '${f[2]}' of ${f[1]} is not a sha256 digest"
			[[ "${f[4]}" =~ $verified_re ]] || die "$where: '${f[4]}' is not a GitHub Actions run URL"
			n=$((n + 1))
			((n <= max_accept)) || die "$where: more than $max_accept accept lines"
			;;
		reject)
			((${#f[@]} == 3)) || die "$where: want 'reject <version> <owner/repo#issue>'"
			[[ "${f[1]}" =~ $version_re ]] || die "$where: reject version '${f[1]}' is not a release version"
			[[ "${f[2]}" =~ $issue_re ]] || die "$where: '${f[2]}' is not an issue such as zerobull-consulting/dagger#117"
			;;
		*)
			die "$where: unknown line '${f[0]}': want base, accept or reject"
			;;
		esac
		[[ "${f[0]}" == base ]] && continue
		[[ -z "${seen[${f[1]}]:-}" ]] || die "$where: ${f[1]} is on line ${seen[${f[1]}]} already"
		seen[${f[1]}]=$lineno
		if [[ "${f[0]}" == accept ]]; then
			echo "${f[1]} ${f[2]}"
		fi
	done <"$file"
	((bases == 1)) || die "$file: no base line"
	((n > 0)) || die "$file: no accept line"
	if [[ "$want_base" == base ]]; then
		echo "base $base"
	fi
}

case "${1:-}" in
pin)
	[[ $# -le 2 ]] || die "usage: engines.sh pin [<version>]"
	list="$(accepts)"
	if [[ $# -eq 1 ]]; then
		read -r v d <<<"$list"
	else
		want="$2" v="" d=""
		[[ "$want" =~ $version_re ]] || die "'$want' is not a release version such as v1.0.0-beta.25"
		while read -r lv ld; do
			[[ "$lv" == "$want" ]] && { v="$lv" d="$ld"; break; }
		done <<<"$list"
		[[ -n "$v" ]] || die "$want is not accepted by $file: it accepts only $(cut -d' ' -f1 <<<"$list" | paste -sd' ')"
	fi
	echo "STOCK_ENGINE=registry.dagger.io/engine:$v@$d"
	echo "STOCK_ENGINE_TAG=$v"
	;;
base)
	[[ $# -eq 1 ]] || die "usage: engines.sh base"
	list="$(accepts base)"
	tail -n1 <<<"$list" | cut -d' ' -f2
	;;
versions)
	[[ $# -eq 1 ]] || die "usage: engines.sh versions"
	list="$(accepts)"
	# Each version matched version_re, so it needs no JSON escaping.
	cut -d' ' -f1 <<<"$list" | awk 'BEGIN { printf "[" } { printf "%s\"%s\"", (NR > 1 ? "," : ""), $1 } END { print "]" }'
	;;
candidate)
	[[ $# -eq 3 ]] || die "usage: engines.sh candidate <version> <digest>"
	v="$2" d="$3"
	[[ "$v" =~ $version_re ]] || die "candidate version '$v' is not a release version such as v1.0.0-beta.25"
	[[ "$d" =~ $digest_re ]] || die "candidate digest '$d' is not a sha256 digest"
	for var in GITHUB_SERVER_URL GITHUB_REPOSITORY GITHUB_RUN_ID; do
		[[ -n "${!var:-}" ]] || die "$var is not set: a candidate is verified by a CI run"
	done
	# The whole manifest is checked first, and the candidate must fit: a
	# full list is refused before anything is written.
	count="$(accepts | wc -l)"
	((count < max_accept)) || die "$file has $max_accept accept lines already: drop one before adding $v"
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
	die "usage: engines.sh pin [<version>] | base | versions | candidate <version> <digest>"
	;;
esac
