#!/usr/bin/env bash
# The accept-line-only check (zerobull-consulting/dagger#117, D5, outcome
# A): the release check auto-merges its accept PRs, so such a PR must change
# nothing but one added accept line. Run on every pull request by
# .github/workflows/remote-cache-accept-line.yml, as the "accept line only"
# check:
#
#   accept-line-only.sh check <base sha> <head sha> <head ref>
#       A PR from a release-check/accept/<version> branch passes only if,
#       from its merge base with the base branch, it changes one file,
#       cmd/cacheservice/compat/engines.txt, by one added line and nothing
#       else (no mode change, no rename), and that line is
#       'accept <version> <digest> verified: <CI run URL>' for the branch's
#       own version, the manifest's first accept line, in a manifest
#       engines.sh takes. Every other PR passes: it isn't auto-merged.
#
#   accept-line-only.sh disable-auto-merge <PR number>
#       Turns the PR's auto-merge off if it's on (GH_TOKEN, GITHUB_TOKEN).
#       The workflow runs it when the check fails.
#
# The workflow runs on pull_request_target, with the default branch's copy
# of this script and of engines.sh, so a PR can't change the check that
# measures it.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# The workflow runs copies of the base branch's scripts, from elsewhere.
engines="${ENGINES_SH:-$here/engines.sh}"
manifest=cmd/cacheservice/compat/engines.txt
repo="${GITHUB_REPOSITORY:-zerobull-consulting/dagger}"
version_re='^v1\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+(\.[0-9A-Za-z]+)*)?$'
digest_re='^sha256:[0-9a-f]{64}$'
run_re='^https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/actions/runs/[0-9]+(/attempts/[0-9]+)?$'

fail() {
	echo "::error file=$manifest::accept line only: $*"
	exit 1
}

check() {
	local base="$1" head="$2" ref="$3" v changes numstat summary added a tmp first
	[[ "$base" =~ ^[0-9a-f]{40}$ && "$head" =~ ^[0-9a-f]{40}$ ]] || fail "the base and head must be commit ids"
	if [[ "$ref" != release-check/accept/* ]]; then
		echo "not a release-check accept PR ($ref): nothing to check"
		return
	fi
	v="${ref#release-check/accept/}"
	[[ "$v" =~ $version_re ]] || fail "the branch '$ref' doesn't name a v1 release"
	changes="$(git diff --no-renames --name-status "$base...$head")"
	[[ "$changes" == "M"$'\t'"$manifest" ]] || fail "the PR must change only $manifest; it changes:"$'\n'"$changes"
	numstat="$(git diff --no-renames --numstat "$base...$head")"
	[[ "$numstat" == "1"$'\t'"0"$'\t'"$manifest" ]] || fail "the PR must add one line to $manifest and remove none: $numstat"
	summary="$(git diff --no-renames --summary "$base...$head")"
	[[ -z "$summary" ]] || fail "the PR changes $manifest's mode: $summary"
	added="$(git diff --no-renames -U0 "$base...$head" -- "$manifest" | sed -n 's/^+\([^+]\)/\1/p; s/^+$//p')"
	local -a f
	read -r -a f <<<"$added"
	((${#f[@]} == 5)) && [[ "${f[0]}" == accept && "${f[3]}" == verified: && "$added" == "${f[*]}" ]] ||
		fail "the added line must be 'accept <version> <digest> verified: <CI run URL>', single-spaced: '$added'"
	[[ "${f[1]}" == "$v" ]] || fail "the line accepts ${f[1]}, not the branch's $v"
	[[ "${f[2]}" =~ $digest_re ]] || fail "'${f[2]}' is not a sha256 digest"
	[[ "${f[4]}" =~ $run_re ]] || fail "'${f[4]}' is not a GitHub Actions run URL"
	tmp="$(mktemp)"
	git show "$head:$manifest" >"$tmp"
	first="$(ENGINES_FILE="$tmp" "$engines" pin | sed -n 's/^STOCK_ENGINE_TAG=//p')" || {
		rm -f "$tmp"
		fail "engines.sh refuses the PR's manifest"
	}
	rm -f "$tmp"
	[[ "$first" == "$v" ]] || fail "$v must be the first accept line, so the PR's CI runs it; the first is $first"
	echo "the PR adds only '$added': passes"
}

disable() {
	local pr="$1" info node
	[[ "$pr" =~ ^[0-9]{1,9}$ ]] || { echo "accept-line-only.sh: '$pr' is not a PR number" >&2; exit 1; }
	info="$(timeout 60 gh api "repos/$repo/pulls/$pr")"
	if jq -e '.auto_merge == null' <<<"$info" >/dev/null; then
		echo "auto-merge is off on #$pr"
		return
	fi
	node="$(jq -r .node_id <<<"$info")"
	# shellcheck disable=SC2016 # a GraphQL variable, not shell
	timeout 60 gh api graphql -f query='mutation($id: ID!) { disablePullRequestAutoMerge(input: {pullRequestId: $id}) { clientMutationId } }' \
		-f id="$node" >/dev/null
	echo "::warning::turned auto-merge off on #$pr: it changes more than one accept line"
}

case "${1:-}" in
check) [[ $# -eq 4 ]] || fail "usage: accept-line-only.sh check <base sha> <head sha> <head ref>"; check "$2" "$3" "$4" ;;
disable-auto-merge) [[ $# -eq 2 ]] || exit 2; disable "$2" ;;
*) echo "usage: accept-line-only.sh check <base> <head> <ref> | disable-auto-merge <PR>" >&2; exit 2 ;;
esac
