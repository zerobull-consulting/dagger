#!/usr/bin/env bash
# The "auto-merge: adds compatible engine only" check
# (zerobull-consulting/dagger#117, D5, outcome A): the release check
# auto-merges its accept PRs, so such a PR must change nothing but one added
# accept line. Run on every pull request by
# .github/workflows/remote-cache-auto-merge-add-compatible-engine.yml:
#
#   accept-line-only.sh check <base sha> <head sha> <head ref>
#       A PR from a release-check/accept/<version> branch passes only if,
#       from its merge base with the base branch, it changes one file,
#       cmd/cacheservice/compat/engines.txt, by one added line and nothing
#       else (no mode change, no rename), and that line is
#       'accept <version> <digest> verified: <CI run URL>' for the branch's
#       own version, the manifest's first accept line, in a manifest
#       engines.sh takes, and the cited run is what the line says it is: a
#       completed, successful workflow_dispatch run of remote-cache-tests.yml
#       of this repository, titled for that version and digest, whose
#       "real engines (e2e)" and "sidecar demo" jobs each completed
#       successfully (see verify_run; GH_TOKEN, GITHUB_TOKEN, actions: read).
#       Every other PR passes: it isn't auto-merged.
#
#   accept-line-only.sh fetch-head <PR number> <head sha> <head ref>
#       For a release-check/accept/* PR, fetches refs/pull/<PR>/head of the
#       base repository (where GitHub keeps every PR's head, a fork's too)
#       explicitly, so the head commit's objects are here whether or not any
#       branch holds it, and fails unless the head is that ref's tip or in its
#       history. Every other PR passes untouched. It checks nothing out: check
#       only reads commits (git diff, git show).
#       (zerobull-consulting/dagger#165)
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
# The workflow runs copies of the base branch's scripts, from elsewhere
# (this one, engine-jobs.sh beside it, and engines.sh).
engines="${ENGINES_SH:-$here/engines.sh}"
manifest=cmd/cacheservice/compat/engines.txt
repo="${GITHUB_REPOSITORY:-zerobull-consulting/dagger}"
version_re='^v1\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+(\.[0-9A-Za-z]+)*)?$'
digest_re='^sha256:[0-9a-f]{64}$'
run_re='^https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/actions/runs/[0-9]+(/attempts/[0-9]+)?$'

tests_wf=.github/workflows/remote-cache-tests.yml
# Seconds between tries of a read of the cited run (three tries).
retry_seconds="${ACCEPT_LINE_RETRY_SECONDS:-5}"

# fail <message>: an annotation. The message can hold text the PR or the
# cited run controls, so %, CR and LF are escaped as workflow commands need,
# or a line of it could be a command of its own.
fail() {
	local msg="$*"
	msg="${msg//'%'/%25}"
	msg="${msg//$'\r'/%0D}"
	msg="${msg//$'\n'/%0A}"
	echo "::error file=$manifest::auto-merge: adds compatible engine only: $msg"
	exit 1
}

# read_api <path>: gh api <path>, tried three times (a transient error
# shouldn't fail the check, which turns auto-merge off), each bounded.
read_api() {
	local try out
	for try in 1 2 3; do
		out="$(timeout 60 gh api "$1")" && { printf '%s\n' "$out"; return 0; }
		((try == 3)) || sleep "$retry_seconds"
	done
	return 1
}

# verify_run <version> <digest> <run URL>: the provenance of the line. The
# runs API doesn't return a dispatch's inputs, so remote-cache-tests.yml's
# run-name carries them: 'Remote cache tests: candidate <version> <digest>'
# (display_title). Which branch the run was dispatched on is not checked:
# the release check dispatches on RELEASE_CHECK_BRANCH (default
# feature/remote-cache), on RELEASE_CHECK_TEST_REF in a dry run, and on
# release-check/rebuild/<version> for outcome B, and a hand-added line may
# cite a run from any branch. A writer who edits the run-name on a branch
# can still make a run look like this (zerobull-consulting/dagger#165).
verify_run() {
	local v="$1" d="$2" url="$3" path urlrepo id attempt run api_path got want jobs why n
	path="${url#https://github.com/}"
	# GitHub's names are case-insensitive.
	urlrepo="${path%%/actions/*}"
	[[ "${urlrepo,,}" == "${repo,,}" ]] || fail "the cited run is of $urlrepo, not of $repo"
	path="${path#*/actions/runs/}"
	id="${path%%/*}"
	attempt=""
	[[ "$path" == */attempts/* ]] && attempt="${path##*/}"
	api_path="repos/$repo/actions/runs/$id${attempt:+/attempts/$attempt}"
	run="$(read_api "$api_path")" || fail "couldn't read the cited run $url"
	jq -e . >/dev/null 2>&1 <<<"$run" || fail "the cited run $url didn't read as JSON"
	# One jq per field, so a field the API leaves out reads as empty.
	field() { jq -r "$1 // \"\"" <<<"$run"; }
	got="$(field .repository.full_name)"
	[[ "${got,,}" == "${repo,,}" ]] || fail "the cited run belongs to '$got', not to $repo"
	got="$(field .head_repository.full_name)"
	[[ "${got,,}" == "${repo,,}" ]] || fail "the cited run ran the code of '$got', not of $repo"
	got="$(field .path)"
	[[ "$got" == "$tests_wf" ]] || fail "the cited run is of '$got', not of $tests_wf"
	got="$(field .event)"
	[[ "$got" == workflow_dispatch ]] || fail "the cited run was a '$got' run, not a workflow_dispatch"
	got="$(field .status)/$(field .conclusion)"
	[[ "$got" == completed/success ]] || fail "the cited run is $got, not completed/success"
	want="Remote cache tests: candidate $v $d"
	got="$(field .display_title)"
	[[ "$got" == "$want" ]] || fail "the cited run is titled '$got', not '$want': it didn't verify this version and digest"
	# A run whose engine jobs were skipped still ends green, so the verdict
	# is the engine jobs': each there once, completed, successful
	# (zerobull-consulting/dagger#223). They're read from the attempt the
	# run read above describes (its run_attempt), so a re-run started
	# between the two reads can't swap in another attempt's jobs.
	n="$(field .run_attempt)"
	[[ "$n" =~ ^[1-9][0-9]{0,4}$ ]] || fail "the cited run's attempt is '$n', not a number"
	[[ -z "$attempt" || "$n" == "$attempt" ]] || fail "the cited run's attempt reads as $n, not the URL's $attempt"
	jobs="$(read_api "repos/$repo/actions/runs/$id/attempts/$n/jobs?per_page=100")" || fail "couldn't read the cited run $url's jobs"
	why="$(engine_jobs_unlisted "$jobs")"
	[[ -z "$why" ]] || fail "the cited run $url: $why"
	jq -e --argjson e "$engine_json" '[.jobs[] | select('"$is_engine"')] | all(.status == "completed" and .conclusion == "success")' <<<"$jobs" >/dev/null || {
		got="$(jq -r --argjson e "$engine_json" '[.jobs[] | select('"$is_engine"') | "\(.name): \(.status)/\(.conclusion)"] | join(", ")' <<<"$jobs")"
		fail "the cited run's engine jobs aren't each completed/success: $got"
	}
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
	verify_run "${f[1]}" "${f[2]}" "${f[4]}"
	echo "the PR adds only '$added': passes"
}

# fetch_head <PR> <head sha> <head ref>: see the header. GH_TOKEN, if set,
# reaches git through its environment, never its arguments (the checkout
# persists none).
fetch_head() {
	local pr="$1" head="$2" ref="$3" remote="${FETCH_REMOTE:-origin}" url auth
	[[ "$ref" == release-check/accept/* ]] || { echo "not a release-check accept PR ($ref): nothing to fetch"; return; }
	[[ "$pr" =~ ^[0-9]{1,9}$ ]] || fail "'$pr' is not a PR number"
	[[ "$head" =~ ^[0-9a-f]{40}$ ]] || fail "the head must be a commit id"
	url="$(git remote get-url "$remote")"
	if [[ -n "${GH_TOKEN:-}" && "$url" == https://* ]]; then
		auth="$(printf 'x-access-token:%s' "$GH_TOKEN" | base64 -w0)"
		export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0="http.${url%/*/*}/.extraheader" GIT_CONFIG_VALUE_0="AUTHORIZATION: basic $auth"
	fi
	timeout 300 git fetch -q --no-tags "$remote" "+refs/pull/$pr/head:refs/remotes/pull/$pr" ||
		fail "fetching refs/pull/$pr/head failed"
	# The ref has moved on if the PR got a newer push, and its history still
	# holds this head; a push that rewrote it doesn't, and then a newer run
	# measures the newer head.
	git merge-base --is-ancestor "$head" "refs/remotes/pull/$pr" 2>/dev/null ||
		fail "refs/pull/$pr/head is $(git rev-parse --short "refs/remotes/pull/$pr") and doesn't hold $head: the PR was pushed again"
	echo "fetched $head from refs/pull/$pr/head"
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
check)
	[[ $# -eq 4 ]] || fail "usage: accept-line-only.sh check <base sha> <head sha> <head ref>"
	# The engine jobs (engine_jobs, engine_json, is_engine,
	# engine_jobs_unlisted), shared with release-check.sh; only check needs
	# them, so fetch-head and disable-auto-merge don't depend on the file.
	# The workflow copies it next to this script.
	# shellcheck source=engine-jobs.sh
	source "$here/engine-jobs.sh"
	check "$2" "$3" "$4"
	;;
fetch-head) [[ $# -eq 4 ]] || fail "usage: accept-line-only.sh fetch-head <PR> <head sha> <head ref>"; fetch_head "$2" "$3" "$4" ;;
disable-auto-merge) [[ $# -eq 2 ]] || exit 2; disable "$2" ;;
*) echo "usage: accept-line-only.sh check <base> <head> <ref> | fetch-head <PR> <head> <ref> | disable-auto-merge <PR>" >&2; exit 2 ;;
esac
