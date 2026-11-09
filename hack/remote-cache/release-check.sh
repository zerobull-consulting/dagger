#!/usr/bin/env bash
# The release check (zerobull-consulting/dagger#117, D5): verifies each new
# upstream Dagger release against the cache service, and ends it as one of
#
#   A  passes as-is: a PR adding its accept line, which auto-merges once
#      the "auto-merge: adds compatible engine only" check is required
#      (RELEASE_CHECK_AUTO_MERGE);
#   B  passes after rebuilding on the new tag: a PR merging the tag, moving
#      the base line and adding the accept line, for a human to merge with
#      a merge commit;
#   C  fails: an issue labeled compat-break, and a PR adding a reject line
#      that names it.
#
# .github/workflows/remote-cache-release-check.yml runs it hourly and by
# hand, and only decides when and with which token; everything else is
# here. Each subcommand is one job's work:
#
#   release-check.sh [--dry-run] publish
#       If the branch's head has no successful publish run and none
#       queued or in progress (an auto-merge by GITHUB_TOKEN starts no
#       workflows; a publish may fail), dispatches cacheservice-publish.yml
#       on it, which moves the floating :<engine version> tags. At most two
#       dispatches per head (counting its workflow_dispatch runs, a hand
#       re-run too): past that it prints publish=bounded and a warning and
#       dispatches nothing. Skips a head with no run at all younger than 10
#       minutes, whose push run may not be listed yet.
#   release-check.sh [--dry-run] plan [<version>]
#       Finds the oldest upstream v1 release newer than every version the
#       manifest names, and prints what to do with it (below). With a
#       version (dry run only), that release, listed or not.
#   release-check.sh [--dry-run] verify <version> <digest>
#       Runs the current service against that stock engine: dispatches
#       remote-cache-tests.yml on the branch (RELEASE_CHECK_TEST_REF: a dry
#       run's own ref) with the candidate inputs, writes the advisory diff
#       while it runs, and waits for it. Prints result=pass or
#       result=fail: the verdict of the "real engines (e2e)" and "sidecar demo"
#       jobs alone, read by name from the run's jobs. A run that doesn't end
#       in success or failure (cancelled, timed out waiting), that failed
#       before testing (its checkout or pin step), in which any other job
#       (unit tests, MinIO) failed, or in which an engine job is missing or
#       skipped, is an error, never a failure: it must not lead to a reject
#       line.
#   release-check.sh bundle <version>
#       TestBundleVersionMatchesEngineCapture with the tag merged into the
#       branch (RELEASE_CHECK_TEST_REF: a dry run's own ref, as verify's).
#       It runs upstream's code, so the workflow runs it in a job of its own
#       with no credential, and it refuses to run if a token is in its
#       environment. Prints bundle=pass, fail or conflict (not run: the tag
#       conflicts with the branch), and merge=. A needs verify's pass and
#       this pass; the outcome commands read this verdict from
#       RELEASE_CHECK_BUNDLE.
#   release-check.sh [--dry-run] accept <version> <digest> <run URL>
#       Outcome A: pushes release-check/accept/<version> with the accept
#       line, and opens its PR.
#   release-check.sh [--dry-run] auto-merge <PR number>
#       Enables squash auto-merge on an accept PR, with GITHUB_TOKEN, only
#       if RELEASE_CHECK_AUTO_MERGE is true and the PR changes nothing but
#       one added accept line.
#   release-check.sh [--dry-run] rebuild <version> <digest>
#       Outcome B, tried after A fails: pushes release-check/rebuild/
#       <version> (the tag merged, the base moved), verifies it as verify
#       does, and if it passes adds the accept line (or resets the list to
#       it, if the bundle or protocol version changed) and opens its PR.
#       Prints result=pass, fail or skipped (the tag is already merged, so
#       a rebuild is the code A ran; the merge conflicts; or the bundle
#       test of that same merge didn't pass).
#   release-check.sh [--dry-run] reject <version> [<issue number>]
#       Outcome C: files (or finds) the compat-break issue with the failing
#       jobs and the advisory diff, then pushes release-check/reject/
#       <version> with the reject line and opens its PR.
#
# plan prints action=, one of:
#   none            nothing new, or the oldest new release has no image
#                   yet (nothing newer is tried before it has)
#   verify          version= and digest=: a new release to verify
#   pending         kind= (accept, rebuild, reject) and pr=: the release's
#                   PR is open; nothing else happens until it's merged or
#                   closed, so releases are handled one at a time, in order
#   resume-reject   version= and issue=: its compat-break issue is open
#                   but its reject branch doesn't exist (a run that
#                   stopped between the two)
# and fails if a release-check branch for the release exists with no open
# PR (a PR closed by hand, or a run that died): delete the branch to retry.
#
# With --dry-run (or RELEASE_CHECK_DRY_RUN=1), nothing is pushed, opened,
# labeled, enabled or dispatched, except verify's own dispatch, which
# changes nothing: each of those is printed instead, starting "dry run:".
#
# Tokens: GH_TOKEN is the workflow's GITHUB_TOKEN, for reads, dispatches,
# issues and auto-merge. BOT_TOKEN is the bot's token (CACHE_BOT_PR_TOKEN or
# CACHE_BOT_SYNC_TOKEN), only for pushes and opening PRs, whose CI must run.
# Neither is ever printed or put on a command line.
#
# Everything from upstream or a registry is checked against a strict
# pattern before it reaches a branch name, a file or a command. Every
# network call is bounded. Settings (for tests): RELEASE_CHECK_REPO,
# RELEASE_CHECK_BRANCH, RELEASE_CHECK_REMOTE, RELEASE_CHECK_UPSTREAM,
# RELEASE_CHECK_DIR (notes passed between jobs), RELEASE_CHECK_POLL_SECONDS,
# RELEASE_CHECK_WAIT_MINUTES, RELEASE_CHECK_MIN_HEAD_AGE.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
engines="$here/engines.sh"
manifest_path=cmd/cacheservice/compat/engines.txt

repo="${RELEASE_CHECK_REPO:-${GITHUB_REPOSITORY:-zerobull-consulting/dagger}}"
branch="${RELEASE_CHECK_BRANCH:-feature/remote-cache}"
server="${GITHUB_SERVER_URL:-https://github.com}"
remote="${RELEASE_CHECK_REMOTE:-$server/$repo.git}"
upstream="${RELEASE_CHECK_UPSTREAM:-https://github.com/dagger/dagger}"
engine_repo=registry.dagger.io/engine
notes="${RELEASE_CHECK_DIR:-${RUNNER_TEMP:-/tmp}/release-check}"
poll="${RELEASE_CHECK_POLL_SECONDS:-60}"
wait_minutes="${RELEASE_CHECK_WAIT_MINUTES:-150}"
min_head_age="${RELEASE_CHECK_MIN_HEAD_AGE:-600}"
dry="${RELEASE_CHECK_DRY_RUN:-}"
auto_merge="${RELEASE_CHECK_AUTO_MERGE:-}"
bot_name="zerobull release check"
bot_email="release-check@users.noreply.github.com"

# The release tags the check considers (zerobull-consulting/dagger#117
# step 3), and what engines.sh takes as a version: a tag must match both.
tag_re='^v1\.[0-9]+\.[0-9]+(-[A-Za-z0-9.]+)?$'
version_re='^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+(\.[0-9A-Za-z]+)*)?$'
digest_re='^sha256:[0-9a-f]{64}$'
repo_re='^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$'
run_re='^https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/actions/runs/[0-9]+(/attempts/[0-9]+)?$'

die() {
	echo "::error::release-check.sh: $*" >&2
	exit 1
}
say() { echo "$*" >&2; }
would() { echo "dry run: would $*" >&2; }
is_dry() { [[ "$dry" == 1 || "$dry" == true ]]; }
# out <name> <value>: a result, on stdout and in the step's outputs.
out() {
	[[ "$2" != *$'\n'* ]] || die "output $1 has a newline"
	echo "$1=$2"
	if [[ -n "${GITHUB_OUTPUT:-}" ]]; then echo "$1=$2" >>"$GITHUB_OUTPUT"; fi
}
valid_tag() { [[ "$1" =~ $tag_re && "$1" =~ $version_re ]]; }
need_tag() { valid_tag "$1" || die "'$1' is not a v1 release tag such as v1.0.0-beta.26"; }
need_digest() { [[ "$1" =~ $digest_re ]] || die "'$1' is not a sha256 digest"; }
need_run() { [[ "$1" =~ $run_re ]] || die "'$1' is not a GitHub Actions run URL"; }
need_number() { [[ "$1" =~ ^[0-9]{1,9}$ ]] || die "'$1' is not a number"; }
# need_test_ref: RELEASE_CHECK_TEST_REF, if set, is a branch name git accepts
# (so no ':', '~', '^', '?', '*', '[', '\\', space or '..'), as the workflow
# gives it. It is only ever used after "refs/heads/", in quoted arguments, and
# only by verify and bundle.
need_test_ref() {
	[[ -z "${RELEASE_CHECK_TEST_REF:-}" ]] || git check-ref-format "refs/heads/$RELEASE_CHECK_TEST_REF" ||
		die "RELEASE_CHECK_TEST_REF is not a branch name"
}

[[ "$repo" =~ $repo_re ]] || die "RELEASE_CHECK_REPO '$repo' is not owner/name"
[[ "$poll" =~ ^[0-9]{1,4}$ && "$wait_minutes" =~ ^[0-9]{1,4}$ && "$min_head_age" =~ ^[0-9]{1,6}$ ]] ||
	die "RELEASE_CHECK_POLL_SECONDS, RELEASE_CHECK_WAIT_MINUTES and RELEASE_CHECK_MIN_HEAD_AGE must be numbers"

# api [gh api args...]: a read, or a write with GITHUB_TOKEN, bounded.
api() { timeout 60 gh api "$@"; }
# bot_api [gh api args...]: as the bot (BOT_TOKEN), so the PR's CI runs.
bot_api() {
	[[ -n "${BOT_TOKEN:-}" ]] || die "BOT_TOKEN is not set: the bot's token is missing or expired; nothing was opened"
	GH_TOKEN="$BOT_TOKEN" timeout 60 gh api "$@"
}

# vsort: sorts versions (stdin) ascending, by semver precedence: a release
# after its pre-releases, numeric identifiers numerically
# (v1.0.0-beta.9 < v1.0.0-beta.25 < v1.0.0).
vsort() {
	awk '
	function cmp(a, b,   ac, bc, ap, bp, x, y, n, m, i) {
		sub(/^v/, "", a); sub(/^v/, "", b)
		ap = ""; bp = ""
		if (index(a, "-")) { ap = substr(a, index(a, "-") + 1); a = substr(a, 1, index(a, "-") - 1) }
		if (index(b, "-")) { bp = substr(b, index(b, "-") + 1); b = substr(b, 1, index(b, "-") - 1) }
		split(a, ac, "."); split(b, bc, ".")
		for (i = 1; i <= 3; i++) if (ac[i] + 0 != bc[i] + 0) return ac[i] + 0 < bc[i] + 0 ? -1 : 1
		if (ap == bp) return 0
		if (ap == "") return 1
		if (bp == "") return -1
		n = split(ap, x, "."); m = split(bp, y, ".")
		for (i = 1; i <= n && i <= m; i++) {
			if (x[i] == y[i]) continue
			if (x[i] ~ /^[0-9]+$/ && y[i] ~ /^[0-9]+$/) return x[i] + 0 < y[i] + 0 ? -1 : 1
			if (x[i] ~ /^[0-9]+$/) return -1
			if (y[i] ~ /^[0-9]+$/) return 1
			return x[i] < y[i] ? -1 : 1
		}
		return n < m ? -1 : (n > m ? 1 : 0)
	}
	{ v[NR] = $0 }
	END {
		for (i = 2; i <= NR; i++) { t = v[i]; j = i - 1; while (j >= 1 && cmp(v[j], t) > 0) { v[j + 1] = v[j]; j-- } v[j + 1] = t }
		for (i = 1; i <= NR; i++) print v[i]
	}'
}

# upstream_tags: the upstream v1 release tags, valid ones only, ascending.
upstream_tags() {
	local raw
	raw="$(timeout 120 git ls-remote --tags "$upstream" 'refs/tags/v1.*')" || die "couldn't list $upstream's tags"
	awk '{ print $2 }' <<<"$raw" | sed -n 's#^refs/tags/##p' | grep -v '\^{}$' | while IFS= read -r t; do
		# sdk/* and v0.* never match refs/tags/v1.*; the pattern is checked
		# anyway, and anything else is dropped quietly.
		if valid_tag "$t"; then echo "$t"; fi
	done | vsort
}

# image_digest <tag>: the engine image's multi-platform index digest, or
# nothing (status 3) if the registry has no such tag yet.
image_digest() {
	local tag="$1" outp media digest
	if ! outp="$(timeout 120 docker buildx imagetools inspect --format '{{.Manifest.MediaType}} {{.Manifest.Digest}}' "$engine_repo:$tag" 2>&1)"; then
		if grep -qiE 'not found|manifest unknown' <<<"$outp"; then return 3; fi
		say "$outp"
		die "looking up $engine_repo:$tag failed"
	fi
	read -r media digest <<<"$outp"
	need_digest "$digest"
	case "$media" in
	application/vnd.oci.image.index.v1+json | application/vnd.docker.distribution.manifest.list.v2+json) ;;
	*) die "$engine_repo:$tag is a $media, not a multi-platform index" ;;
	esac
	echo "$digest"
}

# fetch_branch [<ref>]: the branch's head (or the named branch's), fresh, as
# release-check/head.
fetch_branch() {
	local ref="${1:-$branch}"
	timeout 300 git fetch -q --no-tags "$remote" "+refs/heads/$ref:refs/remotes/release-check/head" ||
		die "couldn't fetch $ref"
}
# fetch_tag <tag>: the upstream tag, as refs/tags/<tag>.
fetch_tag() {
	timeout 600 git fetch -q --no-tags "$upstream" "+refs/tags/$1:refs/tags/$1" || die "couldn't fetch $1 from $upstream"
}

# pending_run: a dispatched candidate run whose verdict hasn't been read
# yet. If the script ends in failure with one pending (in verify's fetches
# and advisory diff, or in verify's or rebuild's wait or verdict read), it is
# cancelled, best effort and with GITHUB_TOKEN, rather than left to run
# unread (zerobull-consulting/dagger#163, zerobull-consulting/dagger#187).
# wait_since: when verify dispatched (SECONDS), which wait_run counts its
# deadline from.
pending_run=""
wait_since=""
# tree: a fresh worktree of the branch's head (or of new_tree's argument), in
# $tree.
tree=""
cleanup() {
	local rc=$?
	if [[ -n "$pending_run" && $rc -ne 0 ]]; then
		local url="$server/$repo/actions/runs/$pending_run" state
		# A run that has already ended can't be cancelled (the API refuses):
		# it is said to have ended, not to be cancelled.
		if api -X POST "repos/$repo/actions/runs/$pending_run/cancel" >/dev/null 2>&1; then
			say "the candidate run $url is not awaited and no result is given: asked GitHub to cancel it"
		else
			# "<status> <conclusion>", read once; empty when the read fails
			# (gh prints an error's body on stdout, so its output alone
			# doesn't say the read worked).
			state="$(api "repos/$repo/actions/runs/$pending_run" 2>/dev/null |
				jq -r '"\(.status) \(.conclusion // "")"' 2>/dev/null)" || state=""
			case "$state" in
			"completed "*) say "the candidate run $url ended '${state#completed }' and no result is given: nothing to cancel" ;;
			"") say "the candidate run $url is not awaited and no result is given; cancelling it and reading its state failed: it may finish unread" ;;
			*) say "the candidate run $url is not awaited and no result is given; cancelling it failed: it will finish unread" ;;
			esac
		fi
	fi
	if [[ -n "$tree" ]]; then git worktree remove --force "$tree" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT
new_tree() {
	fetch_branch "$@"
	tree="$(mktemp -d)"
	git worktree add -q --detach "$tree" release-check/head
}
# manifest_edit <engines.sh edit args...>: edits the worktree's manifest.
# In a dry run, an edit engines.sh would refuse because the version is
# listed already (a dry run of a chosen, accepted version) is printed
# instead, so the rest of the dry run still shows.
manifest_edit() {
	local f="$tree/$manifest_path"
	if is_dry && [[ "$1" != set-base ]] && ENGINES_FILE="$f" "$engines" listed | grep -qxF "$2"; then
		would "run 'engines.sh $*' on $manifest_path (refused for real: $2 is listed already)"
		echo "# dry run: $*" >>"$f"
		return
	fi
	ENGINES_FILE="$f" "$engines" "$@" >&2
}
commit() {
	git -C "$tree" -c user.name="$bot_name" -c user.email="$bot_email" commit -q "$@"
}

# push <branch>: pushes the worktree's HEAD as a new branch, as the bot.
# The token goes in through git's environment, never its arguments.
push() {
	local to="$1"
	if is_dry; then
		would "push $to at $(git -C "$tree" rev-parse --short HEAD), as the bot"
		return
	fi
	[[ -n "${BOT_TOKEN:-}" ]] || die "BOT_TOKEN is not set: the bot's token is missing or expired; nothing was pushed"
	local auth
	auth="$(printf 'x-access-token:%s' "$BOT_TOKEN" | base64 -w0)"
	GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0="http.$server/.extraheader" GIT_CONFIG_VALUE_0="AUTHORIZATION: basic $auth" \
		timeout 300 git -C "$tree" push -q "$remote" "HEAD:refs/heads/$to" ||
		die "pushing $to failed: is the bot's token missing, expired, or without access?"
}

# open_pr <head> <title> <body file>: opens a PR into the branch, as the
# bot, and prints its number.
open_pr() {
	local head="$1" title="$2" body="$3"
	if is_dry; then
		would "open a PR from $head: '$title', with this description:"
		sed 's/^/    | /' "$body" >&2
		echo 0
		return
	fi
	local pr
	pr="$(jq -n --arg t "$title" --arg h "$head" --arg b "$branch" --rawfile body "$body" \
		'{title: $t, head: $h, base: $b, body: $body, maintainer_can_modify: true}' |
		bot_api -X POST "repos/$repo/pulls" --input - | jq -r .number)" ||
		die "opening the PR from $head failed: is the bot's token missing, expired, or without access?"
	need_number "$pr"
	say "opened $server/$repo/pull/$pr"
	echo "$pr"
}

# release_prs <version>: the open release-check PRs for the version, from
# this repository's own branches (a fork's PR may use the same name), as
# "<number> <kind>" lines.
release_prs() {
	api "repos/$repo/pulls?state=open&base=$branch&per_page=100" |
		jq -r --arg v "$1" --arg r "$repo" '.[] | select(.head.repo.full_name == $r) | select(.head.ref | test("^release-check/(accept|rebuild|reject)/") ) |
			select((.head.ref | split("/") | .[2]) == $v) | "\(.number) \(.head.ref | split("/") | .[1])"'
}
# release_branches <version>: the release-check branches for the version.
release_branches() {
	api "repos/$repo/git/matching-refs/heads/release-check/" |
		jq -r --arg v "$1" '.[].ref | sub("^refs/heads/"; "") | select(test("^release-check/(accept|rebuild|reject)/") and (split("/") | .[2]) == $v)'
}
issue_title() { echo "compat-break: engine $1 fails against the cache service"; }
# open_issue <version>: the open compat-break issue's number, if any.
open_issue() {
	api "repos/$repo/issues?labels=compat-break&state=open&per_page=100" |
		jq -r --arg t "$(issue_title "$1")" '[.[] | select(.pull_request == null and .title == $t)][0].number // empty'
}

# --- the verifying run ---

# dispatch <workflow> <ref> <inputs JSON>: starts a workflow_dispatch run
# with GITHUB_TOKEN and prints its id, from the API's reply.
dispatch() {
	local wf="$1" ref="$2" inputs="$3" reply id
	reply="$(jq -n --arg r "$ref" --argjson i "$inputs" '{ref: $r, inputs: $i, return_run_details: true}' |
		api -X POST "repos/$repo/actions/workflows/$wf/dispatches" --input -)" || die "dispatching $wf on $ref failed"
	id="$(jq -r '.workflow_run_id // empty' <<<"$reply" 2>/dev/null || true)"
	[[ "$id" =~ ^[0-9]{1,20}$ ]] || die "dispatching $wf on $ref started a run the API didn't name; nothing waits on a guess"
	echo "$id"
}

# wait_run <id>: waits, bounded, until the run completes, and prints its
# conclusion.
wait_run() {
	local id="$1" deadline run status conclusion
	# From the dispatch (wait_since, if set), not from here: work done between
	# the two doesn't extend the wait past the step's timeout.
	deadline=$((${wait_since:-$SECONDS} + wait_minutes * 60))
	while :; do
		if run="$(api "repos/$repo/actions/runs/$id")"; then
			status="$(jq -r .status <<<"$run")"
			conclusion="$(jq -r '.conclusion // ""' <<<"$run")"
			if [[ "$status" == completed ]]; then
				echo "$conclusion"
				return
			fi
		else
			say "reading run $id failed; trying again"
		fi
		((SECONDS < deadline)) || die "run $id didn't complete within $wait_minutes minutes"
		sleep "$poll"
	done
}

# The jobs of remote-cache-tests.yml whose result is the engine's: the
# verdict is theirs alone (zerobull-consulting/dagger#166). Any other job
# (unit tests, the MinIO tests) doesn't depend on the engine.
engine_jobs=("real engines (e2e)" "sidecar demo")

engine_json="$(printf '%s\n' "${engine_jobs[@]}" | jq -R . | jq -s -c .)"
# is_engine: jq's filter for "this job is an engine job", $e being the list.
is_engine='(.name as $n | $e | index($n)) != null'

# failing_jobs: an engine job's failures (a run's jobs JSON on stdin), as a
# Markdown list.
failing_jobs() {
	jq -r --argjson e "$engine_json" \
		'.jobs[] | select(.conclusion == "failure" or .conclusion == "timed_out") | select('"$is_engine"') | "- [\(.name)](\(.html_url)): \(.conclusion)"'
}

# The e2e and the sidecar demo against the candidate, waited on, write
# <prefix>_run,
# <prefix>_conclusion and <prefix>-jobs.md to the notes. Prints pass or
# fail; anything else is an error.
# start_tests <ref> <version> <digest>: the dispatch alone; prints the run's
# id. finish_tests <id> <prefix>: the wait and the verdict. Apart, so the
# bundle test runs while the run does.
start_tests() {
	local id
	id="$(dispatch remote-cache-tests.yml "$1" "$(jq -n --arg v "$2" --arg d "$3" '{"candidate-version": $v, "candidate-digest": $d}')")"
	need_run "$server/$repo/actions/runs/$id"
	say "verifying $2 on $1: $server/$repo/actions/runs/$id"
	echo "$id"
}
finish_tests() {
	local id="$1" p="$2" url conclusion jobs names early other n
	url="$server/$repo/actions/runs/$id"
	conclusion="$(wait_run "$id")"
	note "${p}_run" "$url"
	note "${p}_conclusion" "$conclusion"
	case "$conclusion" in
	success | failure) ;;
	*) die "the verifying run $url ended '$conclusion', neither green nor red: not deciding on it" ;;
	esac
	# The verdict is the engine jobs' alone, so the jobs are read, once, on
	# a green run too (a skipped engine job leaves the run green). A failed
	# read decides nothing.
	# (tried three times: a transient error shouldn't cost the whole run).
	local try
	for try in 1 2 3; do
		jobs="$(api "repos/$repo/actions/runs/$id/jobs?per_page=100")" && jq -e '.jobs | type == "array"' <<<"$jobs" >/dev/null && break
		jobs=""
		((try == 3)) || sleep "$poll"
	done
	[[ -n "$jobs" ]] ||
		die "couldn't read the verifying run $url's jobs: not deciding on it"
	jq -e '(.total_count // (.jobs | length)) <= (.jobs | length)' <<<"$jobs" >/dev/null ||
		die "the verifying run $url has more jobs than were listed: not deciding on it"
	# A job that isn't the engine's and didn't pass says nothing about the
	# engine: an error, and the next hour verifies the release again.
	other="$(jq -r --argjson e "$engine_json" \
		'[.jobs[] | select('"$is_engine"' | not) | select(.conclusion != "success" and .conclusion != "skipped") | "\(.name): \(.conclusion)"] | join(", ")' <<<"$jobs")"
	[[ -z "$other" ]] || die "the verifying run $url had jobs fail that don't test the engine ($other): not deciding on it"
	# Each engine job must be there, once.
	for n in "${engine_jobs[@]}"; do
		[[ "$(jq -r --arg n "$n" '[.jobs[] | select(.name == $n)] | length' <<<"$jobs")" == 1 ]] ||
			die "the verifying run $url has no single '$n' job: not deciding on it"
	done
	names="$(jq -r --argjson e "$engine_json" \
		'[.jobs[] | select('"$is_engine"') | .conclusion] | join(" ")' <<<"$jobs")"
	# Every engine job has to have run to a green or red end first: one
	# failed and the other skipped or cancelled never tested both engines.
	jq -e --argjson e "$engine_json" '[.jobs[] | select('"$is_engine"') | .conclusion] | all(. == "success" or . == "failure" or . == "timed_out")' <<<"$jobs" >/dev/null ||
		die "the verifying run $url ended '$conclusion' with its engine jobs '$names': not deciding on it"
	case " $names " in
	*" failure "* | *" timed_out "*)
		# A job that failed before testing anything (its checkout, or the
		# pin step refusing the candidate) says nothing about the engine.
		early="$(jq -r '[.jobs[] | select(.conclusion == "failure") |
			select(any(.steps[]?; .conclusion == "failure" and (.name == "Checkout" or .name == "The engine pin, from the manifest"))) | .name] | join(", ")' <<<"$jobs")"
		[[ -z "$early" ]] || die "the verifying run $url failed before testing ($early): not deciding on it"
		failing_jobs <<<"$jobs" >"$notes/$p-jobs.md"
		echo fail
		;;
	*)
		# Green is both engine jobs green, in a run that is green.
		[[ "$names" == "success success" && "$conclusion" == success ]] ||
			die "the verifying run $url ended '$conclusion' with its engine jobs '$names': not deciding on it"
		: >"$notes/$p-jobs.md"
		echo pass
		;;
	esac
}

# note <name> <value>: a scalar for a later job, in $notes/notes.env.
note() {
	mkdir -p "$notes"
	[[ "$2" =~ ^[A-Za-z0-9_.:/#@+-]*$ ]] || die "note $1 has an unexpected character"
	echo "$1=$2" >>"$notes/notes.env"
}
# get <name>: a note's last value.
get() {
	[[ -f "$notes/notes.env" ]] || { echo ""; return; }
	awk -F= -v k="$1" '$1 == k { v = substr($0, length(k) + 2) } END { print v }' "$notes/notes.env"
}

# merge_tag <tag>: merges the tag into the worktree. Prints merged,
# ancestor (already merged: nothing changes) or conflict (aborted).
merge_tag() {
	local tag="$1"
	if git -C "$tree" merge-base --is-ancestor "refs/tags/$tag" HEAD; then
		echo ancestor
		return
	fi
	if git -C "$tree" -c user.name="$bot_name" -c user.email="$bot_email" merge -q --no-ff --no-edit \
		-m "Merge upstream $tag into $branch (release check)" "refs/tags/$tag" >&2; then
		echo merged
	else
		git -C "$tree" merge --abort >/dev/null 2>&1 || true
		echo conflict
	fi
}

bundle_const() {
	git show "$1:dagql/cache_transfer_types.go" 2>/dev/null | sed -n 's/^const valueBundleVersion = \([0-9][0-9]*\)$/\1/p'
}

# advisory <tag>: the diff of the engine's side of the remote cache from the
# manifest's base to the tag, and whether the bundle or protocol version
# changed. Advisory: the service also depends on engine.* events and
# session spans outside these paths, and the e2e is the gate.
advisory() {
	local tag="$1" base old new proto=no
	base="$("$engines" base)"
	fetch_tag "$base"
	old="$(bundle_const "refs/tags/$base")"
	new="$(bundle_const "refs/tags/$tag")"
	# An unread version is never "unchanged": that would keep accept lines a
	# changed bundle format invalidates.
	[[ -n "$old" && -n "$new" ]] ||
		die "couldn't read dagql's valueBundleVersion in $base or $tag (dagql/cache_transfer_types.go): update bundle_const for its new form"
	local paths=(engine/remotecache cmd/engine/remotecache.go 'engine/server/remote_cache*.go' 'dagql/cache*.go' ':!**/*_test.go')
	git diff --quiet "refs/tags/$base" "refs/tags/$tag" -- engine/remotecache/protocol ':!**/*_test.go' || proto=yes
	local reset=no
	[[ "$old" == "$new" && "$proto" == no ]] || reset=yes
	note reset "$reset"
	{
		echo "The engine's side of the remote cache, $base to $tag (advisory: the e2e is the gate):"
		echo
		echo '```'
		git diff --stat "refs/tags/$base" "refs/tags/$tag" -- "${paths[@]}" || true
		echo '```'
		echo
		echo "- Bundle version (dagql's valueBundleVersion): ${old:-?} in $base, ${new:-?} in $tag."
		echo "- Protocol (engine/remotecache/protocol): $([[ "$proto" == yes ]] && echo changed || echo unchanged)."
	} >"$notes/advisory.md"
	cat "$notes/advisory.md" >&2
}

# bundle_result: the bundle job's verdict (RELEASE_CHECK_BUNDLE, from the
# workflow), or the notes'.
bundle_result() { echo "${RELEASE_CHECK_BUNDLE:-$(get bundle)}"; }

# bundle_test: TestBundleVersionMatchesEngineCapture in the worktree. It
# compiles and runs upstream's code, so only cmd_bundle calls it, in a job
# that holds no credential.
bundle_test() {
	if (cd "$tree" && timeout 1200 go test -count=1 -run '^TestBundleVersionMatchesEngineCapture$' ./cmd/cacheservice/metastore/ >&2); then
		echo pass
	else
		echo fail
	fi
}

# --- the subcommands ---

# The most dispatches of cacheservice-publish.yml for one head: a publish
# that failed is retried, but not forever (zerobull-consulting/dagger#166).
max_publish_dispatches=2

cmd_publish() {
	local head sha date age runs total ok active dispatched
	head="$(api "repos/$repo/branches/$branch")" || die "couldn't read $branch"
	sha="$(jq -r .commit.sha <<<"$head")"
	[[ "$sha" =~ ^[0-9a-f]{40}$ ]] || die "$branch's head isn't a commit id"
	date="$(jq -r .commit.commit.committer.date <<<"$head")"
	age=$(($(date +%s) - $(date -d "$date" +%s)))
	runs="$(api "repos/$repo/actions/workflows/cacheservice-publish.yml/runs?head_sha=$sha&branch=$branch&per_page=100")" || die "couldn't read the publish runs of ${sha:0:12}"
	# A failed read decides nothing, and neither does a reply that isn't a
	# list of runs: both are errors, never "no run".
	jq -e '.workflow_runs | type == "array"' <<<"$runs" >/dev/null || die "the publish runs of ${sha:0:12} aren't a list"
	# More runs than were listed (a head with over 100) decides nothing.
	jq -e '(.total_count // (.workflow_runs | length)) <= (.workflow_runs | length)' <<<"$runs" >/dev/null || die "the head ${sha:0:12} has more publish runs than were listed"
	total="$(jq -r '.workflow_runs | length' <<<"$runs")"
	need_number "$total"
	ok="$(jq -r '[.workflow_runs[] | select(.status == "completed" and .conclusion == "success")] | length' <<<"$runs")"
	# Queued, waiting, in progress: anything that isn't completed may still
	# publish.
	active="$(jq -r '[.workflow_runs[] | select(.status != "completed")] | length' <<<"$runs")"
	dispatched="$(jq -r '[.workflow_runs[] | select(.event == "workflow_dispatch")] | length' <<<"$runs")"
	need_number "$ok"
	need_number "$active"
	need_number "$dispatched"
	if ((ok > 0)); then
		say "$branch's head ${sha:0:12} has a successful publish run: nothing to dispatch"
		out publish none
	elif ((active > 0)); then
		say "$branch's head ${sha:0:12} has a publish run queued or in progress: nothing to dispatch"
		out publish none
	elif ((total == 0 && age < min_head_age)); then
		say "$branch's head ${sha:0:12} is ${age}s old, and its push's run may not be listed yet: next hour"
		out publish none
	elif ((dispatched >= max_publish_dispatches)); then
		# On stdout, for the run's summary: nothing else will say it.
		say "::warning::$branch's head ${sha:0:12} has no successful publish after $dispatched dispatches (the most: $max_publish_dispatches); not dispatching again. Re-run cacheservice-publish.yml by hand."
		echo "$branch's head ${sha:0:12} has no successful publish after $dispatched dispatches (the most: $max_publish_dispatches); not dispatching again. Re-run cacheservice-publish.yml by hand."
		out publish bounded
	elif is_dry; then
		would "dispatch cacheservice-publish.yml on $branch (head ${sha:0:12} has no successful or running publish)"
		out publish dispatched
	else
		dispatch cacheservice-publish.yml "$branch" '{}' >/dev/null
		say "dispatched cacheservice-publish.yml on $branch for ${sha:0:12}, which has $total publish run(s), none successful"
		out publish dispatched
	fi
}

cmd_plan() {
	local forced="${1:-}" listed newest t prs branches issue d pr kind
	listed="$("$engines" listed)"
	newest="$(vsort <<<"$listed" | tail -n1)"
	local -a tags
	if [[ -n "$forced" ]]; then
		# Verifying a chosen release, listed or not, changes nothing only in
		# a dry run.
		is_dry || die "plan <version> is for dry runs only"
		need_tag "$forced"
		tags=("$forced")
	else
		# Listed first, in this shell: a failed poll fails the run, never
		# reads as "nothing new".
		local all
		all="$(upstream_tags)"
		mapfile -t tags < <(while IFS= read -r t; do
			[[ -n "$t" && "$(printf '%s\n%s\n' "$newest" "$t" | vsort | tail -n1)" == "$t" && "$t" != "$newest" ]] && echo "$t"
			true
		done <<<"$all")
		say "the manifest's newest version is $newest; newer upstream releases: ${tags[*]:-none}"
	fi
	for t in "${tags[@]}"; do
		prs="$(release_prs "$t")"
		if [[ -n "$prs" ]]; then
			read -r pr kind <<<"$(head -n1 <<<"$prs")"
			say "$t: its $kind PR $server/$repo/pull/$pr is open; waiting for it before anything newer"
			out action pending
			out version "$t"
			out kind "$kind"
			out pr "$pr"
			return
		fi
		branches="$(release_branches "$t")"
		issue="$(open_issue "$t")"
		if [[ -n "$issue" ]] && ! grep -qxF "release-check/reject/$t" <<<"$branches"; then
			need_number "$issue"
			say "$t: compat-break issue #$issue is open, without its reject PR: opening it"
			out action resume-reject
			out version "$t"
			out issue "$issue"
			return
		fi
		if [[ -n "$branches" && -z "$forced" ]]; then
			die "$t has release-check branch(es) with no open PR: $(paste -sd' ' <<<"$branches"). A PR closed by hand, or a run that stopped part way. Delete the branch(es) to verify $t again, or add its line to the manifest by hand"
		fi
		local rc=0
		d="$(image_digest "$t")" || rc=$?
		if ((rc == 3)); then
			# Not skipped past: a newer release accepted first would make
			# this one old, and it would never be checked.
			say "$t: no $engine_repo:$t image yet; waiting for it before anything newer"
			out action none
			return
		fi
		((rc == 0)) || exit "$rc"
		say "$t: new, image $d: verifying it"
		out action verify
		out version "$t"
		out digest "$d"
		return
	done
	out action none
}

# cmd_bundle: the bundle test with the tag merged, on its own: it runs
# upstream's code, which must never share a process tree, or a job, with a
# token (it could read one from its environment or its parent's). Refused
# if any token is in the environment.
cmd_bundle() {
	local v="$1" merge bundle var
	need_tag "$v"
	need_test_ref
	for var in GH_TOKEN GITHUB_TOKEN BOT_TOKEN ACTIONS_ID_TOKEN_REQUEST_TOKEN; do
		[[ -z "${!var:-}" ]] || die "$var is set: the bundle test runs upstream's code, and must run without any credential"
	done
	fetch_tag "$v"
	# The service as it is: the branch's, or in a dry run (from the workflow),
	# the dry run's own ref, the one verify's e2e runs on.
	new_tree "${RELEASE_CHECK_TEST_REF:-$branch}"
	merge="$(merge_tag "$v")"
	if [[ "$merge" == conflict ]]; then
		# Not run, so not passed: it verified nothing.
		bundle=conflict
	else
		bundle="$(bundle_test)"
	fi
	say "TestBundleVersionMatchesEngineCapture with $v merged: $bundle"
	out merge "$merge"
	out bundle "$bundle"
}

cmd_verify() {
	local v="$1" d="$2" a
	need_tag "$v"
	need_digest "$d"
	need_test_ref
	mkdir -p "$notes"
	: >"$notes/notes.env"
	note version "$v"
	note digest "$d"
	local id
	wait_since=$SECONDS
	# The service as it is: the branch's, or in a dry run (from the
	# workflow), the dry run's own ref. Dispatched first: the fetches and the
	# advisory diff run while the candidate run does, not before it, out of
	# the step's timeout.
	id="$(start_tests "${RELEASE_CHECK_TEST_REF:-$branch}" "$v" "$d")"
	# A failure from here until the verdict is read (the fetches, the
	# advisory diff, a wait that times out, a read that fails) is an error (a
	# step that fails), never a verdict, and the exit trap cancels the
	# dispatched run, which nothing would read: the next hour verifies the
	# release again.
	pending_run="$id"
	fetch_tag "$v"
	advisory "$v"
	a="$(finish_tests "$id" a)"
	pending_run=""
	# The e2e and the sidecar demo's verdict. A is this and the bundle job's
	# both passing (the workflow decides): a bundle test that didn't run (the
	# tag conflicts with the branch) verified nothing.
	out result "$a"
	out run "$(get a_run)"
}

pr_title() { echo "remote cache: $1 engine $2 (release check)"; }

cmd_accept() {
	local v="$1" d="$2" run="$3" body pr
	need_tag "$v"
	need_digest "$d"
	need_run "$run"
	new_tree
	manifest_edit add-accept "$v" "$d" "$run" >&2
	git -C "$tree" add "$manifest_path"
	commit -m "remote cache: accept engine $v" -m "Verified by $run (release check)."
	push "release-check/accept/$v"
	body="$(mktemp)"
	{
		echo "The release check verified upstream \`$v\` against the cache service as it is: outcome **A**. This PR only adds its accept line:"
		echo
		echo '```'
		echo "accept $v $d verified: $run"
		echo '```'
		echo
		echo "- **Verifying run** (the e2e and the sidecar demo against \`$engine_repo:$v@$d\`): $run"
		echo "- **TestBundleVersionMatchesEngineCapture** with \`$v\` merged in: $(bundle_result)."
		echo
		[[ -f "$notes/advisory.md" ]] && cat "$notes/advisory.md" && echo
		if [[ "$auto_merge" == true ]]; then
			echo "**Auto-merge** (squash) is enabled by the release check once this opens, and the \`auto-merge: adds compatible engine only\` check turns it off if this PR ever changes anything else. After the merge, the next hourly run dispatches \`cacheservice-publish.yml\`, which moves \`dagger-remote-cache:$v\`."
		else
			echo "**Auto-merge is off** (\`RELEASE_CHECK_AUTO_MERGE\` isn't \`true\`): merge by hand with a squash. The next hourly run then dispatches \`cacheservice-publish.yml\` if nothing published the merge."
		fi
		echo
		echo "Squash title: \`$(pr_title accept "$v") (zerobull-consulting/dagger#<this PR>)\`. See \`hack/remote-cache/README.md\`, \"Release check\"."
	} >"$body"
	pr="$(open_pr "release-check/accept/$v" "$(pr_title accept "$v")" "$body")"
	out pr "$pr"
}

cmd_auto_merge() {
	local pr="$1" info files title node
	need_number "$pr"
	if [[ "$auto_merge" != true ]]; then
		say "auto-merge is off: RELEASE_CHECK_AUTO_MERGE isn't true (set it once 'auto-merge: adds compatible engine only' is a required check)"
		out auto_merge off
		return
	fi
	info="$(api "repos/$repo/pulls/$pr")" || die "couldn't read PR #$pr"
	jq -e --arg b "$branch" --arg r "$repo" '.state == "open" and .base.ref == $b and .head.repo.full_name == $r and (.head.ref | test("^release-check/accept/v1\\.[0-9]+\\.[0-9]+(-[A-Za-z0-9.]+)?$"))' <<<"$info" >/dev/null ||
		die "PR #$pr is not an open release-check accept PR from this repository into $branch"
	files="$(api "repos/$repo/pulls/$pr/files?per_page=100")" || die "couldn't list PR #$pr's files"
	jq -e --arg p "$manifest_path" 'length == 1 and .[0].filename == $p and .[0].status == "modified" and .[0].additions == 1 and .[0].deletions == 0' <<<"$files" >/dev/null ||
		die "PR #$pr changes more than one added line of $manifest_path: not enabling auto-merge"
	if jq -e '.auto_merge != null' <<<"$info" >/dev/null; then
		say "auto-merge is already on for #$pr"
		out auto_merge on
		return
	fi
	title="$(jq -r .title <<<"$info") (zerobull-consulting/dagger#$pr)"
	node="$(jq -r .node_id <<<"$info")"
	if is_dry; then
		would "enable squash auto-merge on #$pr, titled '$title'"
		out auto_merge on
		return
	fi
	local msg="Adds the accept line the release check verified (zerobull-consulting/dagger#117, D5, outcome A)." err sha
	# shellcheck disable=SC2016 # GraphQL variables, not shell
	if err="$(api graphql -f query='mutation($id: ID!, $title: String!, $body: String!) {
		enablePullRequestAutoMerge(input: {pullRequestId: $id, mergeMethod: SQUASH, commitHeadline: $title, commitBody: $body}) { clientMutationId }
	}' -f id="$node" -f title="$title" -f body="$msg" 2>&1)"; then
		say "auto-merge (squash) enabled on #$pr"
		out auto_merge on
		return
	fi
	# GitHub refuses auto-merge on a PR whose required checks have all
	# passed already ("clean status"): what auto-merge would do then is
	# merge it now, so this does, with GITHUB_TOKEN, pinned to the head the
	# files were checked at. The ruleset still applies: GITHUB_TOKEN can't
	# bypass it, so a check that hasn't passed refuses the merge.
	grep -qi 'clean status' <<<"$err" || { say "$err"; die "enabling auto-merge on #$pr failed"; }
	sha="$(jq -r .head.sha <<<"$info")"
	[[ "$sha" =~ ^[0-9a-f]{40}$ ]] || die "PR #$pr's head isn't a commit id"
	jq -n --arg t "$title" --arg m "$msg" --arg s "$sha" '{merge_method: "squash", commit_title: $t, commit_message: $m, sha: $s}' |
		api -X PUT "repos/$repo/pulls/$pr/merge" --input - >/dev/null || die "#$pr's checks have passed, but merging it failed"
	say "#$pr's required checks had passed already, so auto-merge couldn't be enabled: squash-merged it at ${sha:0:12}"
	out auto_merge merged
}

cmd_rebuild() {
	local v="$1" d="$2" merge reset run b body pr old
	need_tag "$v"
	need_digest "$d"
	mkdir -p "$notes"
	fetch_tag "$v"
	new_tree
	merge="$(merge_tag "$v")"
	note b_merge "$merge"
	case "$merge" in
	ancestor)
		say "$v is already merged into $branch: a rebuild would be the code A ran. Not rebuilding"
		note b skipped
		out result skipped
		return
		;;
	conflict)
		say "merging $v into $branch conflicts: not rebuilding"
		note b skipped
		out result skipped
		return
		;;
	esac
	# The rebuild's code is this same merge (plus a manifest line), whose
	# bundle test the bundle job ran: if it didn't pass, no rebuild can.
	if [[ "$(bundle_result)" != pass ]]; then
		say "the bundle test with $v merged didn't pass ($(bundle_result)): a rebuild is that merge, so not rebuilding"
		note b_merge bundle
		note b skipped
		out result skipped
		return
	fi
	old="$("$engines" base)"
	manifest_edit set-base "$v" >&2
	git -C "$tree" add "$manifest_path"
	commit -m "remote cache: build on upstream $v" -m "Moves the manifest's base from $old (release check)."
	push "release-check/rebuild/$v"
	if is_dry; then
		would "verify $v on release-check/rebuild/$v, as verify does, then add its accept line and open the PR"
		git -C "$tree" diff --stat "release-check/head" HEAD -- . >&2 || true
		note b dry-run
		out result dry-run
		return
	fi
	local id
	id="$(start_tests "release-check/rebuild/$v" "$v" "$d")"
	# As in verify: a wait that times out, or a verdict that can't be read,
	# cancels the run (zerobull-consulting/dagger#187).
	pending_run="$id"
	b="$(finish_tests "$id" b)"
	pending_run=""
	note b "$b"
	if [[ "$b" != pass ]]; then
		out result fail
		return
	fi
	run="$(get b_run)"
	reset="$(get reset)"
	if [[ "$reset" == yes ]]; then
		manifest_edit reset-accept "$v" "$d" "$run" >&2
	else
		manifest_edit add-accept "$v" "$d" "$run" >&2
	fi
	git -C "$tree" add "$manifest_path"
	commit -m "remote cache: accept engine $v" -m "Verified by $run (release check)."
	push "release-check/rebuild/$v"
	body="$(mktemp)"
	{
		echo "Upstream \`$v\` fails against the cache service as it is ($(get a_run)), and passes once the service is rebuilt on it: outcome **B**."
		echo
		echo "- Merges upstream \`$v\` into \`$branch\`, and moves the manifest's \`base\` from \`$old\` to \`$v\`."
		if [[ "$reset" == yes ]]; then
			echo "- **The bundle or protocol version changed**, so the accept list resets to \`$v\` alone. Older builds keep serving older engines: their \`dagger-remote-cache:<version>\` tags stay where they are."
		else
			echo "- Adds \`$v\`'s accept line, first; the other lines stay, and the release matrix verifies each of them again once this merges."
		fi
		echo "- **Verifying run** of this branch against \`$engine_repo:$v@$d\`: $run"
		echo "- Failing jobs of the run as-is:"
		cat "$notes/a-jobs.md" 2>/dev/null || true
		echo
		[[ -f "$notes/advisory.md" ]] && cat "$notes/advisory.md" && echo
		echo "**Merge this by hand, with a merge commit** (\`gh pr merge --merge\`), never a squash: upstream \`$v\` has to stay a parent, or \`fork paths\` keeps measuring from the old base and the next sync conflicts again. The release check doesn't auto-merge a rebuild. See \`hack/remote-cache/README.md\`, \"Release check\"."
	} >"$body"
	pr="$(open_pr "release-check/rebuild/$v" "$(pr_title "build on" "$v")" "$body")"
	out result pass
	out pr "$pr"
}

cmd_reject() {
	local v="$1" issue="${2:-}" body title pr
	need_tag "$v"
	if [[ -z "$issue" ]]; then
		issue="$(open_issue "$v")"
	fi
	title="$(issue_title "$v")"
	if [[ -z "$issue" ]]; then
		body="$(mktemp)"
		{
			echo "Upstream \`$v\` (\`$engine_repo:$v@$(get digest)\`) fails against the cache service: the release check's outcome **C** (zerobull-consulting/dagger#117, D5)."
			echo
			echo "**As it is** (e2e and sidecar demo of \`$branch\` against it): $(get a_run), $(get a_conclusion)."
			cat "$notes/a-jobs.md" 2>/dev/null || true
			echo
			echo "- TestBundleVersionMatchesEngineCapture with \`$v\` merged in: $(bundle_result)."
			case "$(get b)" in
			fail) echo "- **Rebuilt on \`$v\`** (branch \`release-check/rebuild/$v\`): $(get b_run), $(get b_conclusion)." && cat "$notes/b-jobs.md" 2>/dev/null ;;
			skipped)
				case "$(get b_merge)" in
				ancestor) echo "- **Not rebuilt:** \`$branch\` contains \`$v\` already, so a rebuild is the code above." ;;
				conflict) echo "- **Not rebuilt:** merging \`$v\` into \`$branch\` conflicts." ;;
				*) echo "- **Not rebuilt:** the bundle test with \`$v\` merged didn't pass, and a rebuild is that same merge." ;;
				esac
				;;
			*) echo "- Not rebuilt." ;;
			esac
			echo
			[[ -f "$notes/advisory.md" ]] && cat "$notes/advisory.md" && echo
			echo "The service refuses \`$v\` engines until this is fixed. The reject PR names this issue; once a fix lands, remove the reject line and verify \`$v\` again (\`hack/remote-cache/README.md\`, \"Release check\")."
		} >"$body"
		if is_dry; then
			would "label compat-break and file '$title':"
			sed 's/^/    | /' "$body" >&2
			issue=0
		else
			if ! api "repos/$repo/labels/compat-break" >/dev/null 2>&1; then
				api -X POST "repos/$repo/labels" -f name=compat-break -f color=b60205 \
					-f description="An upstream engine release the cache service fails against (release check)" >/dev/null ||
					die "creating the compat-break label failed"
			fi
			issue="$(jq -n --arg t "$title" --rawfile b "$body" '{title: $t, body: $b, labels: ["compat-break"]}' |
				api -X POST "repos/$repo/issues" --input - | jq -r .number)" || die "filing the compat-break issue failed"
			need_number "$issue"
			say "filed $server/$repo/issues/$issue"
		fi
	else
		need_number "$issue"
		say "compat-break issue #$issue is open already"
	fi
	out issue "$issue"
	new_tree
	manifest_edit add-reject "$v" "$repo#$issue" >&2
	git -C "$tree" add "$manifest_path"
	commit -m "remote cache: reject engine $v" -m "It fails against the cache service: $repo#$issue (release check)."
	push "release-check/reject/$v"
	body="$(mktemp)"
	{
		echo "Upstream \`$v\` fails against the cache service: outcome **C**. This PR adds"
		echo
		echo '```'
		echo "reject $v $repo#$issue"
		echo '```'
		echo
		echo "See $repo#$issue for the failing jobs and the diff. Merge it by hand (squash) once you agree. The release check handles nothing newer while this is open."
	} >"$body"
	pr="$(open_pr "release-check/reject/$v" "$(pr_title reject "$v")" "$body")"
	out pr "$pr"
}

if [[ "${1:-}" == --dry-run ]]; then
	dry=1
	shift
fi
if is_dry; then say "dry run: nothing is pushed, opened or enabled"; fi
case "${1:-}" in
publish) [[ $# -eq 1 ]] || die "usage: release-check.sh publish"; cmd_publish ;;
plan) [[ $# -le 2 ]] || die "usage: release-check.sh plan [<version>]"; cmd_plan "${2:-}" ;;
verify) [[ $# -eq 3 ]] || die "usage: release-check.sh verify <version> <digest>"; cmd_verify "$2" "$3" ;;
bundle) [[ $# -eq 2 ]] || die "usage: release-check.sh bundle <version>"; cmd_bundle "$2" ;;
accept) [[ $# -eq 4 ]] || die "usage: release-check.sh accept <version> <digest> <run URL>"; cmd_accept "$2" "$3" "$4" ;;
auto-merge) [[ $# -eq 2 ]] || die "usage: release-check.sh auto-merge <PR>"; cmd_auto_merge "$2" ;;
rebuild) [[ $# -eq 3 ]] || die "usage: release-check.sh rebuild <version> <digest>"; cmd_rebuild "$2" "$3" ;;
reject) [[ $# -le 3 && $# -ge 2 ]] || die "usage: release-check.sh reject <version> [<issue>]"; cmd_reject "$2" "${3:-}" ;;
*) die "usage: release-check.sh [--dry-run] publish | plan [<version>] | verify <version> <digest> | bundle <version> | accept <version> <digest> <run> | auto-merge <PR> | rebuild <version> <digest> | reject <version> [<issue>]" ;;
esac
