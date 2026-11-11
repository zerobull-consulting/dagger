#!/usr/bin/env bash
# The release check (zerobull-consulting/dagger#117, D5): brings each new
# upstream Dagger release into feature/remote-cache through one sync branch
# at a time, release-check/sync/<version>:
#
#   1. A new release starts the sync: the tag merged into the branch (with a
#      merge commit), the manifest's base moved to it, pushed as
#      release-check/sync/<version>, and a draft PR from it.
#   2. Each hour the sync branch's head is verified: remote-cache-tests.yml
#      dispatched on it with the release as the candidate engine (every job:
#      the unit tests, TestBundleVersionMatchesEngineCapture among them, the
#      MinIO tests, the e2e and the sidecar demo).
#   3. Red: a compat-break issue pointing at the failing run (filed once;
#      each later red head is a comment on it). The branch is expected to
#      stay red until it's compatible: fixes are PRs into the sync branch,
#      squash-merged, and each new head is verified again.
#   4. Green: the release's accept line (citing the green run) is pushed to
#      the sync branch and the PR is marked ready. A human merges it, with a
#      merge commit.
#
# Only one sync is open at a time: newer releases wait until it is merged,
# and are then verified against the branch with its fixes in (a fix made
# for beta.26 may be all beta.27 needs).
#
# .github/workflows/remote-cache-release-check.yml runs it hourly and by
# hand, and only decides when and with which token; everything else is
# here. Each subcommand is one job's work:
#
#   release-check.sh [--dry-run] publish
#       If the branch's head has no successful publish run and none
#       queued or in progress (a publish may fail), dispatches
#       cacheservice-publish.yml on it, which moves the floating
#       :<engine version> tags. At most two dispatches per head (counting
#       its workflow_dispatch runs, a hand re-run too): past that it prints
#       publish=bounded and a warning and dispatches nothing. Skips a head
#       with no run at all younger than 10 minutes, whose push run may not
#       be listed yet.
#   release-check.sh [--dry-run] plan [<version>]
#       Reads the state and prints what to do (below). Reads only. With a
#       version (dry run only), plans starting that release's sync, listed
#       or not.
#   release-check.sh [--dry-run] start <version> <digest>
#       Step 1, as the sync bot (BOT_TOKEN): merges the tag (a conflict
#       leaves the merge to a human, and the branch on feature/remote-cache
#       with the base moved), pushes release-check/sync/<version>, opens its
#       draft PR, and dispatches its first verification (GITHUB_TOKEN).
#   release-check.sh [--dry-run] verify <version> <digest>
#       Step 2: dispatches remote-cache-tests.yml on the sync branch with
#       the candidate inputs, and returns: a later hour reads its verdict.
#   release-check.sh [--dry-run] report <version> <run URL> <PR>
#       Step 3, with GITHUB_TOKEN: files the compat-break issue with the
#       run's failing jobs and the advisory diff and links it from the PR,
#       or comments on the open issue with the run, once per run.
#   release-check.sh [--dry-run] ready <version> <PR>
#       Marks the sync PR ready and says so on it and on the issue
#       (GITHUB_TOKEN), if it is still a draft once its accept line is
#       pushed: accept does this itself, and this retries it if accept
#       stopped after its push.
#   release-check.sh [--dry-run] accept <version> <digest> <run URL> <sha> <PR>
#       Step 4: pushes the accept line to the sync branch as the bot (only
#       if its head is still <sha>, the commit the run verified), then
#       marks the PR ready and says so on it and on the issue
#       (GITHUB_TOKEN). If the bundle or protocol version changed since the
#       manifest's base, the accept list resets to this one version.
#
# plan prints action=, one of:
#   none    nothing new, or the oldest new release has no image yet
#           (nothing newer is tried before it has)
#   start   version= and digest=: a new release, and no sync open
#   sync    version=, digest=, pr=, sha= (the sync branch's head) and state=:
#             needs-merge  the branch doesn't contain the tag yet (its merge
#                          conflicted): a human merges it
#             verify       no verifying run of this head: dispatch one
#             running      its verifying run hasn't finished
#             red          run=: its verifying run failed: report it
#             green        run=: its verifying run passed: accept
#             ready        the branch has the release's accept line: a
#                          human merges the PR (draft=true: mark it ready
#                          first, with the ready subcommand)
#             bounded      two verifying runs of this head ended neither
#                          green nor red (cancelled, say): re-run by hand
# and fails if a release-check branch for the release it would start
# exists without an open sync PR (a PR closed by hand, a run that died, or
# a branch from the earlier accept/rebuild/reject flow): delete it to retry.
#
# With --dry-run (or RELEASE_CHECK_DRY_RUN=1), nothing is pushed, opened,
# filed, commented, marked or dispatched: each is printed instead, starting
# "dry run:".
#
# Tokens: GH_TOKEN is the workflow's GITHUB_TOKEN, for reads, dispatches,
# issues, comments and marking the PR ready. BOT_TOKEN is the sync bot's
# (CACHE_BOT_SYNC_TOKEN), only for pushes and opening the PR, whose CI must
# run. Neither is ever printed or put on a command line. Neither merges.
#
# Upstream's code never runs here: only git merges, diffs and pushes it.
# It runs in the verifying run, remote-cache-tests.yml, with that
# workflow's own read-only token.
#
# Everything from upstream or a registry is checked against a strict
# pattern before it reaches a branch name, a file or a command. Every
# network call is bounded. Settings (for tests): RELEASE_CHECK_REPO,
# RELEASE_CHECK_BRANCH, RELEASE_CHECK_REMOTE, RELEASE_CHECK_UPSTREAM,
# RELEASE_CHECK_MIN_HEAD_AGE.
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
min_head_age="${RELEASE_CHECK_MIN_HEAD_AGE:-600}"
dry="${RELEASE_CHECK_DRY_RUN:-}"
bot_name="zerobull release check"
bot_email="release-check@users.noreply.github.com"
sync_prefix=release-check/sync
# The most verifying runs of one head that may end neither green nor red
# before the check stops dispatching more.
max_verify_dispatches=2

# The release tags the check considers (zerobull-consulting/dagger#117
# step 3), and what engines.sh takes as a version: a tag must match both.
tag_re='^v1\.[0-9]+\.[0-9]+(-[A-Za-z0-9.]+)?$'
version_re='^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+(\.[0-9A-Za-z]+)*)?$'
digest_re='^sha256:[0-9a-f]{64}$'
repo_re='^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$'
run_re='^https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/actions/runs/[0-9]+(/attempts/[0-9]+)?$'
sha_re='^[0-9a-f]{40}$'

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
need_sha() { [[ "$1" =~ $sha_re ]] || die "'$1' is not a commit id"; }

[[ "$repo" =~ $repo_re ]] || die "RELEASE_CHECK_REPO '$repo' is not owner/name"
[[ "$min_head_age" =~ ^[0-9]{1,6}$ ]] || die "RELEASE_CHECK_MIN_HEAD_AGE must be a number"

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

# tree: a fresh worktree of the branch's head (or of new_tree's argument), in
# $tree. scratch: files this run writes (PR and issue bodies, the advisory
# diff).
tree=""
scratch="$(mktemp -d)"
cleanup() {
	if [[ -n "$tree" ]]; then git worktree remove --force "$tree" >/dev/null 2>&1 || true; fi
	rm -rf "$scratch"
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

# push <branch>: pushes the worktree's HEAD to the branch, as the bot: a new
# branch, or a fast-forward of it (never forced: a head that moved refuses).
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
		die "pushing $to failed: has it moved since it was fetched, or is the bot's token missing, expired, or without access?"
}

# open_pr <head> <title> <body file>: opens a draft PR into the branch, as
# the bot, and prints its number.
open_pr() {
	local head="$1" title="$2" body="$3"
	if is_dry; then
		would "open a draft PR from $head: '$title', with this description:"
		sed 's/^/    | /' "$body" >&2
		echo 0
		return
	fi
	local pr
	pr="$(jq -n --arg t "$title" --arg h "$head" --arg b "$branch" --rawfile body "$body" \
		'{title: $t, head: $h, base: $b, body: $body, draft: true, maintainer_can_modify: true}' |
		bot_api -X POST "repos/$repo/pulls" --input - | jq -r .number)" ||
		die "opening the PR from $head failed: is the bot's token missing, expired, or without access?"
	need_number "$pr"
	say "opened $server/$repo/pull/$pr"
	echo "$pr"
}

# comment <issue or PR number> <body file>: a comment, with GITHUB_TOKEN.
comment() {
	if is_dry; then
		would "comment on #$1:"
		sed 's/^/    | /' "$2" >&2
		return
	fi
	jq -n --rawfile b "$2" '{body: $b}' | api -X POST "repos/$repo/issues/$1/comments" --input - >/dev/null ||
		die "commenting on #$1 failed"
}

sync_branch() { echo "$sync_prefix/$1"; }

# sync_prs: the open sync PRs into the branch, from this repository's own
# branches (a fork's PR may use the same name), as
# "<number> <version> <head sha> <draft>" lines.
sync_prs() {
	api "repos/$repo/pulls?state=open&base=$branch&per_page=100" |
		jq -r --arg r "$repo" --arg p "$sync_prefix/" '.[] | select(.head.repo.full_name == $r) | select(.head.ref | startswith($p)) |
			"\(.number) \(.head.ref | ltrimstr($p)) \(.head.sha) \(.draft // false)"'
}
# release_branches <version>: the release-check branches for the version,
# of any kind (the earlier flow's accept/rebuild/reject too).
release_branches() {
	api "repos/$repo/git/matching-refs/heads/release-check/" |
		jq -r --arg v "$1" '.[].ref | sub("^refs/heads/"; "") | select(test("^release-check/[a-z]+/") and (split("/") | .[2]) == $v)'
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
	[[ "$id" =~ ^[0-9]{1,20}$ ]] || die "dispatching $wf on $ref started a run the API didn't name"
	echo "$id"
}

# The jobs of remote-cache-tests.yml whose result is the engine's
# (engine_jobs, engine_json, is_engine, engine_jobs_unlisted).
# shellcheck source=engine-jobs.sh
source "$here/engine-jobs.sh"

# run_title <version> <digest>: a candidate run's title, from
# remote-cache-tests.yml's run-name: the runs API doesn't return a
# dispatch's inputs, so this is how a run is matched to what it verified.
run_title() { echo "Remote cache tests: candidate $1 $2"; }

# start_tests <version> <digest>: dispatches remote-cache-tests.yml on the
# sync branch with the candidate inputs. Prints the run's URL.
start_tests() {
	local v="$1" d="$2" ref id
	ref="$(sync_branch "$v")"
	if is_dry; then
		would "dispatch remote-cache-tests.yml on $ref with candidate $v $d"
		return
	fi
	id="$(dispatch remote-cache-tests.yml "$ref" "$(jq -n --arg v "$v" --arg d "$d" '{"candidate-version": $v, "candidate-digest": $d}')")"
	need_run "$server/$repo/actions/runs/$id"
	say "verifying $ref against $v: $server/$repo/actions/runs/$id"
	echo "$server/$repo/actions/runs/$id"
}

# run_jobs <run id>: the run's jobs (one page of 100), tried three times.
run_jobs() {
	local try jobs
	for try in 1 2 3; do
		if jobs="$(api "repos/$repo/actions/runs/$1/jobs?per_page=100")" && jq -e '.jobs | type == "array"' <<<"$jobs" >/dev/null 2>&1; then
			echo "$jobs"
			return
		fi
		((try == 3)) || sleep 5
	done
	return 1
}

# early_failure <run id>: the run's jobs that failed before testing anything
# (their checkout, or the pin step refusing the candidate), which says
# nothing about the code, comma-separated; nothing if none did. A jobs read
# that fails fails the plan.
early_failure() {
	local jobs
	jobs="$(run_jobs "$1")" || die "couldn't read the verifying run $server/$repo/actions/runs/$1's jobs: not deciding on it"
	jq -r '[.jobs[] | select(.conclusion == "failure") |
		select(any(.steps[]?; .conclusion == "failure" and (.name == "Checkout" or .name == "The engine pin, from the manifest"))) | .name] | join(", ")' <<<"$jobs"
}

# verdict <version> <digest> <sha>: the state of the sync branch's head
# from its verifying runs (dispatches of remote-cache-tests.yml titled for
# the candidate, at that commit). Prints "<state> [<run URL>]": green, red,
# running, verify or bounded. The latest run decides. Green is a green run
# whose engine jobs both ran and passed: a run whose engine jobs were
# skipped ends green too (zerobull-consulting/dagger#223), and decides
# nothing.
verdict() {
	local v="$1" d="$2" sha="$3" runs latest id status conclusion url jobs why undecided
	runs="$(api "repos/$repo/actions/workflows/remote-cache-tests.yml/runs?event=workflow_dispatch&head_sha=$sha&per_page=100")" ||
		die "couldn't read the verifying runs of ${sha:0:12}"
	jq -e '.workflow_runs | type == "array"' <<<"$runs" >/dev/null || die "the verifying runs of ${sha:0:12} aren't a list"
	runs="$(jq -c --arg t "$(run_title "$v" "$d")" '[.workflow_runs[] | select(.display_title == $t)] | sort_by(.id)' <<<"$runs")"
	if [[ "$(jq length <<<"$runs")" == 0 ]]; then
		echo verify
		return
	fi
	latest="$(jq -c '.[-1]' <<<"$runs")"
	id="$(jq -r .id <<<"$latest")"
	[[ "$id" =~ ^[0-9]{1,20}$ ]] || die "a verifying run's id isn't a number"
	status="$(jq -r .status <<<"$latest")"
	conclusion="$(jq -r '.conclusion // ""' <<<"$latest")"
	url="$server/$repo/actions/runs/$id"
	need_run "$url"
	if [[ "$status" != completed ]]; then
		echo "running $url"
		return
	fi
	case "$conclusion" in
	failure | timed_out)
		# verdict runs in a command substitution, where set -e doesn't
		# reach: a failed read exits by hand.
		why="$(early_failure "$id")" || exit 1
		if [[ -z "$why" ]]; then
			echo "red $url"
			return
		fi
		say "the verifying run $url failed before testing ($why): it decides nothing"
		;;
	success)
		if jobs="$(run_jobs "$id")"; then
			why="$(engine_jobs_unlisted "$jobs")"
			if [[ -z "$why" ]] && jq -e --argjson e "$engine_json" '[.jobs[] | select('"$is_engine"') | .conclusion] | all(. == "success")' <<<"$jobs" >/dev/null; then
				echo "green $url"
				return
			fi
			say "the verifying run $url is green, but its engine jobs didn't both run and pass${why:+ ($why)}: it decides nothing"
		else
			die "couldn't read the verifying run $url's jobs: not deciding on it"
		fi
		;;
	*) say "the verifying run $url ended '$conclusion': it decides nothing" ;;
	esac
	# The latest run of this head ended undecided (cancelled, say): try
	# again, a bounded number of times. Every completed run that ended
	# undecided counts; a red one doesn't, but one that failed before
	# testing does.
	local rid rconclusion early
	undecided=0
	while read -r rid rconclusion; do
		case "$rconclusion" in
		failure | timed_out)
			early="$(early_failure "$rid")" || exit 1
			[[ -z "$early" ]] || undecided=$((undecided + 1))
			;;
		*) undecided=$((undecided + 1)) ;;
		esac
	done < <(jq -r '.[] | select(.status == "completed") | "\(.id) \(.conclusion // "")"' <<<"$runs")
	if ((undecided >= max_verify_dispatches)); then
		echo "bounded $url"
	else
		echo verify
	fi
}

# failing_jobs <run id>: the run's failed jobs, as a Markdown list.
failing_jobs() {
	local jobs
	if jobs="$(run_jobs "$1")"; then
		jq -r '.jobs[] | select(.conclusion == "failure" or .conclusion == "timed_out") | "- [\(.name)](\(.html_url)): \(.conclusion)"' <<<"$jobs"
	else
		echo "- (the run's jobs couldn't be read: see the run)"
	fi
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
		-m "Merge upstream $tag into $(sync_branch "$tag") (release check)" "refs/tags/$tag" >&2; then
		echo merged
	else
		git -C "$tree" merge --abort >/dev/null 2>&1 || true
		echo conflict
	fi
}

bundle_const() {
	git show "$1:dagql/cache_transfer_types.go" 2>/dev/null | sed -n 's/^const valueBundleVersion = \([0-9][0-9]*\)$/\1/p'
}

# advisory <tag>: writes $scratch/advisory.md, the diff of the engine's side
# of the remote cache from the manifest's base (feature/remote-cache's, as
# checked out) to the tag, and whether the bundle or protocol version
# changed; sets reset=yes if either did. Advisory: the service also depends
# on engine.* events and session spans outside these paths, and the
# verifying run is the gate.
reset=""
advisory() {
	local tag="$1" base old new proto=no
	base="$("$engines" base)"
	fetch_tag "$base"
	fetch_tag "$tag"
	old="$(bundle_const "refs/tags/$base")"
	new="$(bundle_const "refs/tags/$tag")"
	# An unread version is never "unchanged": that would keep accept lines a
	# changed bundle format invalidates.
	[[ -n "$old" && -n "$new" ]] ||
		die "couldn't read dagql's valueBundleVersion in $base or $tag (dagql/cache_transfer_types.go): update bundle_const for its new form"
	local paths=(engine/remotecache cmd/engine/remotecache.go 'engine/server/remote_cache*.go' 'dagql/cache*.go' ':!**/*_test.go')
	git diff --quiet "refs/tags/$base" "refs/tags/$tag" -- engine/remotecache/protocol ':!**/*_test.go' || proto=yes
	reset=no
	[[ "$old" == "$new" && "$proto" == no ]] || reset=yes
	{
		echo "The engine's side of the remote cache, $base to $tag (advisory: the verifying run is the gate):"
		echo
		echo '```'
		git diff --stat "refs/tags/$base" "refs/tags/$tag" -- "${paths[@]}" || true
		echo '```'
		echo
		echo "- Bundle version (dagql's valueBundleVersion): $old in $base, $new in $tag."
		echo "- Protocol (engine/remotecache/protocol): $([[ "$proto" == yes ]] && echo changed || echo unchanged)."
	} >"$scratch/advisory.md"
}

# --- the subcommands ---

# The most dispatches of cacheservice-publish.yml for one head: a publish
# that failed is retried, but not forever (zerobull-consulting/dagger#166).
max_publish_dispatches=2

cmd_publish() {
	local head sha date age runs total ok active dispatched
	head="$(api "repos/$repo/branches/$branch")" || die "couldn't read $branch"
	sha="$(jq -r .commit.sha <<<"$head")"
	[[ "$sha" =~ $sha_re ]] || die "$branch's head isn't a commit id"
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

# plan_sync <PR> <version> <draft>: the open sync's state.
plan_sync() {
	local pr="$1" v="$2" draft="$3" sha d rc=0 ref state url accepted vout
	need_number "$pr"
	need_tag "$v"
	ref="$(sync_branch "$v")"
	d="$(image_digest "$v")" || rc=$?
	((rc != 3)) || die "$v's sync is open, but $engine_repo:$v is gone from the registry"
	((rc == 0)) || exit "$rc"
	out action sync
	out version "$v"
	out digest "$d"
	out pr "$pr"
	# The branch's head as git sees it, which may be newer than the PR's
	# listing.
	fetch_branch "$ref"
	fetch_tag "$v"
	sha="$(git rev-parse release-check/head)"
	need_sha "$sha"
	out sha "$sha"
	if ! git merge-base --is-ancestor "refs/tags/$v" release-check/head; then
		say "$ref (PR #$pr) doesn't contain $v yet: merge it into the branch by hand (git merge $v, resolve, push), and it is verified the next hour"
		out state needs-merge
		return
	fi
	accepted="$(mktemp -p "$scratch")"
	git show "release-check/head:$manifest_path" >"$accepted"
	if ENGINES_FILE="$accepted" "$engines" pin "$v" >/dev/null 2>&1; then
		say "$ref (PR #$pr) has $v's accept line: waiting for a human to merge it, with a merge commit"
		out state ready
		# Still a draft: the accept that pushed the line didn't get to mark
		# it ready.
		out draft "$draft"
		return
	fi
	# Apart from the read, so a verdict that fails fails the plan.
	vout="$(verdict "$v" "$d" "$sha")"
	read -r state url <<<"$vout"
	case "$state" in
	verify) say "$ref's head ${sha:0:12} has no verifying run yet: dispatching one" ;;
	running) say "$ref's head ${sha:0:12} is being verified: $url" ;;
	red) say "$ref's head ${sha:0:12} fails: $url. Fixes go in as PRs into $ref" ;;
	green) say "$ref's head ${sha:0:12} passes: $url. Adding the accept line" ;;
	bounded)
		say "::warning::$ref's head ${sha:0:12} has $max_verify_dispatches verifying runs that ended neither green nor red (the latest: $url). Not dispatching again: re-run it by hand."
		echo "$ref's head ${sha:0:12} has $max_verify_dispatches verifying runs that ended neither green nor red (the latest: $url). Re-run it by hand."
		;;
	*) die "unexpected verdict '$state'" ;;
	esac
	out state "$state"
	if [[ -n "$url" ]]; then out run "$url"; fi
}

cmd_plan() {
	local forced="${1:-}" listed newest t prs n pr v sha branches d
	if [[ -n "$forced" ]]; then
		# Starting a chosen release, listed or not, changes nothing only in
		# a dry run.
		is_dry || die "plan <version> is for dry runs only"
		need_tag "$forced"
	else
		# One sync at a time: an open one is all there is to do.
		prs="$(sync_prs)"
		n="$(grep -c . <<<"$prs" || true)"
		((n <= 1)) || die "more than one sync PR is open: $(awk '{ print "#" $1 }' <<<"$prs" | paste -sd' '). Close all but one"
		if ((n == 1)); then
			local draft
			read -r pr v sha draft <<<"$prs"
			plan_sync "$pr" "$v" "$draft"
			return
		fi
	fi
	listed="$("$engines" listed)"
	newest="$(vsort <<<"$listed" | tail -n1)"
	local -a tags
	if [[ -n "$forced" ]]; then
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
		branches="$(release_branches "$t")"
		if [[ -n "$branches" && -z "$forced" ]]; then
			die "$t has release-check branch(es) with no open sync PR: $(paste -sd' ' <<<"$branches"). A PR closed by hand, a run that stopped part way, or a branch of the earlier accept/rebuild/reject flow. Delete the branch(es) to sync $t, or add a line for it to the manifest by hand to skip it"
		fi
		local rc=0
		d="$(image_digest "$t")" || rc=$?
		if ((rc == 3)); then
			# Not skipped past: a newer release synced first would make
			# this one old, and it would never be checked.
			say "$t: no $engine_repo:$t image yet; waiting for it before anything newer"
			out action none
			return
		fi
		((rc == 0)) || exit "$rc"
		say "$t: new, image $d: starting its sync"
		out action start
		out version "$t"
		out digest "$d"
		return
	done
	out action none
}

pr_title() { echo "remote cache: sync upstream $1 (release check)"; }

cmd_start() {
	local v="$1" d="$2" ref merge old body pr run
	need_tag "$v"
	need_digest "$d"
	ref="$(sync_branch "$v")"
	fetch_tag "$v"
	new_tree
	merge="$(merge_tag "$v")"
	old="$(ENGINES_FILE="$tree/$manifest_path" "$engines" base)"
	advisory "$v"
	if [[ "$old" != "$v" ]]; then
		manifest_edit set-base "$v"
		git -C "$tree" add "$manifest_path"
		commit -m "remote cache: build on upstream $v" -m "Moves the manifest's base from $old (release check)."
	elif [[ "$merge" != merged ]]; then
		# Nothing to change (the tag merged already, or its merge left to a
		# human), but a PR needs a commit.
		commit --allow-empty -m "remote cache: sync upstream $v" -m "The manifest's base is $v already (release check)."
	fi
	push "$ref"
	body="$scratch/pr.md"
	{
		echo "Brings upstream \`$v\` (\`$engine_repo:$v@$d\`) into \`$branch\` (zerobull-consulting/dagger#117, D5)."
		echo
		case "$merge" in
		merged) echo "- Merges upstream \`$v\`, and moves the manifest's \`base\` from \`$old\` to \`$v\`." ;;
		ancestor) echo "- \`$branch\` contains \`$v\` already; this moves the manifest's \`base\` from \`$old\` to \`$v\`." ;;
		conflict)
			echo "- **Merging \`$v\` conflicts**, so this branch is \`$branch\` with only the \`base\` moved from \`$old\`. Merge the tag into it by hand, keeping a merge commit, then push:"
			echo
			echo '  ```sh'
			echo "  git fetch https://github.com/dagger/dagger tag $v"
			echo "  git switch $ref && git merge $v   # resolve, commit"
			echo "  git push origin $ref"
			echo '  ```'
			;;
		esac
		echo
		echo "**How this PR moves.** Each hour the release check verifies this branch's head: \`remote-cache-tests.yml\` dispatched on it with \`$v\` as the candidate engine (every job, \`TestBundleVersionMatchesEngineCapture\` among them)."
		echo "- **Red:** a \`compat-break\` issue points at the failing run, and this PR stays a draft. Fix it with PRs **into \`$ref\`**, squash-merged; each new head is verified again."
		echo "- **Green:** the release check adds \`$v\`'s accept line (citing the green run) and marks this PR ready."
		echo
		echo "**Merge it with a merge commit** (\`gh pr merge --merge\`), never a squash: upstream \`$v\` has to stay a parent, or \`fork paths\` keeps measuring from the old base and the next sync conflicts again. Newer releases wait until this merges, and are then verified against \`$branch\` with these fixes in."
		echo
		cat "$scratch/advisory.md"
		echo
		echo "See \`hack/remote-cache/README.md\`, \"Release check\"."
	} >"$body"
	pr="$(open_pr "$ref" "$(pr_title "$v")" "$body")"
	out pr "$pr"
	out merge "$merge"
	if [[ "$merge" == conflict ]]; then
		say "merging $v conflicts: $ref waits for a human to merge it; nothing to verify yet"
		return
	fi
	run="$(start_tests "$v" "$d")"
	if [[ -n "$run" ]]; then out run "$run"; fi
}

cmd_verify() {
	local v="$1" d="$2" run
	need_tag "$v"
	need_digest "$d"
	run="$(start_tests "$v" "$d")"
	if [[ -n "$run" ]]; then out run "$run"; fi
}

# link_issue <PR> <issue>: the PR closes the issue when it merges: its
# description gets "Closes #<issue>." unless it has it already.
link_issue() {
	local pr="$1" issue="$2" pbody
	pbody="$(api "repos/$repo/pulls/$pr" | jq -r '.body // ""')" || die "couldn't read #$pr"
	if grep -qF "Closes #$issue." <<<"$pbody"; then return; fi
	if is_dry; then
		would "add 'Closes #$issue.' to #$pr's description"
		return
	fi
	jq -n --arg b "$pbody"$'\n\n'"Closes #$issue." '{body: $b}' | api -X PATCH "repos/$repo/pulls/$pr" --input - >/dev/null ||
		die "linking #$issue from #$pr failed"
}

cmd_report() {
	local v="$1" run="$2" pr="$3" id issue title body seen
	need_tag "$v"
	need_run "$run"
	need_number "$pr"
	id="${run##*/actions/runs/}"
	id="${id%%/*}"
	issue="$(open_issue "$v")"
	title="$(issue_title "$v")"
	body="$scratch/issue.md"
	if [[ -z "$issue" ]]; then
		advisory "$v"
		{
			echo "Upstream \`$v\` fails against the cache service: its sync branch, \`$(sync_branch "$v")\` ($repo#$pr), is red (zerobull-consulting/dagger#117, D5)."
			echo
			echo "**Failing run:** $run"
			failing_jobs "$id"
			echo
			cat "$scratch/advisory.md"
			echo
			echo "Fix it with PRs into \`$(sync_branch "$v")\`, squash-merged. The release check verifies each new head hourly, comments here while it stays red, and once it's green adds the accept line and marks $repo#$pr ready; merging that closes this issue. Until then the service refuses \`$v\` engines, and no newer release is synced."
		} >"$body"
		if is_dry; then
			would "label compat-break and file '$title':"
			sed 's/^/    | /' "$body" >&2
			would "add 'Closes #<issue>' to #$pr's description"
			out issue 0
			return
		fi
		if ! api "repos/$repo/labels/compat-break" >/dev/null 2>&1; then
			api -X POST "repos/$repo/labels" -f name=compat-break -f color=b60205 \
				-f description="An upstream engine release the cache service fails against (release check)" >/dev/null ||
				die "creating the compat-break label failed"
		fi
		issue="$(jq -n --arg t "$title" --rawfile b "$body" '{title: $t, body: $b, labels: ["compat-break"]}' |
			api -X POST "repos/$repo/issues" --input - | jq -r .number)" || die "filing the compat-break issue failed"
		need_number "$issue"
		say "filed $server/$repo/issues/$issue"
		link_issue "$pr" "$issue"
		out issue "$issue"
		return
	fi
	need_number "$issue"
	out issue "$issue"
	# An issue filed by a run that stopped before linking it, or by the
	# earlier flow, is linked now.
	link_issue "$pr" "$issue"
	# Once per run: the issue's body or a comment names it already.
	seen="$( (
		api "repos/$repo/issues/$issue" | jq -r '.body // ""'
		api "repos/$repo/issues/$issue/comments?per_page=100" | jq -r '.[].body // ""'
	))" ||
		die "couldn't read #$issue and its comments"
	if grep -qF "$run" <<<"$seen"; then
		say "#$issue names $run already"
		return
	fi
	{
		echo "Still red after the latest change to \`$(sync_branch "$v")\` (or a re-run): $run"
		failing_jobs "$id"
	} >"$body"
	comment "$issue" "$body"
	say "commented on #$issue with $run"
}

# mark_ready <PR>: marks the PR ready for review, with GITHUB_TOKEN.
mark_ready() {
	local node
	if is_dry; then
		would "mark #$1 ready for review"
		return
	fi
	node="$(api "repos/$repo/pulls/$1" | jq -r .node_id)" || die "couldn't read #$1"
	# shellcheck disable=SC2016 # GraphQL variables, not shell
	api graphql -f query='mutation($id: ID!) { markPullRequestReadyForReview(input: {pullRequestId: $id}) { clientMutationId } }' \
		-f id="$node" >/dev/null || die "marking #$1 ready failed (the next hour tries again)"
	say "marked #$1 ready for review"
}

# cmd_ready <version> <PR>: what accept does after its push, for a sync
# whose accept line is pushed but whose PR is still a draft.
cmd_ready() {
	local v="$1" pr="$2" issue
	need_tag "$v"
	need_number "$pr"
	# A human may have marked it ready since the plan read it.
	if [[ "$(api "repos/$repo/pulls/$pr" | jq -r '.draft // false')" != true ]]; then
		say "#$pr is ready already"
		return
	fi
	mark_ready "$pr"
	echo "\`$(sync_branch "$v")\` has \`$v\`'s accept line, pushed by an earlier run that stopped before marking this ready. Merge it **with a merge commit** (\`gh pr merge --merge\`), never a squash, once its checks pass." >"$scratch/ready.md"
	comment "$pr" "$scratch/ready.md"
	issue="$(open_issue "$v")"
	if [[ -n "$issue" ]]; then
		need_number "$issue"
		echo "Green: #$pr has \`$v\`'s accept line and is ready to merge (with a merge commit), which closes this." >"$scratch/issue-ready.md"
		comment "$issue" "$scratch/issue-ready.md"
	fi
}

cmd_accept() {
	local v="$1" d="$2" run="$3" sha="$4" pr="$5" ref head issue body
	need_tag "$v"
	need_digest "$d"
	need_run "$run"
	need_sha "$sha"
	need_number "$pr"
	ref="$(sync_branch "$v")"
	new_tree "$ref"
	head="$(git -C "$tree" rev-parse HEAD)"
	[[ "$head" == "$sha" ]] ||
		die "$ref moved from ${sha:0:12}, which $run verified, to ${head:0:12}: the next hour verifies the new head"
	# The list resets if the bundle or protocol version changed since
	# $branch's base (the manifest as checked out).
	advisory "$v"
	if [[ "$reset" == yes ]]; then
		manifest_edit reset-accept "$v" "$d" "$run"
	else
		manifest_edit add-accept "$v" "$d" "$run"
	fi
	git -C "$tree" add "$manifest_path"
	commit -m "remote cache: accept engine $v" -m "Verified by $run (release check)."
	push "$ref"
	body="$scratch/green.md"
	{
		echo "\`$ref\` is green at \`${sha:0:12}\`: $run"
		echo
		if [[ "$reset" == yes ]]; then
			echo "Added \`$v\`'s accept line. **The bundle or protocol version changed**, so the accept list resets to \`$v\` alone; older builds keep serving older engines, and their \`dagger-remote-cache:<version>\` tags stay where they are."
		else
			echo "Added \`$v\`'s accept line, first; the other lines stay, and the release matrix verifies each of them again once this merges."
		fi
		echo
		echo "Ready to merge **with a merge commit** (\`gh pr merge --merge\`), never a squash, once its checks pass."
	} >"$body"
	mark_ready "$pr"
	comment "$pr" "$body"
	issue="$(open_issue "$v")"
	if [[ -n "$issue" ]]; then
		need_number "$issue"
		echo "Green: $run. #$pr is ready to merge (with a merge commit), which closes this." >"$scratch/issue-green.md"
		comment "$issue" "$scratch/issue-green.md"
	fi
	out accepted "$v"
}

if [[ "${1:-}" == --dry-run ]]; then
	dry=1
	shift
fi
if is_dry; then say "dry run: nothing is pushed, opened, filed, commented, marked or dispatched"; fi
case "${1:-}" in
publish) [[ $# -eq 1 ]] || die "usage: release-check.sh publish"; cmd_publish ;;
plan) [[ $# -le 2 ]] || die "usage: release-check.sh plan [<version>]"; cmd_plan "${2:-}" ;;
start) [[ $# -eq 3 ]] || die "usage: release-check.sh start <version> <digest>"; cmd_start "$2" "$3" ;;
verify) [[ $# -eq 3 ]] || die "usage: release-check.sh verify <version> <digest>"; cmd_verify "$2" "$3" ;;
report) [[ $# -eq 4 ]] || die "usage: release-check.sh report <version> <run URL> <PR>"; cmd_report "$2" "$3" "$4" ;;
ready) [[ $# -eq 3 ]] || die "usage: release-check.sh ready <version> <PR>"; cmd_ready "$2" "$3" ;;
accept) [[ $# -eq 6 ]] || die "usage: release-check.sh accept <version> <digest> <run URL> <sha> <PR>"; cmd_accept "$2" "$3" "$4" "$5" "$6" ;;
*) die "usage: release-check.sh [--dry-run] publish | plan [<version>] | start <version> <digest> | verify <version> <digest> | report <version> <run URL> <PR> | ready <version> <PR> | accept <version> <digest> <run URL> <sha> <PR>" ;;
esac
