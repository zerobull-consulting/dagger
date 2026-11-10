#!/usr/bin/env bash
# Tests of accept-line-only.sh, offline: a scratch repository whose branches
# are the PRs, and a fake gh for turning auto-merge off. Run it from
# anywhere:
#
#   hack/remote-cache/accept-line-only_test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
script="$here/accept-line-only.sh"
failures=0
check() {
	local what="$1"
	shift
	if "$@" >/dev/null; then
		echo "ok    $what"
	else
		echo "FAIL  $what"
		failures=$((failures + 1))
	fi
}
not() { ! "$@"; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
export GIT_CONFIG_GLOBAL="$tmp/gitconfig" GIT_CONFIG_NOSYSTEM=1
git config --global user.email t@example.com
git config --global user.name t

d1="sha256:$(printf '1%.0s' {1..64})"
d2="sha256:$(printf '2%.0s' {1..64})"
run1=https://github.com/o/r/actions/runs/1
run2=https://github.com/o/r/actions/runs/2
m=cmd/cacheservice/compat/engines.txt
repo="$tmp/repo"
mkdir -p "$repo/cmd/cacheservice/compat"
git -C "$repo" init -q -b main
printf '# c\nbase v1.0.0-beta.25\n\naccept v1.0.0-beta.25 %s verified: %s\n' "$d1" "$run1" >"$repo/$m"
echo x >"$repo/other.txt"
git -C "$repo" add -A && git -C "$repo" commit -q -m base
base="$(git -C "$repo" rev-parse HEAD)"

# pr <name> <script editing the checkout>: a branch off base with that
# change; prints its head.
pr() {
	git -C "$repo" checkout -q -B "$1" "$base"
	(cd "$repo" && eval "$2")
	git -C "$repo" add -A && git -C "$repo" commit -q -m "$1"
	git -C "$repo" rev-parse HEAD
}
line="accept v1.0.0-beta.26 $d2 verified: $run2"
first="sed -i 's|^accept v1.0.0-beta.25|$line\naccept v1.0.0-beta.25|' $m"
# A fake gh: the cited run (FAKE_RUN, a JSON file; FAKE_RUN_FAIL=1: the read
# fails), and the PR's auto-merge for disable-auto-merge.
mkdir -p "$tmp/bin"
cat >"$tmp/bin/gh" <<'G'
#!/usr/bin/env bash
echo "$*" >>"${FAKE_CALLS:-/dev/null}"
case "$2" in
repos/*/actions/runs/*)
	[[ -z "${FAKE_RUN_FAIL:-}" ]] || { echo "HTTP 404" >&2; exit 1; }
	cat "$FAKE_RUN" ;;
repos/*/pulls/*) echo "{\"node_id\": \"PR_1\", \"auto_merge\": $FAKE_AUTO_MERGE}" ;;
graphql) echo '{}' ;;
esac
G
chmod +x "$tmp/bin/gh"
export PATH="$tmp/bin:$PATH" GITHUB_REPOSITORY=o/r FAKE_CALLS="$tmp/calls"

# run <jq edit>: the cited run for run2/beta.26/d2, in the shape of a real
# one (beta.24's verifying run, 42336689501, was a successful workflow_dispatch
# of remote-cache-tests.yml on the branch remote-cache/release-check), edited.
run() {
	jq -n --arg t "Remote cache tests: candidate v1.0.0-beta.26 $d2" '{
		repository: {full_name: "o/r"}, head_repository: {full_name: "o/r"},
		path: ".github/workflows/remote-cache-tests.yml", event: "workflow_dispatch",
		status: "completed", conclusion: "success", head_branch: "remote-cache/release-check",
		display_title: $t}' | jq "${1:-.}" >"$tmp/run.json"
	export FAKE_RUN="$tmp/run.json"
}
run

# verdict <head> [<ref>]: runs the check; status in $status.
verdict() {
	(cd "$repo" && "$script" check "$base" "$1" "${2:-release-check/accept/v1.0.0-beta.26}") >"$tmp/out" 2>&1
	status=$?
}
passes() { verdict "$@"; ((status == 0)); }
fails() { verdict "$@"; ((status != 0)); }

check "passes: exactly one added accept line, first" passes "$(pr ok "$first")"
check "passes: any other PR is not checked" passes "$(pr other "echo y >other.txt")" feature/x
check "fails: another file too" fails "$(pr two-files "$first; echo y >other.txt")"
check "fails: only another file" fails "$(pr no-manifest "echo y >other.txt")"
check "fails: two added lines" fails "$(pr two-lines "$first; echo '# more' >>$m")"
check "fails: two accept lines" fails "$(pr two-accepts "sed -i 's|^accept v1.0.0-beta.25|$line\naccept v1.0.0-beta.27 $d2 verified: $run2\naccept v1.0.0-beta.25|' $m")"
check "fails: a changed line" fails "$(pr changed "sed -i 's|^base v1.0.0-beta.25|base v1.0.0-beta.26|' $m")"
check "fails: a changed line plus the accept line" fails "$(pr changed-and-added "$first; sed -i 's|^# c|# d|' $m")"
check "fails: a reject line" fails "$(pr reject "echo 'reject v1.0.0-beta.26 o/r#1' >>$m")"
check "fails: the accept line not first" fails "$(pr last "echo '$line' >>$m")"
check "fails: another version than the branch's" fails "$(pr wrong-version "$first")" release-check/accept/v1.0.0-beta.27
check "fails: a malformed branch version" fails "$(pr bad-ref "$first")" 'release-check/accept/v1;true'
check "fails: a bad digest" fails "$(pr bad-digest "sed -i 's|^accept v1.0.0-beta.25|accept v1.0.0-beta.26 sha256:abc verified: $run2\naccept v1.0.0-beta.25|' $m")"
check "fails: a run URL that isn't one" fails "$(pr bad-run "sed -i 's|^accept v1.0.0-beta.25|accept v1.0.0-beta.26 $d2 verified: https://example.com/1\naccept v1.0.0-beta.25|' $m")"
check "fails: extra spaces" fails "$(pr spaces "sed -i 's|^accept v1.0.0-beta.25|accept  v1.0.0-beta.26 $d2 verified: $run2\naccept v1.0.0-beta.25|' $m")"
check "fails: a mode change" fails "$(pr mode "$first; chmod +x $m")"
check "fails: a version already listed" fails "$(pr dup "sed -i 's|^accept v1.0.0-beta.25|accept v1.0.0-beta.25 $d2 verified: $run2\naccept v1.0.0-beta.25|' $m")" release-check/accept/v1.0.0-beta.25
check "fails: so engines.sh refuses it" grep -q "engines.sh refuses" "$tmp/out"
check "fails: a renamed manifest" fails "$(pr renamed "$first; git mv $m cmd/cacheservice/compat/e.txt")"
verdict "$(pr msg "echo y >other.txt")"
check "fails with an annotation naming the file" grep -q "::error file=$m::" "$tmp/out"

# The cited run (zerobull-consulting/dagger#165, lead 3). The fake gh serves
# $FAKE_RUN; the PR is always the good one, unless its line cites another URL.
okhead="$(pr run-ok "$first")"
withrun() { # <run URL>: a PR whose line cites it
	pr "run-$RANDOM" "sed -i 's|^accept v1.0.0-beta.25|accept v1.0.0-beta.26 $d2 verified: $1\naccept v1.0.0-beta.25|' $m"
}
run
check "run: a real run's shape passes (head_branch remote-cache/release-check)" passes "$okhead"
run '.head_branch = "feature/remote-cache"'
check "run: any head_branch passes (the release check dispatches on several)" passes "$okhead"
run
check "run: an /attempts/N URL reads that attempt" passes "$(withrun $run2/attempts/2)"
check "run: ... through the attempts endpoint" grep -q "repos/o/r/actions/runs/2/attempts/2" "$tmp/calls"
check "run: a run of another repository fails" fails "$(withrun https://github.com/x/y/actions/runs/2)"
check "run: ... and says so" grep -q "of x/y, not of o/r" "$tmp/out"
: >"$tmp/calls"
verdict "$(withrun https://github.com/x/y/actions/runs/2)"
check "run: ... without asking the API about it" not grep -q actions/runs "$tmp/calls"
run '.repository.full_name = "x/y"'
check "run: a run the API says belongs to another repository fails" fails "$okhead"
run '.head_repository.full_name = "x/fork"'
check "run: a run of a fork's code fails" fails "$okhead"
run '.conclusion = "failure"'
check "run: a failed run fails" fails "$okhead"
check "run: ... and says so" grep -q "completed/failure" "$tmp/out"
run '.conclusion = "cancelled"'
check "run: a cancelled run fails" fails "$okhead"
run '.status = "in_progress" | .conclusion = null'
check "run: a run still going fails" fails "$okhead"
run '.path = ".github/workflows/other.yml"'
check "run: another workflow fails" fails "$okhead"
run '.event = "push"'
check "run: a push run fails" fails "$okhead"
run '.event = "pull_request"'
check "run: a pull_request run fails" fails "$okhead"
run ".display_title = \"Remote cache tests: candidate v1.0.0-beta.27 $d2\""
check "run: a run for another version fails" fails "$okhead"
check "run: ... and says it didn't verify this version" grep -q "didn't verify this version and digest" "$tmp/out"
run ".display_title = \"Remote cache tests: candidate v1.0.0-beta.26 $d1\""
check "run: a run for another digest fails" fails "$okhead"
run '.display_title = "Remote cache tests"'
check "run: a run with the default title fails" fails "$okhead"
run ".display_title = \"Remote cache tests: candidate v1.0.0-beta.26 $d2 \""
check "run: a title with trailing text fails" fails "$okhead"
run 'del(.display_title)'
check "run: a run without a title fails" fails "$okhead"
run
export FAKE_RUN_FAIL=1
check "run: a run that can't be read fails" fails "$okhead"
unset FAKE_RUN_FAIL
echo 'not json' >"$tmp/run.json"
check "run: a reply that isn't JSON fails" fails "$okhead"
run
check "run: and a good run passes again" passes "$okhead"
# remote-cache-tests.yml titles a candidate's run the way verify_run reads it:
# take the format string from the workflow, fill it in, and cite that title.
fmt="$(sed -n "s/^run-name: .*format('\([^']*\)', inputs.candidate-version, inputs.candidate-digest).*/\1/p" "$here/../../.github/workflows/remote-cache-tests.yml")"
title="${fmt//\{0\}/v1.0.0-beta.26}"
title="${title//\{1\}/$d2}"
check "run-name: the workflow's format string was found" test -n "$fmt"
run ".display_title = \"$title\""
check "run-name: a run titled by the workflow's own run-name passes" passes "$okhead"
run
# Text the cited run controls can't start a workflow command of its own.
run '.display_title = "x\n::stop-commands::tok\n100%"'
verdict "$okhead"
check "annotation: a title with newlines still fails" test "$status" -ne 0
check "annotation: no output line is a command of the title's" not grep -q '^::stop' "$tmp/out"
check "annotation: and the newlines and % are escaped" grep -q 'x%0A::stop-commands::tok%0A100%25' "$tmp/out"
run
# GitHub's names don't care about case.
check "run: a URL differing from the repository in case passes" passes "$(withrun https://github.com/O/R/actions/runs/2)"
# Other PRs never read a run.
: >"$tmp/calls"
verdict "$(pr other2 "echo z >other.txt")" feature/x
check "run: a PR that isn't an accept PR reads no run" not grep -q actions/runs "$tmp/calls"

# fetch-head (zerobull-consulting/dagger#165, lead 1): a fork PR's head is on
# no branch of this repository, only under refs/pull/<N>/head. "up" is the
# base repository; each case clones it fresh, as actions/checkout does.
up="$tmp/up.git"
git init -q --bare -b main "$up"
git -C "$repo" push -q "$up" main
pushpr() { # <name> <ref>: a commit off base, pushed only to <ref>; prints it
	git -C "$repo" checkout -q -B "$1" "$base"
	echo "$1" >"$repo/fork.txt"
	git -C "$repo" add fork.txt && git -C "$repo" commit -q -m "$1"
	git -C "$repo" push -q "$up" "HEAD:$2"
	git -C "$repo" rev-parse HEAD
}
fresh() { rm -rf "$tmp/co" && git clone -q "file://$up" "$tmp/co"; }
fetchhead() { # <PR> <sha> [<ref>]
	(cd "$tmp/co" && FETCH_REMOTE=origin "$script" fetch-head "$1" "$2" "${3:-release-check/accept/v1.0.0-beta.26}") >"$tmp/out" 2>&1
}
have() { git -C "$tmp/co" cat-file -e "$1^{commit}"; }

fork="$(pushpr fork refs/pull/7/head)"
fresh
check "fetch-head: control: a fork's head isn't reachable from a plain clone" not git -C "$tmp/co" checkout -q --detach "$fork"
fresh
check "fetch-head: checks out a head that is only under refs/pull/<N>/head" fetchhead 7 "$fork"
check "fetch-head: and the commit is here" have "$fork"
check "fetch-head: and nothing was checked out" test "$(git -C "$tmp/co" rev-parse HEAD)" != "$fork"
inbranch="$(pushpr inbranch refs/heads/also-a-branch)"
git -C "$up" update-ref refs/pull/8/head "$inbranch"
fresh
check "fetch-head: a head that is on a branch too" fetchhead 8 "$inbranch"
check "fetch-head: and the commit is here (reachable)" have "$inbranch"
# The PR was pushed again: the ref is a child of the event's head.
git -C "$repo" checkout -q -B child "$fork"
echo c >"$repo/child.txt" && git -C "$repo" add child.txt && git -C "$repo" commit -q -m child
git -C "$repo" push -q "$up" HEAD:refs/pull/7/head
fresh
check "fetch-head: the ref moved on, the event's head is its ancestor" fetchhead 7 "$fork"
check "fetch-head: and the event's head is here" have "$fork"
# The PR was force-pushed: the ref no longer holds the event's head.
other="$(pushpr other refs/pull/9/head)"
fresh
check "fetch-head: fails when the ref doesn't hold the head" not fetchhead 9 "$fork"
check "fetch-head: and says the PR was pushed again" grep -q "pushed again" "$tmp/out"
# A head some branch holds is still refused when the PR's ref no longer does.
git -C "$up" update-ref refs/pull/10/head "$other"
fresh
check "fetch-head: fails for a head reachable from a branch, not from the ref" not fetchhead 10 "$inbranch"
check "fetch-head: fails for a PR with no such ref" not fetchhead 99 "$fork"
check "fetch-head: any other PR passes untouched, with no fetch" fetchhead 99 "$fork" feature/x
check "fetch-head: fails for a PR number that isn't one" not fetchhead '7;x' "$fork"
check "fetch-head: fails for a head that isn't a commit id" not fetchhead 7 main
# The token reaches git through its environment, not its arguments.
mkdir -p "$tmp/gitbin"
cat >"$tmp/gitbin/git" <<'G'
#!/usr/bin/env bash
echo "$*" >>"$FAKE_GIT_ARGS"
[[ "$1" == remote ]] && { echo https://example.invalid/o/r; exit 0; }
[[ "$1" == fetch ]] && { echo "extra=${GIT_CONFIG_KEY_0}" >>"$FAKE_GIT_ARGS"; exit 1; }
exec /usr/bin/env -i PATH="$ORIG_PATH" git "$@"
G
chmod +x "$tmp/gitbin/git"
(cd "$tmp/co" && PATH="$tmp/gitbin:$PATH" ORIG_PATH="$PATH" FAKE_GIT_ARGS="$tmp/gitargs" GH_TOKEN=tok-secret "$script" fetch-head 7 "$fork" release-check/accept/v1.0.0-beta.26) >"$tmp/out" 2>&1
check "fetch-head: uses the token through git's environment" grep -q "extra=http.https://example.invalid/.extraheader" "$tmp/gitargs"
check "fetch-head: never puts the token in git's arguments" not grep -q tok-secret "$tmp/gitargs"
check "fetch-head: never prints the token" not grep -q tok-secret "$tmp/out"

# The workflow takes the PR's head through fetch-head, not as the checkout's ref.
wf="$here/../../.github/workflows/remote-cache-auto-merge-add-compatible-engine.yml"
check "workflow: calls fetch-head with the PR number and head" grep -q 'accept-line-only.sh" fetch-head "\$PR" "\$HEAD_SHA" "\$HEAD_REF"' "$wf"
check "workflow: the checkout doesn't take the head as its ref" not grep -qE '^\s+ref: .*head\.sha' "$wf"

# disable-auto-merge, against the fake gh.
PATH="$tmp/bin:$PATH" FAKE_CALLS="$tmp/calls" FAKE_AUTO_MERGE='{"merge_method": "squash"}' "$script" disable-auto-merge 7 >"$tmp/out" 2>&1
check "disable: turns auto-merge off when it's on" grep -q disablePullRequestAutoMerge "$tmp/calls"
: >"$tmp/calls"
PATH="$tmp/bin:$PATH" FAKE_CALLS="$tmp/calls" FAKE_AUTO_MERGE=null "$script" disable-auto-merge 7 >"$tmp/out" 2>&1
check "disable: does nothing when it's off" not grep -q graphql "$tmp/calls"

echo
if ((failures > 0)); then
	echo "$failures failed"
	exit 1
fi
echo "all passed"
