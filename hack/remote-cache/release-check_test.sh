#!/usr/bin/env bash
# Tests of release-check.sh, offline: local git repositories stand in for
# upstream dagger/dagger (its tags) and for this fork (its branches), and
# fake gh and docker stand in for the GitHub API and the registry. Run it
# from anywhere:
#
#   hack/remote-cache/release-check_test.sh
set -uo pipefail
# The caller's own tokens never reach the script under test.
unset GITHUB_TOKEN BOT_TOKEN ACTIONS_ID_TOKEN_REQUEST_TOKEN

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
script="$here/release-check.sh"
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
has() { grep -qF -- "$2" "$1"; }
hasx() { grep -qE -- "$2" "$1"; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
export GIT_CONFIG_GLOBAL="$tmp/gitconfig" GIT_CONFIG_NOSYSTEM=1
git config --global user.email t@example.com
git config --global user.name t
git config --global init.defaultBranch main
git config --global advice.detachedHead false

d1="sha256:$(printf '1%.0s' {1..64})"
d2="sha256:$(printf '2%.0s' {1..64})"
d3="sha256:$(printf '3%.0s' {1..64})"
bot_secret="bot-secret-token-do-not-print"
gh_secret="gh-secret-token-do-not-print"
manifest=cmd/cacheservice/compat/engines.txt
v26=v1.0.0-beta.26
sync26=release-check/sync/$v26
title26="Remote cache tests: candidate $v26 $d2"

# --- fakes ---
mkdir -p "$tmp/bin"
# docker: imagetools inspect finds the tags in $FAKE/images ("<tag> <media>
# <digest>" lines), or fails with FAKE_REGISTRY_ERROR.
cat >"$tmp/bin/docker" <<'EOF'
#!/usr/bin/env bash
[[ "$1 $2 $3" == "buildx imagetools inspect" ]] || exit 1
[[ -n "${FAKE_REGISTRY_ERROR:-}" ]] && { echo "$FAKE_REGISTRY_ERROR" >&2; exit 1; }
tag="${@: -1}"; tag="${tag##*:}"
line="$(awk -v t="$tag" '$1 == t' "$FAKE/images" 2>/dev/null)"
[[ -n "$line" ]] || { echo "ERROR: registry.dagger.io/engine:$tag: not found" >&2; exit 1; }
echo "${line#* }"
EOF
# go: never called; upstream's code doesn't run here.
cat >"$tmp/bin/go" <<'EOF'
#!/usr/bin/env bash
echo "go $*" >>"$FAKE/go-calls"
EOF
# gh: the API calls the script makes, against JSON state in $FAKE. Every
# call is logged as "<who> <method> <path>", who being bot or gh by the
# token it carried.
cat >"$tmp/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == api ]] || exit 1
shift
method=GET path="" input="" fields=()
while (($#)); do
	case "$1" in
	-X) method="$2"; shift 2 ;;
	--input) input="$(cat)"; shift 2 ;;
	-f | -F) fields+=("$2"); shift 2 ;;
	*) path="$1"; shift ;;
	esac
done
[[ "$path" == graphql ]] && method=POST
who=other
[[ "${GH_TOKEN:-}" == "$BOT_SECRET" ]] && who=bot
[[ "${GH_TOKEN:-}" == "$GH_SECRET" ]] && who=gh
echo "$who $method $path" >>"$FAKE/calls"
n() { local f="$FAKE/next-$1" v; v=$(($(cat "$f" 2>/dev/null || echo 0) + 1)); echo "$v" >"$f"; echo "$v"; }
case "$method $path" in
"GET repos/o/r/pulls?state=open"*) jq '[.[] | select(.state == "open")]' "$FAKE/pulls.json" ;;
"POST repos/o/r/pulls")
	num=$(n pr)
	jq --argjson n "$num" --argjson in "$input" '. + [{number: $n, state: "open", title: $in.title, body: $in.body, draft: $in.draft,
		head: {ref: $in.head, sha: "0123456789012345678901234567890123456789", repo: {full_name: "o/r"}}, base: {ref: $in.base}, node_id: "PR_\($n)"}]' "$FAKE/pulls.json" >"$FAKE/p.tmp"
	mv "$FAKE/p.tmp" "$FAKE/pulls.json"
	echo "{\"number\": $num}" ;;
"GET repos/o/r/pulls/"*) jq --argjson n "${path##*/}" '.[] | select(.number == $n)' "$FAKE/pulls.json" ;;
"PATCH repos/o/r/pulls/"*)
	jq --argjson n "${path##*/}" --argjson in "$input" 'map(if .number == $n then .body = $in.body else . end)' "$FAKE/pulls.json" >"$FAKE/p.tmp"
	mv "$FAKE/p.tmp" "$FAKE/pulls.json"
	echo '{}' ;;
"GET repos/o/r/git/matching-refs/heads/release-check/")
	git -C "$FAKE/origin.git" for-each-ref --format='%(refname)' refs/heads/release-check/ | jq -R . | jq -s 'map({ref: .})' ;;
"GET repos/o/r/issues?labels=compat-break"*) jq '[.[] | select(.state == "open")]' "$FAKE/issues.json" ;;
"GET repos/o/r/issues/"*/comments*)
	num="${path#repos/o/r/issues/}"; num="${num%%/*}"
	jq --argjson n "$num" '[.[] | select(.on == $n)]' "$FAKE/comments.json" ;;
"POST repos/o/r/issues/"*/comments)
	num="${path#repos/o/r/issues/}"; num="${num%%/*}"
	jq --argjson n "$num" --argjson in "$input" '. + [{on: $n, body: $in.body}]' "$FAKE/comments.json" >"$FAKE/c.tmp"
	mv "$FAKE/c.tmp" "$FAKE/comments.json"
	echo '{}' ;;
"GET repos/o/r/issues/"*) jq --argjson n "${path##*/}" '.[] | select(.number == $n)' "$FAKE/issues.json" ;;
"GET repos/o/r/labels/compat-break") [[ -f "$FAKE/label" ]] || { echo '{"message":"Not Found"}'; exit 1; } ;;
"POST repos/o/r/labels") touch "$FAKE/label"; echo '{}' ;;
"POST repos/o/r/issues")
	num=$((100 + $(n issue)))
	jq --argjson n "$num" --argjson in "$input" '. + [{number: $n, state: "open", title: $in.title, body: $in.body, labels: $in.labels}]' \
		"$FAKE/issues.json" >"$FAKE/i.tmp"
	mv "$FAKE/i.tmp" "$FAKE/issues.json"
	echo "{\"number\": $num}" ;;
"POST repos/o/r/actions/workflows/"*/dispatches)
	wf="${path#repos/o/r/actions/workflows/}"; wf="${wf%/dispatches}"
	echo "$input" >"$FAKE/dispatch-$wf-$(n dispatch-$wf)"
	[[ -n "${FAKE_NO_RUN_ID:-}" ]] && exit 0
	id=$((5000 + $(n run)))
	echo "{\"workflow_run_id\": $id, \"run_url\": \"x\", \"html_url\": \"x\"}" ;;
"GET repos/o/r/actions/workflows/remote-cache-tests.yml/runs?"*)
	# $FAKE/runs.json: [{id, head_sha, display_title, status, conclusion}].
	sha="${path#*head_sha=}"; sha="${sha%%&*}"
	jq --arg s "$sha" '{workflow_runs: [.[] | select(.head_sha == $s)]}' "$FAKE/runs.json" ;;
"GET repos/o/r/actions/runs/"*/jobs*)
	[[ -n "${FAKE_JOBS_FAIL:-}" ]] && { echo "HTTP 502" >&2; exit 1; }
	# FAKE_JOBS: the jobs array as it is; otherwise the four jobs of a
	# candidate run, the e2e red in a red run.
	if [[ -n "${FAKE_JOBS:-}" ]]; then echo "{\"jobs\": $FAKE_JOBS}"; exit 0; fi
	rid="${path#repos/o/r/actions/runs/}"; rid="${rid%%/*}"
	c="$(jq -r --argjson i "$rid" '.[] | select(.id == $i) | .conclusion' "$FAKE/runs.json")"
	e2e=success; [[ "$c" == failure ]] && e2e=failure
	echo '{"jobs": [{"name": "unit tests", "conclusion": "success", "html_url": "https://github.com/o/r/actions/runs/1/job/4"},
		{"name": "S3 tests against MinIO", "conclusion": "success", "html_url": "https://github.com/o/r/actions/runs/1/job/5"},
		{"name": "real engines (e2e)", "conclusion": "'"$e2e"'", "html_url": "https://github.com/o/r/actions/runs/1/job/2"},
		{"name": "sidecar demo", "conclusion": "success", "html_url": "https://github.com/o/r/actions/runs/1/job/3"}]}' ;;
"GET repos/o/r/branches/feature/remote-cache")
	sha="$(git -C "$FAKE/origin.git" rev-parse refs/heads/feature/remote-cache)"
	echo "{\"commit\": {\"sha\": \"$sha\", \"commit\": {\"committer\": {\"date\": \"${FAKE_HEAD_DATE:-2020-01-01T00:00:00Z}\"}}}}" ;;
"GET repos/o/r/actions/workflows/cacheservice-publish.yml/runs?head_sha="*)
	# FAKE_PUBLISH_RUNS: space-separated "<event>:<status>:<conclusion>".
	jq -n --arg r "${FAKE_PUBLISH_RUNS:-}" '{workflow_runs: [$r | split(" ")[] | select(. != "") | split(":") | {event: .[0], status: .[1], conclusion: (.[2] // null)}]}' ;;
"POST graphql") printf '%s\n' "${fields[@]}" >"$FAKE/graphql"; echo '{}' ;;
*) echo "fake gh: unexpected $method $path" >&2; exit 1 ;;
esac
EOF
chmod +x "$tmp/bin/"*

# --- upstream: tags beta.24 to beta.28 on main, plus tags the check must
# skip. Each release changes a file the fork doesn't touch; beta.28 changes
# the bundle version. ---
up="$tmp/upstream"
mkdir -p "$up/dagql" "$up/engine/remotecache/protocol"
git -C "$up" init -q
echo 'const valueBundleVersion = 3' >"$up/dagql/cache_transfer_types.go"
echo 'package protocol' >"$up/engine/remotecache/protocol/protocol.go"
for v in 24 25; do
	echo "$v" >"$up/release"
	git -C "$up" add -A && git -C "$up" commit -q -m "beta.$v"
	git -C "$up" tag "v1.0.0-beta.$v"
done
git -C "$up" tag -a -m sdk "sdk/go/v1.0.0-beta.26"
git -C "$up" tag v0.19.9
git -C "$up" tag v1.0.0-beta_x
echo 26 >"$up/release"
echo 'func changed() {}' >>"$up/dagql/cache_live.go"
git -C "$up" add -A && git -C "$up" commit -q -m beta.26
git -C "$up" tag -a -m beta.26 v1.0.0-beta.26
echo 27 >"$up/release"
git -C "$up" add -A && git -C "$up" commit -q -m beta.27
git -C "$up" tag v1.0.0-beta.27
echo 28 >"$up/release"
echo 'const valueBundleVersion = 7' >"$up/dagql/cache_transfer_types.go"
git -C "$up" add -A && git -C "$up" commit -q -m beta.28
git -C "$up" tag v1.0.0-beta.28

# --- the fork: feature/remote-cache, upstream beta.25 plus the service ---
fork_manifest() {
	echo "# the manifest"
	echo "base v1.0.0-beta.25"
	echo "accept v1.0.0-beta.25 $d1 verified: https://github.com/o/r/actions/runs/1"
}
setup() {
	rm -rf "$tmp/fake" "$tmp/work"
	export FAKE="$tmp/fake"
	mkdir -p "$FAKE"
	echo '[]' >"$FAKE/pulls.json"
	echo '[]' >"$FAKE/issues.json"
	echo '[]' >"$FAKE/comments.json"
	echo '[]' >"$FAKE/runs.json"
	: >"$FAKE/calls"
	printf '%s application/vnd.oci.image.index.v1+json %s\n' "$v26" "$d2" >"$FAKE/images"
	git clone -q --bare "$up" "$FAKE/origin.git"
	git -C "$FAKE/origin.git" tag -l | xargs -r git -C "$FAKE/origin.git" tag -d >/dev/null
	work="$tmp/work"
	git clone -q "$FAKE/origin.git" "$work"
	git -C "$work" checkout -q -b feature/remote-cache "$(git -C "$up" rev-parse v1.0.0-beta.25)"
	mkdir -p "$work/cmd/cacheservice/compat"
	fork_manifest >"$work/$manifest"
	git -C "$work" add -A && git -C "$work" commit -q -m "the service"
	git -C "$work" push -q origin feature/remote-cache
	git -C "$work" tag -l | xargs -r git -C "$work" tag -d >/dev/null
}
# rc <args...>: runs the script in the checkout, as the workflow does;
# output in $tmp/out (stdout and stderr), status in $status.
rc() {
	rm -f "$FAKE/output"
	(
		cd "$work" &&
			PATH="$tmp/bin:$PATH" BOT_SECRET="$bot_secret" GH_SECRET="$gh_secret" GH_TOKEN="$gh_secret" \
				ENGINES_FILE="$work/$manifest" RELEASE_CHECK_REPO=o/r RELEASE_CHECK_REMOTE="$FAKE/origin.git" \
				RELEASE_CHECK_UPSTREAM="$up" GITHUB_SERVER_URL=https://github.com GITHUB_OUTPUT="$FAKE/output" \
				"$@" "$script" "${ARGS[@]}"
	) >"$tmp/out" 2>&1
	status=$?
}
run() { ARGS=("$@"); rc env; }
run_bot() { ARGS=("$@"); rc env BOT_TOKEN="$bot_secret"; }
writes() { grep -cE ' (POST|PATCH|PUT|DELETE) ' "$FAKE/calls" || true; }
branch_file() { git -C "$FAKE/origin.git" show "refs/heads/$1:$manifest"; }
branch_sha() { git -C "$FAKE/origin.git" rev-parse "refs/heads/$1"; }
no_secret() { not grep -qE "$bot_secret|$gh_secret" "$tmp/out"; }
output() { awk -F= -v k="$1" '$1 == k { v = substr($0, length(k) + 2) } END { print v }' "$FAKE/output"; }
# add_run <id> <head sha> <title> <status> [<conclusion>]: a verifying run.
add_run() {
	jq --argjson i "$1" --arg s "$2" --arg t "$3" --arg st "$4" --arg c "${5:-}" \
		'. + [{id: $i, head_sha: $s, display_title: $t, status: $st, conclusion: (if $c == "" then null else $c end)}]' \
		"$FAKE/runs.json" >"$FAKE/r.tmp" && mv "$FAKE/r.tmp" "$FAKE/runs.json"
}
# started: setup, then beta.26's sync started (its PR is #1).
started() {
	setup
	run_bot start "$v26" "$d2"
	[[ "$status" == 0 ]] || { cat "$tmp/out"; echo "start failed"; exit 1; }
	: >"$FAKE/calls"
	rm -f "$FAKE"/dispatch-* "$FAKE"/next-dispatch-*
}
job() { printf '{"name": "%s", "conclusion": "%s", "html_url": "https://github.com/o/r/actions/runs/1/job/9"}' "$1" "$2"; }
jobs_of() { local IFS=,; echo "[$*]"; }

echo "--- plan, nothing open"
setup
run plan
check "plan: the oldest new release with an image starts a sync" has "$tmp/out" "action=start"
check "plan: it is beta.26, not beta.24 (older than the manifest's lines) or beta.27" has "$tmp/out" "version=$v26"
check "plan: with its index digest" has "$tmp/out" "digest=$d2"
check "plan: sdk/*, v0.* and malformed tags are skipped" not hasx "$tmp/out" "sdk/|v0\.19|beta_x"
check "plan: writes nothing" test "$(writes)" = 0
check "plan: outputs reach GITHUB_OUTPUT" has "$FAKE/output" "action=start"

: >"$FAKE/images"
run plan
check "plan: a release with no image yet is skipped this hour" has "$tmp/out" "no registry.dagger.io/engine:$v26 image yet"
check "plan: and nothing is done" has "$tmp/out" "action=none"

printf 'v1.0.0-beta.27 application/vnd.oci.image.index.v1+json %s\n' "$d2" >"$FAKE/images"
run plan
check "plan: a newer release with an image waits for an older one without" has "$tmp/out" "action=none"
check "plan: so the older one is never left behind" not has "$tmp/out" "version=v1.0.0-beta.27"

ARGS=(plan); rc env RELEASE_CHECK_UPSTREAM="$tmp/no-such-repo"
check "plan: an upstream that can't be listed fails the run" test "$status" -ne 0
check "plan: rather than reading as nothing new" not has "$tmp/out" "action=none"

printf '%s application/vnd.oci.image.manifest.v1+json %s\n' "$v26" "$d2" >"$FAKE/images"
run plan
check "plan: a single-platform image fails the run" test "$status" -ne 0

printf '%s application/vnd.oci.image.index.v1+json sha256:abc\n' "$v26" >"$FAKE/images"
run plan
check "plan: a malformed digest from the registry fails the run" test "$status" -ne 0

ARGS=(plan); rc env FAKE_REGISTRY_ERROR="unexpected status 503"
check "plan: a registry error fails the run, rather than skipping" test "$status" -ne 0

run plan v1.0.0-beta.24
check "plan: a chosen version is for dry runs only" test "$status" -ne 0
setup
printf 'v1.0.0-beta.27 application/vnd.oci.image.index.v1+json %s\n' "$d3" >>"$FAKE/images"
run --dry-run plan v1.0.0-beta.27
check "plan: a dry run may choose a version" has "$tmp/out" "version=v1.0.0-beta.27"

setup
git -C "$work" push -q origin "feature/remote-cache:refs/heads/release-check/reject/$v26"
run plan
check "plan: a leftover branch of the earlier flow fails the run" test "$status" -ne 0
check "plan: naming it" has "$tmp/out" "release-check/reject/$v26"

echo "--- start"
setup
run_bot start "$v26" "$d2"
check "start: succeeds" test "$status" = 0
check "start: pushes the sync branch" branch_sha "$sync26"
check "start: with the tag merged" git -C "$FAKE/origin.git" merge-base --is-ancestor "$(git -C "$up" rev-parse "$v26^{commit}")" "refs/heads/$sync26"
check "start: by a merge commit, on feature/remote-cache" test "$(git -C "$FAKE/origin.git" rev-parse "refs/heads/$sync26~1^1")" = "$(branch_sha feature/remote-cache)"
check "start: and the base moved" bash -c "git -C '$FAKE/origin.git' show 'refs/heads/$sync26:$manifest' | grep -qx 'base $v26'"
check "start: the accept lines unchanged" bash -c "git -C '$FAKE/origin.git' show 'refs/heads/$sync26:$manifest' | grep -c '^accept' | grep -qx 1"
check "start: says it merged" has "$tmp/out" "merge=merged"
check "start: opens the PR as the bot" has "$FAKE/calls" "bot POST repos/o/r/pulls"
check "start: a draft" jq -e '.[0].draft == true and .[0].head.ref == "'"$sync26"'" and .[0].base.ref == "feature/remote-cache"' "$FAKE/pulls.json"
check "start: titled for the sync" jq -e '.[0].title == "remote cache: sync upstream '"$v26"' (release check)"' "$FAKE/pulls.json"
check "start: its description says to merge with a merge commit" jq -e '.[0].body | test("gh pr merge --merge")' "$FAKE/pulls.json"
check "start: with the advisory diff" jq -e '.[0].body | test("dagql/cache_live.go")' "$FAKE/pulls.json"
check "start: prints the PR" has "$tmp/out" "pr=1"
check "start: dispatches the first verification on the sync branch" \
	jq -e --arg d "$d2" '.ref == "'"$sync26"'" and .inputs == {"candidate-version": "'"$v26"'", "candidate-digest": $d}' "$FAKE/dispatch-remote-cache-tests.yml-1"
check "start: with GITHUB_TOKEN" has "$FAKE/calls" "gh POST repos/o/r/actions/workflows/remote-cache-tests.yml/dispatches"
check "start: and prints its run" has "$tmp/out" "run=https://github.com/o/r/actions/runs/5001"
check "start: never runs upstream's code" not test -f "$FAKE/go-calls"
check "start: never prints a token" no_secret

setup
run start "$v26" "$d2"
check "start: without the bot's token, fails" test "$status" -ne 0
check "start: saying so" has "$tmp/out" "BOT_TOKEN is not set"
check "start: pushing nothing" test -z "$(git -C "$FAKE/origin.git" for-each-ref refs/heads/release-check/)"

setup
echo fork >"$work/release"
git -C "$work" commit -qam "the fork changes upstream's file"
git -C "$work" push -q origin feature/remote-cache
run_bot start "$v26" "$d2"
check "start: a conflicting tag still starts the sync" test "$status" = 0
check "start: says it conflicts" has "$tmp/out" "merge=conflict"
check "start: the branch is feature/remote-cache with the base moved" test "$(git -C "$FAKE/origin.git" rev-parse "refs/heads/$sync26~1")" = "$(branch_sha feature/remote-cache)"
check "start: without the tag" not git -C "$FAKE/origin.git" merge-base --is-ancestor "$(git -C "$up" rev-parse "$v26^{commit}")" "refs/heads/$sync26"
check "start: the PR says how to merge it by hand" jq -e '.[0].body | test("Merging `'"$v26"'` conflicts")' "$FAKE/pulls.json"
check "start: and nothing is verified yet" not test -f "$FAKE/dispatch-remote-cache-tests.yml-1"

echo "--- plan, a sync open"
started
sha="$(branch_sha "$sync26")"
run plan
check "plan: an open sync is followed" has "$tmp/out" "action=sync"
check "plan: its version, PR and head" bash -c "grep -qx 'version=$v26' '$tmp/out' && grep -qx 'pr=1' '$tmp/out' && grep -qx 'sha=$sha' '$tmp/out'"
check "plan: with no run of its head, verify" has "$tmp/out" "state=verify"
check "plan: writes nothing" test "$(writes)" = 0

printf 'v1.0.0-beta.27 application/vnd.oci.image.index.v1+json %s\n' "$d3" >>"$FAKE/images"
run plan
check "plan: a newer release waits for the open sync" not has "$tmp/out" "action=start"

add_run 7001 "$sha" "$title26" in_progress
run plan
check "plan: a run in progress is waited on" has "$tmp/out" "state=running"
check "plan: and named" has "$tmp/out" "run=https://github.com/o/r/actions/runs/7001"

echo '[]' >"$FAKE/runs.json"
add_run 7002 "$sha" "$title26" completed failure
run plan
check "plan: a red run is red" has "$tmp/out" "state=red"
check "plan: naming it" has "$tmp/out" "run=https://github.com/o/r/actions/runs/7002"

add_run 7003 "$sha" "$title26" completed success
run plan
check "plan: the latest run decides: green" has "$tmp/out" "state=green"
check "plan: naming it" has "$tmp/out" "run=https://github.com/o/r/actions/runs/7003"

echo '[]' >"$FAKE/runs.json"
add_run 7004 "0000000000000000000000000000000000000000" "$title26" completed success
add_run 7005 "$sha" "Remote cache tests: candidate $v26 $d3" completed success
run plan
check "plan: runs of another head, or another digest, don't count" has "$tmp/out" "state=verify"

echo '[]' >"$FAKE/runs.json"
add_run 7006 "$sha" "$title26" completed success
ARGS=(plan); rc env FAKE_JOBS="$(jobs_of "$(job 'unit tests' success)" "$(job 'real engines (e2e)' skipped)" "$(job 'sidecar demo' success)")"
check "plan: a green run whose engine job was skipped decides nothing" has "$tmp/out" "state=verify"
add_run 7007 "$sha" "$title26" completed cancelled
run plan
check "plan: two undecided runs of one head stop the dispatches" has "$tmp/out" "state=bounded"
check "plan: saying so" has "$tmp/out" "Re-run it by hand"

echo '[]' >"$FAKE/runs.json"
add_run 7011 "$sha" "$title26" completed failure
add_run 7012 "$sha" "$title26" completed cancelled
run plan
check "plan: a red run doesn't count toward the bound" has "$tmp/out" "state=verify"

echo '[]' >"$FAKE/runs.json"
add_run 7013 "$sha" "$title26" completed failure
early='{"name": "real engines (e2e)", "conclusion": "failure", "html_url": "x", "steps": [{"name": "Checkout", "conclusion": "failure"}]}'
ARGS=(plan); rc env FAKE_JOBS="$(jobs_of "$(job 'unit tests' success)" "$early" "$(job 'sidecar demo' success)")"
check "plan: a run that failed before testing isn't red" has "$tmp/out" "state=verify"
check "plan: saying why" has "$tmp/out" "failed before testing (real engines (e2e))"
add_run 7014 "$sha" "$title26" completed failure
ARGS=(plan); rc env FAKE_JOBS="$(jobs_of "$(job 'unit tests' success)" "$early" "$(job 'sidecar demo' success)")"
check "plan: two runs that failed before testing stop the dispatches" has "$tmp/out" "state=bounded"
ARGS=(plan); rc env FAKE_JOBS_FAIL=1
check "plan: a red run whose jobs can't be read fails the plan" test "$status" -ne 0

echo '[]' >"$FAKE/runs.json"
add_run 7008 "$sha" "$title26" completed success
ARGS=(plan); rc env FAKE_JOBS_FAIL=1
check "plan: a green run whose jobs can't be read fails the plan" test "$status" -ne 0

jq '. + [.[0] | .number = 2]' "$FAKE/pulls.json" >"$FAKE/p.tmp" && mv "$FAKE/p.tmp" "$FAKE/pulls.json"
run plan
check "plan: two open syncs fail the run" test "$status" -ne 0

setup
echo fork >"$work/release"
git -C "$work" commit -qam "the fork changes upstream's file"
git -C "$work" push -q origin feature/remote-cache
run_bot start "$v26" "$d2"
run plan
check "plan: a sync without the tag waits for a human merge" has "$tmp/out" "state=needs-merge"
git -C "$work" fetch -q origin "$sync26" && git -C "$work" checkout -q -b s FETCH_HEAD
git -C "$work" fetch -q "$up" "refs/tags/$v26:refs/tags/$v26"
git -C "$work" merge -q "$v26" >/dev/null 2>&1; echo fork >"$work/release"; git -C "$work" commit -qam "merge $v26" && git -C "$work" push -q origin "s:$sync26"
git -C "$work" checkout -q feature/remote-cache
run plan
check "plan: once a human merged it, it is verified" has "$tmp/out" "state=verify"

echo "--- verify"
started
run verify "$v26" "$d2"
check "verify: dispatches on the sync branch" jq -e '.ref == "'"$sync26"'"' "$FAKE/dispatch-remote-cache-tests.yml-1"
check "verify: and returns" has "$tmp/out" "run=https://github.com/o/r/actions/runs/500"

echo "--- report"
started
sha="$(branch_sha "$sync26")"
add_run 7002 "$sha" "$title26" completed failure
run report "$v26" https://github.com/o/r/actions/runs/7002 1
check "report: files the compat-break issue" jq -e '.[0].title == "compat-break: engine '"$v26"' fails against the cache service" and .[0].labels == ["compat-break"]' "$FAKE/issues.json"
check "report: with the failing run" jq -e '.[0].body | test("actions/runs/7002")' "$FAKE/issues.json"
check "report: its failing jobs" jq -e '.[0].body | test("real engines \\(e2e\\)")' "$FAKE/issues.json"
check "report: and the sync PR" jq -e '.[0].body | test("o/r#1")' "$FAKE/issues.json"
check "report: with GITHUB_TOKEN" has "$FAKE/calls" "gh POST repos/o/r/issues"
check "report: the PR closes it" jq -e '.[0].body | test("Closes #101.")' "$FAKE/pulls.json"
check "report: prints it" has "$tmp/out" "issue=101"
run report "$v26" https://github.com/o/r/actions/runs/7002 1
check "report: the same run again is said once" has "$tmp/out" "names https://github.com/o/r/actions/runs/7002 already"
check "report: no second issue, no comment" bash -c "jq -e 'length == 1' '$FAKE/issues.json' && jq -e 'length == 0' '$FAKE/comments.json'"
add_run 7010 "$sha" "$title26" completed failure
run report "$v26" https://github.com/o/r/actions/runs/7010 1
check "report: a new red run is a comment" jq -e '.[0].on == 101 and (.[0].body | test("actions/runs/7010"))' "$FAKE/comments.json"
run report "$v26" https://github.com/o/r/actions/runs/7010 1
check "report: once" jq -e 'length == 1' "$FAKE/comments.json"
check "report: the PR names the issue once" test "$(jq -r '.[0].body' "$FAKE/pulls.json" | grep -c 'Closes #101.')" = 1

started
echo '[{"number": 90, "state": "open", "title": "compat-break: engine '"$v26"' fails against the cache service", "body": "x"}]' >"$FAKE/issues.json"
run report "$v26" https://github.com/o/r/actions/runs/7002 1
check "report: an issue already open is reused" jq -e 'length == 1' "$FAKE/issues.json"
check "report: and linked from the PR" jq -e '.[0].body | test("Closes #90.")' "$FAKE/pulls.json"

echo "--- accept"
started
sha="$(branch_sha "$sync26")"
echo '[{"number": 101, "state": "open", "title": "compat-break: engine '"$v26"' fails against the cache service", "body": "x"}]' >"$FAKE/issues.json"
run_bot accept "$v26" "$d2" https://github.com/o/r/actions/runs/7003 "$sha" 1
check "accept: succeeds" test "$status" = 0
check "accept: pushes the accept line, first, to the sync branch" bash -c "git -C '$FAKE/origin.git' show 'refs/heads/$sync26:$manifest' | grep '^accept' | head -n1 | grep -qx 'accept $v26 $d2 verified: https://github.com/o/r/actions/runs/7003'"
check "accept: on top of the verified head" test "$(git -C "$FAKE/origin.git" rev-parse "refs/heads/$sync26~1")" = "$sha"
check "accept: keeps the other lines (same bundle version)" bash -c "git -C '$FAKE/origin.git' show 'refs/heads/$sync26:$manifest' | grep -q '^accept v1.0.0-beta.25 '"
check "accept: marks the PR ready, with GITHUB_TOKEN" bash -c "grep -q 'gh POST graphql' '$FAKE/calls' && grep -q 'markPullRequestReadyForReview' '$FAKE/graphql' && grep -qx 'id=PR_1' '$FAKE/graphql'"
check "accept: says so on the PR" jq -e 'any(.[]; .on == 1 and (.body | test("merge commit")))' "$FAKE/comments.json"
check "accept: and on the issue" jq -e 'any(.[]; .on == 101 and (.body | test("actions/runs/7003")))' "$FAKE/comments.json"
check "accept: never prints a token" no_secret
run plan
check "plan: a sync with its accept line waits for a human" has "$tmp/out" "state=ready"
check "plan: and says the PR is still a draft (the fake doesn't change it)" has "$tmp/out" "draft=true"
rm -f "$FAKE/graphql"
echo '[]' >"$FAKE/comments.json"
run ready "$v26" 1
check "ready: marks a draft ready" bash -c "grep -q 'markPullRequestReadyForReview' '$FAKE/graphql' && grep -qx 'id=PR_1' '$FAKE/graphql'"
check "ready: says so on the PR" jq -e 'any(.[]; .on == 1 and (.body | test("merge commit")))' "$FAKE/comments.json"
check "ready: and on the issue" jq -e 'any(.[]; .on == 101 and (.body | test("ready to merge")))' "$FAKE/comments.json"
jq 'map(.draft = false)' "$FAKE/pulls.json" >"$FAKE/p.tmp" && mv "$FAKE/p.tmp" "$FAKE/pulls.json"
rm -f "$FAKE/graphql"
run ready "$v26" 1
check "ready: a PR marked ready meanwhile is left alone" bash -c "test '$status' = 0 && test ! -f '$FAKE/graphql'"

started
sha="$(branch_sha "$sync26")"
run_bot accept "$v26" "$d2" https://github.com/o/r/actions/runs/7003 0123456789012345678901234567890123456789 1
check "accept: a head that moved since the run is refused" test "$status" -ne 0
check "accept: pushing nothing" test "$(branch_sha "$sync26")" = "$sha"

setup
printf 'v1.0.0-beta.28 application/vnd.oci.image.index.v1+json %s\n' "$d3" >"$FAKE/images"
run_bot start v1.0.0-beta.28 "$d3"
sha="$(branch_sha release-check/sync/v1.0.0-beta.28)"
run_bot accept v1.0.0-beta.28 "$d3" https://github.com/o/r/actions/runs/7020 "$sha" 1
check "accept: a changed bundle version resets the accept list to the release" bash -c "test \"\$(git -C '$FAKE/origin.git' show 'refs/heads/release-check/sync/v1.0.0-beta.28:$manifest' | grep '^accept')\" = 'accept v1.0.0-beta.28 $d3 verified: https://github.com/o/r/actions/runs/7020'"

setup
git -C "$up" checkout -q -b odd v1.0.0-beta.25
echo 'const valueBundleVersion uint = 3 // typed now' >"$up/dagql/cache_transfer_types.go"
git -C "$up" commit -qam odd && git -C "$up" tag -f -a -m odd "$v26" >/dev/null
run_bot start "$v26" "$d2"
check "advisory: a bundle version it can't read fails the run" test "$status" -ne 0
check "advisory: saying so" has "$tmp/out" "couldn't read dagql's valueBundleVersion"
git -C "$up" checkout -q main
git -C "$up" tag -f -a -m beta.26 "$v26" "$(git -C "$up" rev-parse main~2)" >/dev/null
git -C "$up" branch -q -D odd

echo "--- dry run"
setup
run --dry-run start "$v26" "$d2"
check "dry run: start succeeds without a token" test "$status" = 0
check "dry run: start prints the push" has "$tmp/out" "dry run: would push $sync26"
check "dry run: and the draft PR" has "$tmp/out" "dry run: would open a draft PR from $sync26"
check "dry run: and the dispatch" has "$tmp/out" "dry run: would dispatch remote-cache-tests.yml on $sync26"
check "dry run: pushes nothing" test -z "$(git -C "$FAKE/origin.git" for-each-ref refs/heads/release-check/)"
started
sha="$(branch_sha "$sync26")"
run --dry-run report "$v26" https://github.com/o/r/actions/runs/7002 1
check "dry run: report prints the issue" has "$tmp/out" "dry run: would label compat-break and file"
run --dry-run accept "$v26" "$d2" https://github.com/o/r/actions/runs/7003 "$sha" 1
check "dry run: accept prints the push" has "$tmp/out" "dry run: would push $sync26"
check "dry run: and marking the PR ready" has "$tmp/out" "dry run: would mark #1 ready"
run --dry-run verify "$v26" "$d2"
check "dry run: verify prints the dispatch" has "$tmp/out" "dry run: would dispatch remote-cache-tests.yml"
check "dry run: none of them writes" test "$(writes)" = 0
check "dry run: the branch is as it was" test "$(branch_sha "$sync26")" = "$sha"

echo "--- publish"
setup
run publish
check "publish: a head with no publish run is published" has "$tmp/out" "publish=dispatched"
check "publish: by dispatching cacheservice-publish.yml on the branch" jq -e '.ref == "feature/remote-cache"' "$FAKE/dispatch-cacheservice-publish.yml-1"
check "publish: with GITHUB_TOKEN" has "$FAKE/calls" "gh POST repos/o/r/actions/workflows/cacheservice-publish.yml/dispatches"
setup
ARGS=(publish); rc env FAKE_PUBLISH_RUNS="push:completed:success"
check "publish: a head with a successful run isn't" has "$tmp/out" "publish=none"
check "publish: and dispatches nothing" test "$(writes)" = 0
setup
ARGS=(publish); rc env FAKE_PUBLISH_RUNS="push:completed:failure"
check "publish: a head whose only run failed is published again" has "$tmp/out" "publish=dispatched"
check "publish: by a dispatch" test -f "$FAKE/dispatch-cacheservice-publish.yml-1"
setup
ARGS=(publish); rc env FAKE_PUBLISH_RUNS="push:completed:cancelled"
check "publish: so is one whose run was cancelled" has "$tmp/out" "publish=dispatched"
setup
ARGS=(publish); rc env FAKE_PUBLISH_RUNS="push:completed:failure workflow_dispatch:in_progress"
check "publish: a retry in progress isn't dispatched over" has "$tmp/out" "publish=none"
setup
ARGS=(publish); rc env FAKE_PUBLISH_RUNS="push:queued"
check "publish: nor is a queued run" has "$tmp/out" "publish=none"
check "publish: dispatching nothing" test "$(writes)" = 0
setup
ARGS=(publish); rc env FAKE_PUBLISH_RUNS="push:completed:failure workflow_dispatch:completed:failure"
check "publish: one failed dispatch is retried once more" has "$tmp/out" "publish=dispatched"
setup
ARGS=(publish); rc env FAKE_PUBLISH_RUNS="push:completed:failure workflow_dispatch:completed:failure workflow_dispatch:completed:failure"
check "publish: past two dispatches it stops" has "$tmp/out" "publish=bounded"
check "publish: saying so for the run's summary" has "$tmp/out" "not dispatching again"
check "publish: and dispatches nothing" test "$(writes)" = 0
setup
ARGS=(publish); rc env FAKE_PUBLISH_RUNS="workflow_dispatch:completed:failure workflow_dispatch:completed:failure workflow_dispatch:completed:success"
check "publish: the bound doesn't hide a success" has "$tmp/out" "publish=none"
ARGS=(publish); rc env FAKE_HEAD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
check "publish: nor a head just pushed" has "$tmp/out" "publish=none"
check "publish: neither dispatches" test "$(writes)" = 0

setup
ARGS=(publish); rc env FAKE_HEAD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" FAKE_PUBLISH_RUNS="push:completed:failure"
check "publish: a young head whose run already failed is retried at once" has "$tmp/out" "publish=dispatched"
setup
ARGS=(publish); rc env FAKE_PUBLISH_RUNS="push:completed:success workflow_dispatch:completed:failure workflow_dispatch:completed:failure"
check "publish: a success outranks the bound" has "$tmp/out" "publish=none"

check "engine jobs: the names are the workflow's own" bash -c "grep -qxF '    name: real engines (e2e)' '$here/../../.github/workflows/remote-cache-tests.yml' && grep -qxF '    name: sidecar demo' '$here/../../.github/workflows/remote-cache-tests.yml'"

echo "--- remote-cache-tests.yml's checkouts (zerobull-consulting/dagger#165, lead 2)"
# The verifying run executes remote-cache-tests.yml as the sync branch has
# it, after upstream's tag was merged in: no checkout there may leave the
# job's token in .git/config.
tests_yml="$here/../../.github/workflows/remote-cache-tests.yml"
checkouts_without_persist_off() {
	awk '/uses: actions\/checkout@/ { n++; want = 1; next }
		want && /persist-credentials: false/ { want = 0; ok++ }
		want && /^      - name:/ { want = 0 }
		END { if (n == 0 || n != ok) { print n+0 " checkouts, " ok+0 " with persist-credentials: false"; exit 1 } }' "$tests_yml"
}
check "every checkout in remote-cache-tests.yml has persist-credentials: false" checkouts_without_persist_off

echo
if ((failures > 0)); then
	echo "$failures failed"
	exit 1
fi
echo "all passed"
