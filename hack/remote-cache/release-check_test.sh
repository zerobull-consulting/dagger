#!/usr/bin/env bash
# Tests of release-check.sh, offline: local git repositories stand in for
# upstream dagger/dagger (its tags) and for this fork (its branch), and fake
# gh, docker and go stand in for the GitHub API, the registry and the
# bundle test. Run it from anywhere:
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
bot_secret="bot-secret-token-do-not-print"
gh_secret="gh-secret-token-do-not-print"
manifest=cmd/cacheservice/compat/engines.txt

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
# go: the bundle test passes unless FAKE_GO_FAIL is set.
cat >"$tmp/bin/go" <<'EOF'
#!/usr/bin/env bash
echo "go $*" >>"$FAKE/go-calls"
# The tree it runs in: the merge's first parent, the branch it built on.
echo "built on $(git rev-parse HEAD^1 2>/dev/null || git rev-parse HEAD)" >>"$FAKE/go-calls"
[[ -z "${FAKE_GO_FAIL:-}" ]]
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
	jq --argjson n "$num" --argjson in "$input" '. + [{number: $n, state: "open", title: $in.title, body: $in.body,
		head: {ref: $in.head, sha: "0123456789012345678901234567890123456789", repo: {full_name: "o/r"}}, base: {ref: $in.base}, auto_merge: null, node_id: "PR_\($n)"}]' "$FAKE/pulls.json" >"$FAKE/p.tmp"
	mv "$FAKE/p.tmp" "$FAKE/pulls.json"
	echo "{\"number\": $num}" ;;
"GET repos/o/r/pulls/"*/files*)
	num="${path#repos/o/r/pulls/}"; num="${num%%/*}"
	if [[ -f "$FAKE/files-$num.json" ]]; then cat "$FAKE/files-$num.json"; else
		echo '[{"filename": "cmd/cacheservice/compat/engines.txt", "status": "modified", "additions": 1, "deletions": 0}]'; fi ;;
"GET repos/o/r/pulls/"*) jq --argjson n "${path##*/}" '.[] | select(.number == $n)' "$FAKE/pulls.json" ;;
"GET repos/o/r/git/matching-refs/heads/release-check/")
	git -C "$FAKE/origin.git" for-each-ref --format='%(refname)' refs/heads/release-check/ | jq -R . | jq -s 'map({ref: .})' ;;
"GET repos/o/r/issues?labels=compat-break"*) jq '[.[] | select(.state == "open")]' "$FAKE/issues.json" ;;
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
	# Whether the advisory diff was written before this dispatch.
	# How many tags had been fetched by now.
		git tag -l | wc -l >>"$FAKE/tags-at-dispatch"
		if [[ -f "$FAKE/notes/advisory.md" ]]; then echo "$wf" >>"$FAKE/advisory-before-dispatch"; else echo "$wf" >>"$FAKE/dispatch-saw-no-advisory"; fi
	[[ -n "${FAKE_NO_RUN_ID:-}" ]] && exit 0
	id=$((5000 + $(n run)))
	c="$(head -n1 "$FAKE/conclusions" 2>/dev/null || echo success)"
	sed -i 1d "$FAKE/conclusions" 2>/dev/null || true
	echo "$c" >"$FAKE/run-$id"
	echo "{\"workflow_run_id\": $id, \"run_url\": \"x\", \"html_url\": \"x\"}" ;;
"GET repos/o/r/actions/runs/"*/jobs*)
	[[ -n "${FAKE_JOBS_FAIL:-}" ]] && { echo "HTTP 502" >&2; exit 1; }
	# FAKE_JOBS_FAIL_AFTER=1: the first read works, every later one fails.
	# FAKE_JOBS_FAIL_FIRST=1: the first read fails, the later ones work.
	if [[ -n "${FAKE_JOBS_FAIL_FIRST:-}" ]] && (($(n jobs-try) <= FAKE_JOBS_FAIL_FIRST)); then echo "HTTP 502" >&2; exit 1; fi
	if [[ -n "${FAKE_JOBS_FAIL_AFTER:-}" ]] && (($(n jobs-read) > FAKE_JOBS_FAIL_AFTER)); then echo "HTTP 502" >&2; exit 1; fi
	# FAKE_JOBS: the jobs array as it is; otherwise the four jobs of a
	# candidate run, all green in a green run and with the e2e red in a red
	# one (at FAKE_FAILED_STEP).
	if [[ -n "${FAKE_JOBS:-}" ]]; then echo "{\"jobs\": $FAKE_JOBS}"; exit 0; fi
	failed_step="${FAKE_FAILED_STEP:-Real engines}"
	rid="${path#repos/o/r/actions/runs/}"; rid="${rid%%/*}"
	e2e=success; [[ "$(cat "$FAKE/run-$rid")" == failure ]] && e2e=failure
	echo '{"jobs": [{"name": "unit tests", "conclusion": "success", "html_url": "https://github.com/o/r/actions/runs/1/job/4"},
		{"name": "S3 tests against MinIO", "conclusion": "success", "html_url": "https://github.com/o/r/actions/runs/1/job/5"},
		{"name": "real engines (e2e)", "conclusion": "'"$e2e"'", "html_url": "https://github.com/o/r/actions/runs/1/job/2",
		"steps": [{"name": "Checkout", "conclusion": "success"}, {"name": "'"$failed_step"'", "conclusion": "'"$e2e"'"}]},
		{"name": "sidecar demo", "conclusion": "success", "html_url": "https://github.com/o/r/actions/runs/1/job/3"}]}' ;;
"GET repos/o/r/actions/runs/"*)
	# FAKE_RUN_READ_FAIL: no run can be read.
	[[ -n "${FAKE_RUN_READ_FAIL:-}" ]] && { echo "HTTP 502" >&2; exit 1; }
	# A run whose conclusion is "running" hasn't completed (yet).
	c="$(cat "$FAKE/run-${path##*/}")"
	if [[ "$c" == running ]]; then echo '{"status": "in_progress", "conclusion": null}'; else
		echo "{\"status\": \"completed\", \"conclusion\": \"$c\"}"; fi ;;
"GET repos/o/r/branches/feature/remote-cache")
	sha="$(git -C "$FAKE/origin.git" rev-parse refs/heads/feature/remote-cache)"
	echo "{\"commit\": {\"sha\": \"$sha\", \"commit\": {\"committer\": {\"date\": \"${FAKE_HEAD_DATE:-2020-01-01T00:00:00Z}\"}}}}" ;;
"GET repos/o/r/actions/workflows/cacheservice-publish.yml/runs?head_sha="*)
	# FAKE_PUBLISH_RUNS: space-separated "<event>:<status>:<conclusion>".
	jq -n --arg r "${FAKE_PUBLISH_RUNS:-}" '{workflow_runs: [$r | split(" ")[] | select(. != "") | split(":") | {event: .[0], status: .[1], conclusion: (.[2] // null)}]}' ;;
"POST graphql")
	[[ -n "${FAKE_CLEAN:-}" ]] && { echo "gh: Pull request Pull request is in clean status (enablePullRequestAutoMerge)" >&2; exit 1; }
	printf '%s\n' "${fields[@]}" >"$FAKE/graphql" ; echo '{}' ;;
"POST repos/o/r/actions/runs/"*/cancel)
	# As the API does: a run that has completed can't be cancelled.
	# FAKE_CANCEL_FAIL: no cancel works.
	[[ -n "${FAKE_CANCEL_FAIL:-}" ]] && { echo "HTTP 502" >&2; exit 1; }
	rid="${path#repos/o/r/actions/runs/}"; rid="${rid%%/*}"
	[[ "$(cat "$FAKE/run-$rid")" == running ]] || { echo "Cannot cancel a workflow run that is completed. (HTTP 409)" >&2; exit 1; }
	echo cancelled >"$FAKE/run-$rid"
	echo "{}" ;;
"PUT repos/o/r/pulls/"*/merge) echo "$input" >"$FAKE/merged"; echo '{"merged": true}' ;;
*) echo "fake gh: unexpected $method $path" >&2; exit 1 ;;
esac
EOF
chmod +x "$tmp/bin/"*

# --- upstream: tags beta.24 to beta.27 on main, plus tags the check must
# skip. Each release changes a file the fork doesn't touch. ---
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
	: >"$FAKE/calls"
	printf 'v1.0.0-beta.26 application/vnd.oci.image.index.v1+json %s\n' "$d2" >"$FAKE/images"
	git clone -q --bare "$up" "$FAKE/origin.git"
	git -C "$FAKE/origin.git" tag -l | xargs -r git -C "$FAKE/origin.git" tag -d >/dev/null
	work="$tmp/work"
	git clone -q "$FAKE/origin.git" "$work"
	git -C "$work" checkout -q -b feature/remote-cache v1.0.0-beta.25 2>/dev/null ||
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
	(
		cd "$work" &&
			PATH="$tmp/bin:$PATH" BOT_SECRET="$bot_secret" GH_SECRET="$gh_secret" GH_TOKEN="$gh_secret" \
				ENGINES_FILE="$work/$manifest" RELEASE_CHECK_REPO=o/r RELEASE_CHECK_REMOTE="$FAKE/origin.git" \
				RELEASE_CHECK_UPSTREAM="$up" RELEASE_CHECK_DIR="$FAKE/notes" RELEASE_CHECK_POLL_SECONDS=0 \
				GITHUB_SERVER_URL=https://github.com GITHUB_OUTPUT="$FAKE/output" RELEASE_CHECK_BUNDLE="${BUNDLE-pass}" \
				"$@" "$script" "${ARGS[@]}"
	) >"$tmp/out" 2>&1
	status=$?
}
run() { ARGS=("$@"); rc env; }
run_bot() { ARGS=("$@"); rc env BOT_TOKEN="$bot_secret"; }
writes() { grep -cE ' (POST|PATCH|PUT|DELETE) ' "$FAKE/calls" || true; }
branch_file() { git -C "$FAKE/origin.git" show "refs/heads/$1:$manifest"; }
no_secret() { not grep -qE "$bot_secret|$gh_secret" "$tmp/out"; }

echo "--- plan"
setup
run plan
check "plan: the oldest new release with an image is verified" has "$tmp/out" "action=verify"
check "plan: it is beta.26, not beta.24 (older than the manifest's lines) or beta.27" has "$tmp/out" "version=v1.0.0-beta.26"
check "plan: with its index digest" has "$tmp/out" "digest=$d2"
check "plan: sdk/*, v0.* and malformed tags are skipped" not hasx "$tmp/out" "sdk/|v0\.19|beta_x"
check "plan: writes nothing" test "$(writes)" = 0
check "plan: outputs reach GITHUB_OUTPUT" has "$FAKE/output" "action=verify"

: >"$FAKE/images"
run plan
check "plan: a release with no image yet is skipped this hour" has "$tmp/out" "no registry.dagger.io/engine:v1.0.0-beta.26 image yet"
check "plan: and nothing is done" has "$tmp/out" "action=none"

printf 'v1.0.0-beta.27 application/vnd.oci.image.index.v1+json %s\n' "$d2" >"$FAKE/images"
run plan
check "plan: a newer release with an image waits for an older one without" has "$tmp/out" "action=none"
check "plan: so the older one is never left behind" not has "$tmp/out" "version=v1.0.0-beta.27"

ARGS=(plan); rc env RELEASE_CHECK_UPSTREAM="$tmp/no-such-repo"
check "plan: an upstream that can't be listed fails the run" test "$status" -ne 0
check "plan: rather than reading as nothing new" not has "$tmp/out" "action=none"

printf 'v1.0.0-beta.26 application/vnd.oci.image.manifest.v1+json %s\n' "$d2" >"$FAKE/images"
run plan
check "plan: a single-platform image fails the run" test "$status" -ne 0

printf 'v1.0.0-beta.26 application/vnd.oci.image.index.v1+json sha256:abc\n' >"$FAKE/images"
run plan
check "plan: a malformed digest from the registry fails the run" test "$status" -ne 0

ARGS=(plan); rc env FAKE_REGISTRY_ERROR="unexpected status 503"
check "plan: a registry error fails the run, rather than skipping" test "$status" -ne 0

run plan v1.0.0-beta.24
check "plan: a chosen version is for dry runs only" test "$status" -ne 0
setup
run --dry-run plan v1.0.0-beta.26
check "plan: a dry run may choose a version" has "$tmp/out" "version=v1.0.0-beta.26"

echo "--- verify"
setup
echo success >"$FAKE/conclusions"
run verify v1.0.0-beta.26 "$d2"
check "verify: a green run passes" has "$tmp/out" "result=pass"
check "verify: cites the run" has "$tmp/out" "run=https://github.com/o/r/actions/runs/5001"
check "verify: dispatches remote-cache-tests.yml on the branch with the candidate" \
	jq -e --arg d "$d2" '.ref == "feature/remote-cache" and .inputs == {"candidate-version": "v1.0.0-beta.26", "candidate-digest": $d}' "$FAKE/dispatch-remote-cache-tests.yml-1"
check "verify: dispatches with GITHUB_TOKEN" has "$FAKE/calls" "gh POST repos/o/r/actions/workflows/remote-cache-tests.yml/dispatches"
check "verify: never runs upstream's code (the bundle job does)" not test -f "$FAKE/go-calls"
check "verify: writes the advisory diff" has "$FAKE/notes/advisory.md" "dagql/cache_live.go"

setup
echo failure >"$FAKE/conclusions"
run verify v1.0.0-beta.26 "$d2"
check "verify: a red run fails" has "$tmp/out" "result=fail"
check "verify: and lists its failing jobs" has "$FAKE/notes/a-jobs.md" "real engines (e2e)"

setup
echo failure >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env FAKE_JOBS_FAIL=1
check "verify: a red run whose jobs can't be read is an error, not a failure" test "$status" -ne 0
check "verify: so no result is given" not has "$tmp/out" "result="

setup
echo success >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env FAKE_JOBS_FAIL_FIRST=1
check "verify: a jobs read that fails once is tried again" has "$tmp/out" "result=pass"
setup
ARGS=(publish); rc env FAKE_PUBLISH_RUNS="push:completed:failure"
check "publish: lists the runs of the branch only" has "$FAKE/calls" "&branch=feature/remote-cache"
check "engine jobs: the names are the workflow's own" bash -c "grep -qxF '    name: real engines (e2e)' '$here/../../.github/workflows/remote-cache-tests.yml' && grep -qxF '    name: sidecar demo' '$here/../../.github/workflows/remote-cache-tests.yml'"

setup
echo failure >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env FAKE_JOBS_FAIL_AFTER=1
check "verify: a red run's jobs are read once, so a failing second read can't stand in for them" has "$FAKE/notes/a-jobs.md" "real engines (e2e)"
check "verify: and no placeholder list is written" not has "$FAKE/notes/a-jobs.md" "couldn't list"

# Only the engine jobs decide (zerobull-consulting/dagger#166).
job() { printf '{"name": "%s", "conclusion": "%s", "html_url": "https://github.com/o/r/actions/runs/1/job/9", "steps": []}' "$1" "$2"; }
jobs_of() { local IFS=,; echo "[$*]"; }
e2e="real engines (e2e)"
setup
echo failure >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env FAKE_JOBS="$(jobs_of "$(job 'unit tests' failure)" "$(job 'S3 tests against MinIO' success)" "$(job "$e2e" success)" "$(job 'sidecar demo' success)")"
check "verify: a red unit test job is an error, not a failure" test "$status" -ne 0
check "verify: naming the job" has "$tmp/out" "unit tests: failure"
check "verify: so no result is given" not has "$tmp/out" "result="
setup
echo failure >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env FAKE_JOBS="$(jobs_of "$(job 'unit tests' success)" "$(job 'S3 tests against MinIO' timed_out)" "$(job "$e2e" success)" "$(job 'sidecar demo' success)")"
check "verify: nor is a timed-out MinIO job a failure of the engine" test "$status" -ne 0
setup
echo failure >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env FAKE_JOBS="$(jobs_of "$(job 'unit tests' failure)" "$(job 'S3 tests against MinIO' success)" "$(job "$e2e" failure)" "$(job 'sidecar demo' success)")"
check "verify: a red engine job with a red unit job is still an error" test "$status" -ne 0
setup
echo failure >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env FAKE_JOBS="$(jobs_of "$(job 'unit tests' success)" "$(job 'S3 tests against MinIO' success)" "$(job "$e2e" success)" "$(job 'sidecar demo' failure)")"
check "verify: a red sidecar demo is the failure verdict" has "$tmp/out" "result=fail"
check "verify: and the list names only it" has "$FAKE/notes/a-jobs.md" "sidecar demo"
setup
echo success >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env FAKE_JOBS="$(jobs_of "$(job 'unit tests' success)" "$(job 'S3 tests against MinIO' success)" "$(job 'sidecar demo' success)")"
check "verify: a green run without its e2e job is an error" test "$status" -ne 0
check "verify: not a pass" not has "$tmp/out" "result="
setup
echo failure >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env FAKE_JOBS="$(jobs_of "$(job "$e2e" failure)")"
check "verify: a red run without its sidecar job is an error, not a failure" test "$status" -ne 0
setup
echo failure >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env FAKE_JOBS="$(jobs_of "$(job "$e2e" failure)" "$(job 'sidecar demo' skipped)")"
check "verify: a red engine job beside a skipped one is an error, not a failure" test "$status" -ne 0
check "verify: so no result is given" not has "$tmp/out" "result="
setup
echo failure >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env FAKE_JOBS="$(jobs_of "$(job "$e2e" failure)" "$(job "$e2e" success)" "$(job 'sidecar demo' success)")"
check "verify: a duplicated engine job is an error" test "$status" -ne 0
check "verify: not a failure verdict" not has "$tmp/out" "result="
setup
echo success >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env FAKE_JOBS="$(jobs_of "$(job "$e2e" success)" "$(job 'sidecar demo' skipped)")"
check "verify: a skipped engine job in a green run is an error" test "$status" -ne 0
setup
echo success >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env FAKE_JOBS_FAIL=1
check "verify: a green run whose jobs can't be read is an error" test "$status" -ne 0
setup
echo failure >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env FAKE_JOBS="$(jobs_of "$(job 'unit tests' success)" "$(job "$e2e" success)" "$(job 'sidecar demo' success)")"
check "verify: a red run whose every job is green is an error" test "$status" -ne 0

echo "--- the bundle test"
setup
ARGS=(bundle v1.0.0-beta.26); rc env -u GH_TOKEN
check "bundle: runs the bundle test with the tag merged" has "$FAKE/go-calls" "TestBundleVersionMatchesEngineCapture"
check "bundle: passes" has "$tmp/out" "bundle=pass"
check "bundle: notes the tag merges" has "$tmp/out" "merge=merged"
check "bundle: writes nothing" test "$(writes)" = 0
setup
ARGS=(bundle v1.0.0-beta.26); rc env -u GH_TOKEN FAKE_GO_FAIL=1
check "bundle: a failing bundle test fails" has "$tmp/out" "bundle=fail"
for var in GH_TOKEN GITHUB_TOKEN BOT_TOKEN; do
	setup
	ARGS=(bundle v1.0.0-beta.26); rc env -u GH_TOKEN "$var=x"
	check "bundle: refuses to run upstream's code with $var set" test "$status" -ne 0
	check "bundle: before compiling anything ($var)" not test -f "$FAKE/go-calls"
done

setup
echo failure >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env FAKE_FAILED_STEP="The engine pin, from the manifest"
check "verify: a run that failed at its pin step is an error, not a failure" test "$status" -ne 0
check "verify: saying it failed before testing" has "$tmp/out" "failed before testing"

setup
echo success >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env RELEASE_CHECK_TEST_REF=remote-cache/some-branch
check "verify: a dry run's verification runs on its own ref" jq -e '.ref == "remote-cache/some-branch"' "$FAKE/dispatch-remote-cache-tests.yml-1"

setup
echo cancelled >"$FAKE/conclusions"
run verify v1.0.0-beta.26 "$d2"
check "verify: a cancelled run is an error, not a failure" test "$status" -ne 0
check "verify: so no result is given" not has "$tmp/out" "result="

setup
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env FAKE_NO_RUN_ID=1
check "verify: a dispatch whose run isn't named is an error" test "$status" -ne 0

setup
run verify 'v1.0.0-beta.26;true' "$d2"
check "verify: a malformed version is refused" test "$status" -ne 0
run verify v1.0.0-beta.26 sha256:abc
check "verify: a malformed digest is refused" test "$status" -ne 0
check "verify: before anything is dispatched" test "$(writes)" = 0

echo "--- outcome A"
setup
echo success >"$FAKE/conclusions"
run verify v1.0.0-beta.26 "$d2"
run_bot accept v1.0.0-beta.26 "$d2" https://github.com/o/r/actions/runs/5001
check "accept: opens a PR" has "$tmp/out" "pr=1"
check "accept: as the bot" has "$FAKE/calls" "bot POST repos/o/r/pulls"
check "accept: from release-check/accept/<version>" jq -e '.[0].head.ref == "release-check/accept/v1.0.0-beta.26" and .[0].base.ref == "feature/remote-cache"' "$FAKE/pulls.json"
diff_lines="$(git -C "$FAKE/origin.git" diff feature/remote-cache release-check/accept/v1.0.0-beta.26 | grep -E '^[-+][^-+]')"
check "accept: the branch adds only the accept line" test "$diff_lines" = "+accept v1.0.0-beta.26 $d2 verified: https://github.com/o/r/actions/runs/5001"
check "accept: the PR cites the verifying run" jq -e '.[0].body | contains("actions/runs/5001")' "$FAKE/pulls.json"
check "accept: and says auto-merge is off" jq -e '.[0].body | contains("Auto-merge is off")' "$FAKE/pulls.json"
check "accept: no token in the output" no_secret

before="$(writes)"
printf 'v1.0.0-beta.26 application/vnd.oci.image.index.v1+json %s\n' "$d2" >"$FAKE/images"
run plan
check "A again: the next hour finds the open PR" has "$tmp/out" "action=pending"
check "A again: names it" has "$tmp/out" "pr=1"
check "A again: and opens nothing" test "$(writes)" = "$before"

run auto-merge 1
check "auto-merge: off without RELEASE_CHECK_AUTO_MERGE" has "$tmp/out" "auto_merge=off"
check "auto-merge: and enables nothing" not test -f "$FAKE/graphql"
ARGS=(auto-merge 1); rc env RELEASE_CHECK_AUTO_MERGE=true
check "auto-merge: on with it" has "$tmp/out" "auto_merge=on"
check "auto-merge: squash, with GITHUB_TOKEN" has "$FAKE/calls" "gh POST graphql"
check "auto-merge: titled with the full PR reference" has "$FAKE/graphql" "(zerobull-consulting/dagger#1)"
rm -f "$FAKE/graphql"
ARGS=(auto-merge 1); rc env RELEASE_CHECK_AUTO_MERGE=true FAKE_CLEAN=1
check "auto-merge: a PR whose checks passed already is squash-merged" has "$tmp/out" "auto_merge=merged"
check "auto-merge: with GITHUB_TOKEN, pinned to the checked head" jq -e '.merge_method == "squash" and .sha == "0123456789012345678901234567890123456789" and (.commit_title | endswith("(zerobull-consulting/dagger#1)"))' "$FAKE/merged"
check "auto-merge: by GITHUB_TOKEN" has "$FAKE/calls" "gh PUT repos/o/r/pulls/1/merge"
echo '[{"filename": "cmd/cacheservice/compat/engines.txt", "status": "modified", "additions": 2, "deletions": 0}]' >"$FAKE/files-1.json"
ARGS=(auto-merge 1); rc env RELEASE_CHECK_AUTO_MERGE=true
check "auto-merge: refused for a PR adding two lines" test "$status" -ne 0
check "auto-merge: which isn't enabled" not test -f "$FAKE/graphql"
echo '[{"filename": ".github/workflows/x.yml", "status": "modified", "additions": 1, "deletions": 0}]' >"$FAKE/files-1.json"
ARGS=(auto-merge 1); rc env RELEASE_CHECK_AUTO_MERGE=true
check "auto-merge: refused for a PR changing another file" test "$status" -ne 0
rm -f "$FAKE/files-1.json"
jq '.[0].head.repo.full_name = "someone/fork"' "$FAKE/pulls.json" >"$FAKE/p" && mv "$FAKE/p" "$FAKE/pulls.json"
ARGS=(auto-merge 1); rc env RELEASE_CHECK_AUTO_MERGE=true
check "auto-merge: refused for a fork's PR of the same name" test "$status" -ne 0
check "auto-merge: which isn't enabled either" not test -f "$FAKE/graphql"
run plan
check "plan: a fork's PR of the same name isn't the release's PR" not has "$tmp/out" "action=pending"
jq '.[0].head.repo.full_name = "o/r"' "$FAKE/pulls.json" >"$FAKE/p" && mv "$FAKE/p" "$FAKE/pulls.json"

# The PR merged: the line is on the branch, and the tag is done.
git -C "$work" fetch -q origin
git -C "$work" merge -q --ff-only origin/release-check/accept/v1.0.0-beta.26 2>/dev/null ||
	git -C "$work" merge -q origin/release-check/accept/v1.0.0-beta.26
jq 'map(.state = "closed")' "$FAKE/pulls.json" >"$FAKE/p" && mv "$FAKE/p" "$FAKE/pulls.json"
git -C "$FAKE/origin.git" update-ref -d refs/heads/release-check/accept/v1.0.0-beta.26
run plan
check "A merged: the release isn't new any more" not has "$tmp/out" "v1.0.0-beta.26:"
check "A merged: and the next one without an image waits" has "$tmp/out" "action=none"

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

echo "--- outcome B"
setup
printf 'failure\nsuccess\n' >"$FAKE/conclusions"
run verify v1.0.0-beta.26 "$d2"
check "B: A fails first" has "$tmp/out" "result=fail"
run_bot rebuild v1.0.0-beta.26 "$d2"
check "B: the rebuild passes" has "$tmp/out" "result=pass"
check "B: and opens a PR" has "$tmp/out" "pr=1"
check "B: as the bot" has "$FAKE/calls" "bot POST repos/o/r/pulls"
check "B: verified on its own branch" jq -e '.ref == "release-check/rebuild/v1.0.0-beta.26"' "$FAKE/dispatch-remote-cache-tests.yml-2"
check "B: the branch has the tag as a parent" git -C "$FAKE/origin.git" merge-base --is-ancestor "$(git -C "$up" rev-parse 'v1.0.0-beta.26^{commit}')" release-check/rebuild/v1.0.0-beta.26
check "B: moves the base" bash -c "git -C '$FAKE/origin.git' show 'refs/heads/release-check/rebuild/v1.0.0-beta.26:$manifest' | grep -qx 'base v1.0.0-beta.26'"
check "B: adds the accept line, citing its own run, first" bash -c "git -C '$FAKE/origin.git' show 'refs/heads/release-check/rebuild/v1.0.0-beta.26:$manifest' | grep '^accept' | head -n1 | grep -qx 'accept v1.0.0-beta.26 $d2 verified: https://github.com/o/r/actions/runs/5002'"
check "B: keeps the other lines (no version change)" bash -c "git -C '$FAKE/origin.git' show 'refs/heads/release-check/rebuild/v1.0.0-beta.26:$manifest' | grep -q '^accept v1.0.0-beta.25 '"
check "B: says to merge with a merge commit" jq -e '.[0].body | contains("with a merge commit")' "$FAKE/pulls.json"
check "B: no auto-merge" not test -f "$FAKE/graphql"
check "B: no token in the output" no_secret
before="$(writes)"
run plan
check "B again: the open PR is found" has "$tmp/out" "kind=rebuild"
check "B again: nothing new" test "$(writes)" = "$before"

# The bundle version changes in beta.26: the list resets to it.
setup
git -C "$up" checkout -q -b bump v1.0.0-beta.25
echo 'const valueBundleVersion = 4' >"$up/dagql/cache_transfer_types.go"
git -C "$up" commit -qam bump && git -C "$up" tag -f -a -m bumped v1.0.0-beta.26 >/dev/null
printf 'failure\nsuccess\n' >"$FAKE/conclusions"
run verify v1.0.0-beta.26 "$d2"
check "B reset: the advisory names the bundle version change" has "$FAKE/notes/advisory.md" "3 in v1.0.0-beta.25, 4 in v1.0.0-beta.26"
run_bot rebuild v1.0.0-beta.26 "$d2"
check "B reset: the accept list is the new version alone" test "$(branch_file release-check/rebuild/v1.0.0-beta.26 | grep -c '^accept')" = 1
check "B reset: the PR says so" jq -e '.[0].body | contains("resets to")' "$FAKE/pulls.json"
git -C "$up" checkout -q main
git -C "$up" tag -f -a -m beta.26 v1.0.0-beta.26 "$(git -C "$up" rev-parse main~1)" >/dev/null

# The tag is merged already (the sync got there first): no rebuild.
setup
git -C "$work" merge -q --no-edit "$(git -C "$up" rev-parse 'v1.0.0-beta.26^{commit}')" 2>/dev/null ||
	{ git -C "$work" fetch -q "$up" v1.0.0-beta.26 && git -C "$work" merge -q --no-edit FETCH_HEAD; }
git -C "$work" push -q origin feature/remote-cache
run_bot rebuild v1.0.0-beta.26 "$d2"
check "B ancestor: skipped, it would be the code A ran" has "$tmp/out" "result=skipped"
check "B ancestor: nothing pushed or opened" test "$(writes)" = 0

setup
printf 'failure\nsuccess\n' >"$FAKE/conclusions"
run verify v1.0.0-beta.26 "$d2"
before="$(writes)"
ARGS=(rebuild v1.0.0-beta.26 "$d2"); BUNDLE=fail rc env BOT_TOKEN="$bot_secret"
check "B bundle: no rebuild when the bundle test of that merge failed" has "$tmp/out" "result=skipped"
check "B bundle: nothing pushed, dispatched or opened" test "$(writes)" = "$before"
check "B bundle: no rebuild branch" test -z "$(git -C "$FAKE/origin.git" for-each-ref refs/heads/release-check/)"
check "B bundle: never runs upstream's code itself" not test -f "$FAKE/go-calls"
run_bot reject v1.0.0-beta.26
check "B bundle: C's issue says why it wasn't rebuilt" jq -e '.[0].body | contains("the bundle test with")' "$FAKE/issues.json"

setup
printf 'failure\nfailure\n' >"$FAKE/conclusions"
run verify v1.0.0-beta.26 "$d2"
run_bot rebuild v1.0.0-beta.26 "$d2"
check "B red: the rebuild fails" has "$tmp/out" "result=fail"
check "B red: no PR" test "$(jq length "$FAKE/pulls.json")" = 0

echo "--- outcome C"
run_bot reject v1.0.0-beta.26
check "C: files the issue" has "$tmp/out" "issue=101"
check "C: with GITHUB_TOKEN" has "$FAKE/calls" "gh POST repos/o/r/issues"
check "C: labeled compat-break, creating the label" bash -c "grep -q 'gh POST repos/o/r/labels' '$FAKE/calls' && jq -e '.[0].labels == [\"compat-break\"]' '$FAKE/issues.json' >/dev/null"
check "C: with both runs' failing jobs and the diff" jq -e '.[0].body | (contains("actions/runs/5001") and contains("actions/runs/5002") and contains("real engines (e2e)") and contains("dagql/cache_live.go"))' "$FAKE/issues.json"
check "C: opens the reject PR as the bot" has "$FAKE/calls" "bot POST repos/o/r/pulls"
check "C: whose line names the issue" bash -c "git -C '$FAKE/origin.git' show 'refs/heads/release-check/reject/v1.0.0-beta.26:$manifest' | grep -qx 'reject v1.0.0-beta.26 o/r#101'"
check "C: no token in the output" no_secret
before="$(writes)"
run plan
check "C again: the open reject PR is found" has "$tmp/out" "kind=reject"
check "C again: nothing new" test "$(writes)" = "$before"

# A run that stopped after the issue: the reject PR is opened, once.
setup
jq '. + [{number: 101, state: "open", title: "compat-break: engine v1.0.0-beta.26 fails against the cache service"}]' "$FAKE/issues.json" >"$FAKE/i" && mv "$FAKE/i" "$FAKE/issues.json"
run plan
check "C resume: an open issue without its reject branch resumes" has "$tmp/out" "action=resume-reject"
check "C resume: naming the issue" has "$tmp/out" "issue=101"
run_bot reject v1.0.0-beta.26 101
check "C resume: files no second issue" not has "$FAKE/calls" "POST repos/o/r/issues"
check "C resume: opens the reject PR" has "$tmp/out" "pr=1"

# A PR closed by hand leaves its branch: the check stops and says so.
jq 'map(.state = "closed")' "$FAKE/pulls.json" >"$FAKE/p" && mv "$FAKE/p" "$FAKE/pulls.json"
run plan
check "stuck: a branch with no open PR fails the run" test "$status" -ne 0
check "stuck: naming the branch" has "$tmp/out" "release-check/reject/v1.0.0-beta.26"

echo "--- the dry run's ref, and the order"
# zerobull-consulting/dagger#162: a dry run's bundle test builds on the dry
# run's ref, as its e2e does; a real run's on the branch.
setup
git -C "$work" checkout -q -b other
echo "# other" >>"$work/$manifest"
git -C "$work" commit -qam other && git -C "$work" push -q origin other
other="$(git -C "$work" rev-parse HEAD)"
git -C "$work" checkout -q feature/remote-cache
main="$(git -C "$FAKE/origin.git" rev-parse refs/heads/feature/remote-cache)"
ARGS=(bundle v1.0.0-beta.26); rc env -u GH_TOKEN RELEASE_CHECK_TEST_REF=other
check "bundle on the dry run's ref (zerobull-consulting/dagger#162): it builds on that ref" has "$FAKE/go-calls" "built on $other"
check "bundle on the dry run's ref (zerobull-consulting/dagger#162): not on the branch" not has "$FAKE/go-calls" "built on $main"
rm -f "$FAKE/go-calls"
ARGS=(bundle v1.0.0-beta.26); rc env -u GH_TOKEN
check "bundle on the branch (zerobull-consulting/dagger#162): a real run builds on the branch" has "$FAKE/go-calls" "built on $main"
check "bundle on the branch (zerobull-consulting/dagger#162): not on another ref" not has "$FAKE/go-calls" "built on $other"

# zerobull-consulting/dagger#163: verify dispatches before it fetches the
# tags and writes the advisory diff.
setup
echo success >"$FAKE/conclusions"
run verify v1.0.0-beta.26 "$d2"
check "dispatch first (zerobull-consulting/dagger#163): no advisory diff existed at the dispatch" has "$FAKE/dispatch-saw-no-advisory" "remote-cache-tests.yml"
check "dispatch first (zerobull-consulting/dagger#163): and none was written before it" not test -s "$FAKE/advisory-before-dispatch"
check "dispatch first (zerobull-consulting/dagger#163): no tag was fetched at the dispatch" test "$(head -n1 "$FAKE/tags-at-dispatch" | tr -d ' ')" = 0
check "dispatch first (zerobull-consulting/dagger#163): both the tag and the base are fetched after" test "$(git -C "$work" tag -l | wc -l)" -eq 2
check "dispatch first (zerobull-consulting/dagger#163): and it is written after" test -s "$FAKE/notes/advisory.md"
check "dispatch first (zerobull-consulting/dagger#163): the verdict is the same" has "$tmp/out" "result=pass"
# A fetch that fails after the dispatch is an error, never a verdict.
setup
echo running >"$FAKE/conclusions"
run verify v1.0.0-beta.99 "$d2"
check "dispatch first: a tag that can't be fetched is an error" test "$status" -ne 0
check "dispatch first: after the dispatch" test -f "$FAKE/dispatch-remote-cache-tests.yml-1"
check "dispatch first: so no result is given" not has "$tmp/out" "result="
check "dispatch first: saying the run isn't awaited" has "$tmp/out" "is not awaited"
check "dispatch first: and cancelling it" has "$FAKE/calls" "gh POST repos/o/r/actions/runs/5001/cancel"
check "dispatch first: and saying it is cancelled" has "$tmp/out" "no result is given: cancelled it"
# zerobull-consulting/dagger#187: a wait that times out, or a verdict that
# can't be read, cancels the run too.
setup
echo running >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env RELEASE_CHECK_WAIT_MINUTES=0
check "a wait that times out (zerobull-consulting/dagger#187): is an error" test "$status" -ne 0
check "a wait that times out (zerobull-consulting/dagger#187): saying so" has "$tmp/out" "didn't complete within 0 minutes"
check "a wait that times out (zerobull-consulting/dagger#187): so no result is given" not has "$tmp/out" "result="
check "a wait that times out (zerobull-consulting/dagger#187): the run is cancelled" has "$FAKE/calls" "gh POST repos/o/r/actions/runs/5001/cancel"
check "a wait that times out (zerobull-consulting/dagger#187): and said to be" has "$tmp/out" "is not awaited and no result is given: cancelled it"
# A cancel that fails on a run still going: said so, not cancelled.
setup
echo running >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env RELEASE_CHECK_WAIT_MINUTES=0 FAKE_CANCEL_FAIL=1
check "a cancel that fails (zerobull-consulting/dagger#187): is said to have failed" has "$tmp/out" "cancelling it failed: it will finish unread"
check "a cancel that fails (zerobull-consulting/dagger#187): not to have cancelled" not has "$tmp/out" "cancelled it"
# Neither the cancel nor the run's read works: its state is unknown.
setup
echo running >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env RELEASE_CHECK_WAIT_MINUTES=0 FAKE_CANCEL_FAIL=1 FAKE_RUN_READ_FAIL=1
check "a cancel and a read that fail (zerobull-consulting/dagger#187): is an error" test "$status" -ne 0
check "a cancel and a read that fail (zerobull-consulting/dagger#187): the state is said to be unknown" has "$tmp/out" "cancelling it and reading its state failed: it may finish unread"
check "a cancel and a read that fail (zerobull-consulting/dagger#187): not that it will finish unread" not has "$tmp/out" "it will finish unread"
# The run ends while the jobs read keeps failing: the cancel is tried and
# refused, and the message says the run ended rather than that it was
# cancelled.
setup
echo success >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env FAKE_JOBS_FAIL=1
check "a jobs read that fails (zerobull-consulting/dagger#187): is an error" test "$status" -ne 0
check "a jobs read that fails (zerobull-consulting/dagger#187): so no result is given" not has "$tmp/out" "result="
check "a jobs read that fails (zerobull-consulting/dagger#187): the cancel is tried" has "$FAKE/calls" "gh POST repos/o/r/actions/runs/5001/cancel"
check "a jobs read that fails (zerobull-consulting/dagger#187): the run is said to have ended" has "$tmp/out" "ended 'success' and no result is given: nothing to cancel"
check "a jobs read that fails (zerobull-consulting/dagger#187): not to be cancelled" not has "$tmp/out" "cancelled it"
check "a jobs read that fails (zerobull-consulting/dagger#187): nor that the cancel failed" not has "$tmp/out" "cancelling it failed"
# A run that ended cancelled: neither green nor red, and not cancelled again.
setup
echo cancelled >"$FAKE/conclusions"
run verify v1.0.0-beta.26 "$d2"
check "a run that ended cancelled (zerobull-consulting/dagger#187): is an error" test "$status" -ne 0
check "a run that ended cancelled (zerobull-consulting/dagger#187): so no result is given" not has "$tmp/out" "result="
check "a run that ended cancelled (zerobull-consulting/dagger#187): the cancel is tried" has "$FAKE/calls" "gh POST repos/o/r/actions/runs/5001/cancel"
check "a run that ended cancelled (zerobull-consulting/dagger#187): is said to have ended" has "$tmp/out" "ended 'cancelled' and no result is given: nothing to cancel"
check "a run that ended cancelled (zerobull-consulting/dagger#187): not to be cancelled by the check" not has "$tmp/out" "cancelled it"
# A verdict that is read: nothing is cancelled.
setup
echo failure >"$FAKE/conclusions"
run verify v1.0.0-beta.26 "$d2"
check "a verdict that is read (zerobull-consulting/dagger#187): is given" has "$tmp/out" "result=fail"
check "a verdict that is read (zerobull-consulting/dagger#187): nothing is cancelled" not has "$FAKE/calls" "/cancel"
# The rebuild's run too: a wait that times out, or a jobs read that fails.
setup
printf 'failure\nrunning\n' >"$FAKE/conclusions"
run verify v1.0.0-beta.26 "$d2"
ARGS=(rebuild v1.0.0-beta.26 "$d2"); rc env BOT_TOKEN="$bot_secret" RELEASE_CHECK_WAIT_MINUTES=0
check "a rebuild wait that times out (zerobull-consulting/dagger#187): is an error" test "$status" -ne 0
check "a rebuild wait that times out (zerobull-consulting/dagger#187): saying so" has "$tmp/out" "didn't complete within 0 minutes"
check "a rebuild wait that times out (zerobull-consulting/dagger#187): the run is cancelled" has "$FAKE/calls" "gh POST repos/o/r/actions/runs/5002/cancel"
check "a rebuild wait that times out (zerobull-consulting/dagger#187): and said to be" has "$tmp/out" "is not awaited and no result is given: cancelled it"
check "a rebuild wait that times out (zerobull-consulting/dagger#187): no PR" test "$(jq length "$FAKE/pulls.json")" = 0
setup
printf 'failure\nsuccess\n' >"$FAKE/conclusions"
run verify v1.0.0-beta.26 "$d2"
ARGS=(rebuild v1.0.0-beta.26 "$d2"); rc env BOT_TOKEN="$bot_secret" FAKE_JOBS_FAIL=1
check "a rebuild jobs read that fails (zerobull-consulting/dagger#187): is an error" test "$status" -ne 0
check "a rebuild jobs read that fails (zerobull-consulting/dagger#187): the cancel is tried" has "$FAKE/calls" "gh POST repos/o/r/actions/runs/5002/cancel"
check "a rebuild jobs read that fails (zerobull-consulting/dagger#187): the run is said to have ended" has "$tmp/out" "ended 'success' and no result is given: nothing to cancel"
check "a rebuild jobs read that fails (zerobull-consulting/dagger#187): no PR" test "$(jq length "$FAKE/pulls.json")" = 0
# A rebuild whose verdict is read: nothing is cancelled.
setup
printf 'failure\nfailure\n' >"$FAKE/conclusions"
run verify v1.0.0-beta.26 "$d2"
run_bot rebuild v1.0.0-beta.26 "$d2"
check "a rebuild verdict that is read (zerobull-consulting/dagger#187): is given" has "$tmp/out" "result=fail"
check "a rebuild verdict that is read (zerobull-consulting/dagger#187): nothing is cancelled" not has "$FAKE/calls" "/cancel"
setup
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env RELEASE_CHECK_TEST_REF='x:y'
check "a test ref that isn't a branch name is refused" test "$status" -ne 0
check "a test ref that isn't a branch name: before any dispatch" test "$(writes)" = 0
setup
git -C "$work" push -q origin HEAD:refs/heads/fix/a+b@c
echo success >"$FAKE/conclusions"
ARGS=(verify v1.0.0-beta.26 "$d2"); rc env RELEASE_CHECK_TEST_REF='fix/a+b@c'
check "a valid branch name with odd characters is accepted" has "$tmp/out" "result=pass"

echo "--- a tag that conflicts"
setup
git -C "$up" checkout -q -b conflicting v1.0.0-beta.25
mkdir -p "$up/cmd/cacheservice/compat"
echo "upstream's own" >"$up/cmd/cacheservice/compat/engines.txt"
git -C "$up" add -A && git -C "$up" commit -qm conflicting && git -C "$up" tag -f -a -m conflicting v1.0.0-beta.26 >/dev/null
ARGS=(bundle v1.0.0-beta.26); rc env -u GH_TOKEN
check "conflict: the bundle test isn't run, which is no pass" has "$tmp/out" "bundle=conflict"
check "conflict: nothing compiled" not test -f "$FAKE/go-calls"
git -C "$up" checkout -q main
git -C "$up" tag -f -a -m beta.26 v1.0.0-beta.26 "$(git -C "$up" rev-parse main~1)" >/dev/null

echo "--- the bundle version"
setup
git -C "$up" checkout -q -b odd v1.0.0-beta.25
echo 'const valueBundleVersion uint = 3 // typed now' >"$up/dagql/cache_transfer_types.go"
git -C "$up" commit -qam odd && git -C "$up" tag -f -a -m odd v1.0.0-beta.26 >/dev/null
echo success >"$FAKE/conclusions"
run verify v1.0.0-beta.26 "$d2"
check "advisory: a bundle version it can't read fails the run" test "$status" -ne 0
check "advisory: saying so" has "$tmp/out" "couldn't read dagql's valueBundleVersion"
git -C "$up" checkout -q main
git -C "$up" tag -f -a -m beta.26 v1.0.0-beta.26 "$(git -C "$up" rev-parse main~1)" >/dev/null

echo "--- dry run of a listed version"
setup
run --dry-run accept v1.0.0-beta.25 "$d1" https://github.com/o/r/actions/runs/7
check "dry run listed: accept still shows its PR" has "$tmp/out" "dry run: would open a PR from release-check/accept/v1.0.0-beta.25"
check "dry run listed: and says the edit would be refused" has "$tmp/out" "is listed already"
run --dry-run reject v1.0.0-beta.25
check "dry run listed: reject still shows its PR" has "$tmp/out" "dry run: would open a PR from release-check/reject/v1.0.0-beta.25"
check "dry run listed: nothing written" test "$(writes)" = 0

echo "--- dry run"
setup
echo success >"$FAKE/conclusions"
run --dry-run verify v1.0.0-beta.26 "$d2"
check "dry run: verify still verifies" has "$tmp/out" "result=pass"
run --dry-run accept v1.0.0-beta.26 "$d2" https://github.com/o/r/actions/runs/5001
check "dry run: accept prints the push" has "$tmp/out" "dry run: would push release-check/accept/v1.0.0-beta.26"
check "dry run: and the PR" has "$tmp/out" "dry run: would open a PR from release-check/accept/v1.0.0-beta.26"
run --dry-run rebuild v1.0.0-beta.26 "$d2"
check "dry run: rebuild merges locally and prints the push" has "$tmp/out" "dry run: would push release-check/rebuild/v1.0.0-beta.26"
run --dry-run reject v1.0.0-beta.26
check "dry run: reject prints the issue" has "$tmp/out" "dry run: would label compat-break and file"
check "dry run: and the reject PR" has "$tmp/out" "dry run: would open a PR from release-check/reject/v1.0.0-beta.26"
ARGS=(--dry-run auto-merge 1); echo '[{"number":1,"state":"open","title":"t","head":{"ref":"release-check/accept/v1.0.0-beta.26","repo":{"full_name":"o/r"}},"base":{"ref":"feature/remote-cache"},"auto_merge":null,"node_id":"x"}]' >"$FAKE/pulls.json"
rc env RELEASE_CHECK_AUTO_MERGE=true
check "dry run: auto-merge prints it" has "$tmp/out" "dry run: would enable squash auto-merge on #1"
run --dry-run publish
check "dry run: publish prints the dispatch" has "$tmp/out" "dry run: would dispatch cacheservice-publish.yml"
check "dry run: the only write is verify's dispatch" test "$(writes)" = 1
check "dry run: no branch pushed" test -z "$(git -C "$FAKE/origin.git" for-each-ref refs/heads/release-check/)"
run accept v1.0.0-beta.26 "$d2" https://github.com/o/r/actions/runs/5001
check "no bot token: accept fails" test "$status" -ne 0
check "no bot token: and says the token is missing" has "$tmp/out" "BOT_TOKEN is not set"

echo
if ((failures > 0)); then
	echo "$failures failed"
	exit 1
fi
echo "all passed"
