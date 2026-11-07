#!/usr/bin/env bash
# Tests of release-tags.sh, offline: a scratch git repository whose commits
# carry manifests, and a fake docker whose registry holds the commit tags
# listed in $FAKE_PUSHED. Run it from anywhere:
#
#   hack/remote-cache/release-tags_test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
failures=0
check() {
	local what="$1"
	shift
	if "$@"; then
		echo "ok    $what"
	else
		echo "FAIL  $what"
		failures=$((failures + 1))
	fi
}
not() { ! "$@"; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# --- a fake docker: inspect finds the commit tags in FAKE_PUSHED (or fails
# with FAKE_ERROR), and every create is recorded ---
mkdir -p "$tmp/bin"
cat >"$tmp/bin/docker" <<'EOF'
#!/usr/bin/env bash
case "$1 $2 $3" in
"buildx imagetools inspect")
	[[ -n "${FAKE_ERROR:-}" ]] && { echo "$FAKE_ERROR" >&2; exit 1; }
	tag="${4##*:}"
	[[ " $FAKE_PUSHED " == *" $tag "* ]] && { echo "Name: $4"; exit 0; }
	[[ -n "${FAKE_MISSING:-}" ]] && { echo "$FAKE_MISSING" >&2; exit 1; }
	echo "ERROR: $4: not found" >&2
	exit 1
	;;
"buildx imagetools create")
	# FAKE_FAIL_TAG fails that tag's copy, FAKE_FAIL_TIMES times (default:
	# always).
	if [[ "${@: -2:1}" == "${FAKE_FAIL_TAG:-}" ]]; then
		n=$(($(cat "$FAKE_DIR/fails" 2>/dev/null || echo 0) + 1))
		echo "$n" >"$FAKE_DIR/fails"
		((n <= ${FAKE_FAIL_TIMES:-99})) && { echo "ERROR: unexpected status 503" >&2; exit 1; }
	fi
	echo "${@: -2:1} <- ${@: -1}" >>"$FAKE_DIR/created"
	;;
*) exit 1 ;;
esac
EOF
chmod +x "$tmp/bin/docker"

# --- a branch: c1 accepts beta.24 only; c2 accepts beta.25 and beta.24; a
# side branch, accepting beta.26, merged into it (its own commit must never
# be picked; the merge commit carries its manifest); c3, the head, accepts
# only beta.25 ---
digest="sha256:$(printf '0%.0s' {1..64})"
run="https://github.com/o/r/actions/runs/1"
manifest() {
	echo "base v1.0.0-beta.25"
	local v
	for v in "$@"; do echo "accept $v $digest verified: $run"; done
}
repo="$tmp/repo"
mkdir -p "$repo/cmd/cacheservice/compat"
git -C "$repo" init -q -b main
git -C "$repo" config user.email t@example.com
git -C "$repo" config user.name t
commit() {
	shift_msg="$1"
	shift
	manifest "$@" >"$repo/cmd/cacheservice/compat/engines.txt"
	git -C "$repo" add -A
	git -C "$repo" commit -q -m "$shift_msg"
	git -C "$repo" rev-parse HEAD
}
c1="$(commit c1 v1.0.0-beta.24)"
c2="$(commit c2 v1.0.0-beta.25 v1.0.0-beta.24)"
git -C "$repo" checkout -q -b side
side="$(commit side v1.0.0-beta.26)"
git -C "$repo" checkout -q main
git -C "$repo" merge -q --no-ff --no-edit side >/dev/null
manifest v1.0.0-beta.25 >"$repo/cmd/cacheservice/compat/engines.txt"
git -C "$repo" add -A && git -C "$repo" commit -q -m c3
c3="$(git -C "$repo" rev-parse HEAD)"
merge="$(git -C "$repo" rev-parse HEAD~1)"

# tags [VAR=value...]: a run against the branch; its output in $tmp/out,
# the creates in $tmp/fake/created.
tags() {
	rm -rf "$tmp/fake" && mkdir -p "$tmp/fake"
	(
		cd "$repo" || exit 1
		export PATH="$tmp/bin:$PATH" FAKE_DIR="$tmp/fake" FAKE_PUSHED="" IMAGE=r/new OLD_IMAGE=r/old RELEASE_TAGS_RETRY_DELAY=0
		(($#)) && export "$@"
		"$here/release-tags.sh"
	) >"$tmp/out" 2>&1
}
created() { cat "$tmp/fake/created" 2>/dev/null | tr '\n' ';'; }

check "a verified head: its tags move to it, one per accept line of its manifest" tags FAKE_PUSHED="$c1 $c2 $c3"
check "... beta.25 only, and the old name's :feature-remote-cache" \
	[ "$(created)" = "r/new:v1.0.0-beta.25 <- r/new:$c3;r/old:feature-remote-cache <- r/new:$c3;" ]
check "... a version the head dropped (beta.24) keeps its tag" not grep -q 'beta.24' "$tmp/fake/created"

check "a head not verified (red, or still running): the newest verified build before it" tags FAKE_PUSHED="$c1 $c2"
check "... gets the tags of its own manifest" \
	[ "$(created)" = "r/new:v1.0.0-beta.25 <- r/new:$c2;r/new:v1.0.0-beta.24 <- r/new:$c2;r/old:feature-remote-cache <- r/new:$c2;" ]

check "a merged branch's commit is never picked, only the branch's own (first parents)" tags FAKE_PUSHED="$side $c1"
check "... c1, not the side commit" grep -q "r/new:v1.0.0-beta.24 -> $c1" "$tmp/out"
check "... and nothing of the side commit's" not grep -q "$side" "$tmp/fake/created"

check "the merge commit itself counts when verified" tags FAKE_PUSHED="$merge $c1"
check "... with its own manifest's tags (the side branch's beta.26)" grep -q "r/new:v1.0.0-beta.26 -> $merge" "$tmp/out"

check "no verified build in reach: nothing moves, and it succeeds" tags FAKE_PUSHED=""
check "... nothing created" [ -z "$(created)" ]
check "... says so" grep -q "nothing to move" "$tmp/out"
check "a depth that stops before the verified build: nothing moves" tags FAKE_PUSHED="$c1" RELEASE_TAGS_DEPTH=2
check "... nothing created" [ -z "$(created)" ]

check "a registry error other than not found fails it" not tags FAKE_ERROR="unauthorized: authentication required"
check "... before it moves anything" [ -z "$(created)" ]
check "... and says why" grep -q "moving nothing" "$tmp/out"

check "a missing tag reported as MANIFEST_UNKNOWN is a missing tag too" tags FAKE_PUSHED="$c2" FAKE_MISSING="ERROR: unexpected status from HEAD request: 404 Not Found: MANIFEST_UNKNOWN: manifest unknown"
check "... the walk goes on to the verified build" grep -q "r/new:v1.0.0-beta.25 -> $c2" "$tmp/out"

check "a copy that fails twice, then works: retried, and it succeeds" tags FAKE_PUSHED="$c2" FAKE_FAIL_TAG=r/new:v1.0.0-beta.25 FAKE_FAIL_TIMES=2
check "... every tag moved" \
	[ "$(created)" = "r/new:v1.0.0-beta.25 <- r/new:$c2;r/new:v1.0.0-beta.24 <- r/new:$c2;r/old:feature-remote-cache <- r/new:$c2;" ]
check "a copy that keeps failing fails the run" not tags FAKE_PUSHED="$c2" FAKE_FAIL_TAG=r/new:v1.0.0-beta.25
check "... after 3 tries" [ "$(cat "$tmp/fake/fails")" = 3 ]
check "... the other tags still move" \
	[ "$(created)" = "r/new:v1.0.0-beta.24 <- r/new:$c2;r/old:feature-remote-cache <- r/new:$c2;" ]
check "... and it names the stale one, and the way out" grep -q "left where they were: r/new:v1.0.0-beta.25. Re-run this job" "$tmp/out"

check "a bad depth fails it" not tags RELEASE_TAGS_DEPTH=x

if ((failures > 0)); then
	echo "$failures test(s) failed"
	exit 1
fi
echo "all tests passed"
