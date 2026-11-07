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

# disable-auto-merge, against a fake gh.
mkdir -p "$tmp/bin"
cat >"$tmp/bin/gh" <<'G'
#!/usr/bin/env bash
echo "$*" >>"$FAKE_CALLS"
case "$2" in
repos/*/pulls/*) echo "{\"node_id\": \"PR_1\", \"auto_merge\": $FAKE_AUTO_MERGE}" ;;
graphql) echo '{}' ;;
esac
G
chmod +x "$tmp/bin/gh"
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
