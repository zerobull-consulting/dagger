#!/usr/bin/env bash
# Tests of check-shim-pin.sh, offline: a local bare repository stands in for
# the fork's origin and a clone of it for the checkout. Run it from anywhere:
#
#   hack/remote-cache/check-shim-pin_test.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
script="$here/check-shim-pin.sh"
failures=0
check() {
	local what="$1"
	shift
	if "$@" >/dev/null 2>&1; then
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
git config --global init.defaultBranch main
git config --global protocol.file.allow always

uses=zerobull-consulting/dagger/.github/actions/remote-cache/start/run
af=.github/actions/remote-cache/start/action.yml

# origin: feature/remote-cache with c1 <- c2 <- c3, and a side branch off c1
# whose commit s1 is not on it.
git init -q "$tmp/seed"
cd "$tmp/seed"
git checkout -q -b feature/remote-cache
mkdir -p "$(dirname "$af")"
echo a >a
git add a && git commit -qm c1
c1=$(git rev-parse HEAD)
echo b >b
git add b && git commit -qm c2
c2=$(git rev-parse HEAD)
echo c >c
git add c && git commit -qm c3
git checkout -q -b side "$c1"
echo s >s
git add s && git commit -qm s1
s1=$(git rev-parse HEAD)
git checkout -q feature/remote-cache
git clone -q --bare "$tmp/seed" "$tmp/origin.git"

# work <name> <pin line>: a checkout (cloned from origin) whose action.yml has
# the given content.
work() {
	local name="$1"
	git clone -q --no-checkout "file://$tmp/origin.git" "$tmp/$name"
	git -C "$tmp/$name" checkout -q feature/remote-cache
	mkdir -p "$tmp/$name/$(dirname "$af")"
	printf '%s\n' "$2" >"$tmp/$name/$af"
}
run() { (cd "$tmp/$1" && shift && env "$@" "$script"); }
runerr() { (cd "$tmp/$1" && shift && env "$@" "$script" 2>&1 >/dev/null); }

work ok "      uses: $uses@$c2 # comment"
check "a pin that is an ancestor passes" run ok FETCH_TIMEOUT=30

work tip "- uses: $uses@$(git -C "$tmp/seed" rev-parse feature/remote-cache)"
check "a pin at the branch tip passes" run tip FETCH_TIMEOUT=30

work side "      uses: $uses@$s1"
check "a pin on a side branch fails" not run side FETCH_TIMEOUT=30
check "  ...and says what to do" grep -q "change the shim, merge, then re-pin" <(runerr side FETCH_TIMEOUT=30)

work short "      uses: $uses@${c2:0:7}"
check "a short id fails" not run short FETCH_TIMEOUT=30
check "  ...and says it isn't a full id" grep -q "not a full 40-character" <(runerr short FETCH_TIMEOUT=30)

work tag "      uses: $uses@v1.0.0"
check "a tag name fails" not run tag FETCH_TIMEOUT=30
check "  ...and says it isn't a full id" grep -q "not a full 40-character" <(runerr tag FETCH_TIMEOUT=30)

work upper "      uses: $uses@$(tr a-f A-F <<<"$c2")"
check "an upper-case id fails" not run upper FETCH_TIMEOUT=30

work none "      uses: someone/else/.github/actions/x@$c2"
check "a missing uses line fails" not run none FETCH_TIMEOUT=30
check "  ...and says so" grep -q "expected one 'uses:" <(runerr none FETCH_TIMEOUT=30)

work two "$(printf '      uses: %s@%s\n      uses: %s@%s' "$uses" "$c2" "$uses" "$c1")"
check "two uses lines fail" not run two FETCH_TIMEOUT=30

work unknown "      uses: $uses@$(printf '0%.0s' {1..40})"
check "a full id no branch has fails" not run unknown FETCH_TIMEOUT=30

check "a missing action file fails" not run ok PIN_FILE=nope.yml FETCH_TIMEOUT=30
check "an unreachable remote fails" not run ok FORK_REMOTE=nowhere FETCH_TIMEOUT=30
check "a bad timeout fails" not run ok FETCH_TIMEOUT=abc

# An old ancestor behind a shallow fetch is found by deepening.
work deep "      uses: $uses@$c1"
check "a shallow fetch is deepened to find an old ancestor" run deep FETCH_DEPTH=1 DEEPEN_STEP=1 DEEPEN_TRIES=5 FETCH_TIMEOUT=30
work deep0 "      uses: $uses@$c1"
check "  ...but only DEEPEN_TRIES times" not run deep0 FETCH_DEPTH=1 DEEPEN_STEP=1 DEEPEN_TRIES=1 FETCH_TIMEOUT=30
work deep1 "      uses: $uses@$c1"
check "  ...and says it gave up on the pin, not on a bad setting" grep -q "is not on feature/remote-cache" <(runerr deep1 FETCH_DEPTH=1 DEEPEN_STEP=1 DEEPEN_TRIES=1 FETCH_TIMEOUT=30)
check "DEEPEN_TRIES=0 is refused as a setting" not run deep0 DEEPEN_TRIES=0 FETCH_TIMEOUT=30

if ((failures)); then
	echo "$failures failed" >&2
	exit 1
fi
echo "all passed"
