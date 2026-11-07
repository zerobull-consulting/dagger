#!/usr/bin/env bash
# Fails unless the commit that .github/actions/remote-cache/start/action.yml
# pins its shim (start/run) to is a full commit id that is an ancestor of
# origin/feature/remote-cache (zerobull-consulting/dagger#169).
#
# A composite action can't reference a sibling at its own commit, so the shim
# is pinned by commit. A pin that is on no branch is fetchable only through a
# pull request's head ref; if that ref stopped resolving, every caller's
# remote-cache/start would fail while downloading the shim. See
# .github/actions/remote-cache/README.md, "Changing the shim".
#
# A pull request that changes start/run and the pin together fails this check
# on purpose: its new pin can't be on the base branch yet. Change the shim,
# merge, then re-pin in a following pull request.
#
# Usage: hack/remote-cache/check-shim-pin.sh   (from a git checkout)
#
# Needs git. Environment (all optional):
#   PIN_FILE         the action that holds the pin
#                    (default .github/actions/remote-cache/start/action.yml)
#   SHIM_USES        the shim's `uses:` path, without @<commit>
#                    (default zerobull-consulting/dagger/.github/actions/remote-cache/start/run)
#   FORK_REMOTE      the remote to fetch the branch from (default origin)
#   FORK_BRANCH      the branch the pin must be on (default feature/remote-cache)
#   FETCH_DEPTH      first fetch depth of the branch (default 500)
#   DEEPEN_STEP      commits added per deepening (default 1000)
#   DEEPEN_TRIES     deepenings before giving up (default 5)
#   FETCH_TIMEOUT    seconds per git network call (default 120)
set -euo pipefail

PIN_FILE=${PIN_FILE:-.github/actions/remote-cache/start/action.yml}
SHIM_USES=${SHIM_USES:-zerobull-consulting/dagger/.github/actions/remote-cache/start/run}
FORK_REMOTE=${FORK_REMOTE:-origin}
FORK_BRANCH=${FORK_BRANCH:-feature/remote-cache}
FETCH_DEPTH=${FETCH_DEPTH:-500}
DEEPEN_STEP=${DEEPEN_STEP:-1000}
DEEPEN_TRIES=${DEEPEN_TRIES:-5}
FETCH_TIMEOUT=${FETCH_TIMEOUT:-120}
export GIT_TERMINAL_PROMPT=0

readme=".github/actions/remote-cache/README.md, \"Changing the shim\""
fail() {
	echo "check-shim-pin: $*" >&2
	exit 1
}

[[ -f "$PIN_FILE" ]] || fail "$PIN_FILE not found"
for n in FETCH_DEPTH DEEPEN_STEP DEEPEN_TRIES FETCH_TIMEOUT; do
	[[ "${!n}" =~ ^[1-9][0-9]*$ ]] || fail "$n must be a positive integer"
done
[[ "$FORK_BRANCH" =~ ^[A-Za-z0-9._/-]+$ ]] || fail "FORK_BRANCH has characters a branch name shouldn't"

# The pin: the ref after "@" in the one `uses:` line of the shim.
mapfile -t refs < <(sed -nE "s|^[[:space:]]*(-[[:space:]]+)?uses:[[:space:]]*${SHIM_USES//./\\.}@([^[:space:]#]*).*|\2|p" "$PIN_FILE")
((${#refs[@]} == 1)) ||
	fail "expected one 'uses: $SHIM_USES@<commit>' line in $PIN_FILE, found ${#refs[@]}"
pin=${refs[0]}
[[ "$pin" =~ ^[0-9a-f]{40}$ ]] ||
	fail "the pin '$pin' in $PIN_FILE is not a full 40-character commit id (a short id, tag or branch name can move or be ambiguous); see $readme"

tracking="refs/remotes/$FORK_REMOTE/$FORK_BRANCH"
git_net() { timeout "$FETCH_TIMEOUT" git "$@"; }

# Commits only (no blobs): the question is about history.
git_net fetch --quiet --no-tags --filter=blob:none --depth="$FETCH_DEPTH" \
	"$FORK_REMOTE" "+refs/heads/$FORK_BRANCH:$tracking" ||
	fail "couldn't fetch $FORK_BRANCH from $FORK_REMOTE within ${FETCH_TIMEOUT}s"
tip=$(git rev-parse "$tracking^{commit}")

on_branch() { git cat-file -e "$pin^{commit}" 2>/dev/null && git merge-base --is-ancestor "$pin" "$tracking"; }

tries=0
until on_branch; do
	# A shallow history can hide an old ancestor: deepen, bounded.
	if [[ "$(git rev-parse --is-shallow-repository)" == true ]] && ((tries < DEEPEN_TRIES)); then
		tries=$((tries + 1))
		git_net fetch --quiet --no-tags --filter=blob:none --deepen="$DEEPEN_STEP" \
			"$FORK_REMOTE" "+refs/heads/$FORK_BRANCH:$tracking" ||
			fail "couldn't deepen $FORK_BRANCH from $FORK_REMOTE within ${FETCH_TIMEOUT}s"
		continue
	fi
	cat >&2 <<MSG
check-shim-pin: the pin is not on $FORK_BRANCH.
  pin:    $pin (in $PIN_FILE)
  branch: $FORK_REMOTE/$FORK_BRANCH at $tip
The shim's pin must be a commit already on $FORK_BRANCH, or it is reachable only
through a pull request's head ref. If this pull request changes start/run and the
pin together, that is why it fails: change the shim, merge, then re-pin in a
following pull request. See $readme.
MSG
	exit 1
done

echo "ok: the shim's pin $pin is on $FORK_BRANCH (tip $tip)"
