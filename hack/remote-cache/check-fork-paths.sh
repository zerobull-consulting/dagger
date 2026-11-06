#!/usr/bin/env bash
# Fails if the fork changes any path, relative to the upstream commit it last
# merged, that hack/remote-cache/fork-paths.txt doesn't allow
# (zerobull-consulting/dagger#117, D7).
#
# Usage: hack/remote-cache/check-fork-paths.sh
#
# The base is the merge base of HEAD with upstream main: the upstream commit
# the fork last merged. Upstream tags its releases on main, so after a merge
# from a release tag the merge base is that tag's commit, or a newer main
# commit if main was merged since. A v1.* tag merged from off main would make
# two unrelated bases; the check stops and says so rather than guess.
#
# Environment (all optional):
#   UPSTREAM_URL     upstream repository (default https://github.com/dagger/dagger)
#   UPSTREAM_BRANCH  upstream branch the fork tracks (default main)
#   FORK_REMOTE      remote to deepen this checkout from when it's shallow (default origin)
#   FORK_BASE        skip finding the base and use this commit
#   FORK_PATHS_FILE  the allowlist (default hack/remote-cache/fork-paths.txt)
#   FETCH_DEPTH      first fetch depth of upstream (default 200)
#   DEEPEN_STEP      commits added per deepening (default 500)
#   DEEPEN_TRIES     deepenings before giving up (default 4)
#   FETCH_TIMEOUT    seconds per git network call (default 300)
set -euo pipefail

UPSTREAM_URL=${UPSTREAM_URL:-https://github.com/dagger/dagger}
UPSTREAM_BRANCH=${UPSTREAM_BRANCH:-main}
FORK_REMOTE=${FORK_REMOTE:-origin}
FETCH_DEPTH=${FETCH_DEPTH:-200}
DEEPEN_STEP=${DEEPEN_STEP:-500}
DEEPEN_TRIES=${DEEPEN_TRIES:-4}
FETCH_TIMEOUT=${FETCH_TIMEOUT:-300}
export GIT_TERMINAL_PROMPT=0

root=$(git rev-parse --show-toplevel)
cd "$root"
FORK_PATHS_FILE=${FORK_PATHS_FILE:-hack/remote-cache/fork-paths.txt}
upstream_ref=refs/fork-paths/upstream

die() { echo "check-fork-paths: $*" >&2; exit 2; }
net() { timeout --kill-after=10 "$FETCH_TIMEOUT" "$@"; }

head=$(git rev-parse --verify HEAD^{commit})

# --- The base ------------------------------------------------------------

find_base() {
  local depth_flag=(--depth="$FETCH_DEPTH")
  if [[ $(git rev-parse --is-shallow-repository) == false ]]; then
    # A full clone has the base already if upstream's history is in it; a
    # shallow upstream fetch would make the repository shallow, so fetch the
    # branch as it is.
    depth_flag=()
  fi
  echo "Fetching $UPSTREAM_URL $UPSTREAM_BRANCH ${depth_flag[*]}" >&2
  net git fetch --quiet --no-tags "${depth_flag[@]}" "$UPSTREAM_URL" \
    "+refs/heads/$UPSTREAM_BRANCH:$upstream_ref" >&2 \
    || die "couldn't fetch $UPSTREAM_URL $UPSTREAM_BRANCH within ${FETCH_TIMEOUT}s"

  local try base
  for ((try = 0; ; try++)); do
    if base=$(git merge-base "$head" "$upstream_ref"); then
      echo "$base"
      return
    fi
    if ((try >= DEEPEN_TRIES)); then
      die "no merge base with $UPSTREAM_URL $UPSTREAM_BRANCH within the fetched history after $DEEPEN_TRIES deepenings of $DEEPEN_STEP; merge upstream, raise DEEPEN_TRIES, or set FORK_BASE"
    fi
    echo "No merge base yet; deepening both histories by $DEEPEN_STEP" >&2
    net git fetch --quiet --no-tags --deepen="$DEEPEN_STEP" "$UPSTREAM_URL" \
      "+refs/heads/$UPSTREAM_BRANCH:$upstream_ref" >&2 \
      || die "couldn't deepen $UPSTREAM_URL within ${FETCH_TIMEOUT}s"
    if [[ $(git rev-parse --is-shallow-repository) == true ]]; then
      # By commit, so a pull request's merge commit deepens too; a local
      # commit the remote doesn't have falls back to the base branch.
      net git fetch --quiet --no-tags --deepen="$DEEPEN_STEP" "$FORK_REMOTE" "$head" >&2 \
        || net git fetch --quiet --no-tags --deepen="$DEEPEN_STEP" "$FORK_REMOTE" feature/remote-cache >&2 \
        || die "couldn't deepen $FORK_REMOTE within ${FETCH_TIMEOUT}s"
    fi
  done
}

# Upstream v1.* release tags HEAD contains. Each is a base candidate: on
# main it's an ancestor of the merge base and drops out below; from off main
# it stays, and the check stops.
merged_tags() {
  net git ls-remote --tags --refs "$UPSTREAM_URL" 'refs/tags/v1.*' \
    || die "couldn't list $UPSTREAM_URL's tags within ${FETCH_TIMEOUT}s"
}

if [[ -n ${FORK_BASE:-} ]]; then
  base=$(git rev-parse --verify "$FORK_BASE^{commit}") || die "FORK_BASE $FORK_BASE isn't a commit here"
  echo "Base: $base (FORK_BASE)"
else
  base=$(find_base)
  candidates=("$base")
  while read -r sha ref; do
    [[ $ref == refs/tags/v1.* ]] || continue
    git cat-file -e "$sha^{commit}" 2>/dev/null || continue
    git merge-base --is-ancestor "$sha" "$head" || continue
    candidates+=("$sha")
  done < <(merged_tags)
  mapfile -t independent < <(git merge-base --independent "${candidates[@]}")
  if ((${#independent[@]} != 1)); then
    {
      echo "check-fork-paths: HEAD merged upstream commits no single base contains:"
      for c in "${independent[@]}"; do git log -1 --format='  %H %s' "$c"; done
      echo "A release tag from off $UPSTREAM_BRANCH? Set FORK_BASE to the base to measure against."
    } >&2
    exit 2
  fi
  base=${independent[0]}
  echo "Base: $(git log -1 --format='%H (%cs) %s' "$base")"
  echo "      the merge base with $UPSTREAM_URL $UPSTREAM_BRANCH, $(git rev-parse --short "$upstream_ref") as fetched"
fi

# --- The allowlist -------------------------------------------------------

[[ -f $FORK_PATHS_FILE ]] || die "no allowlist at $FORK_PATHS_FILE"
rules=() paths=() used=()
lineno=0
while IFS= read -r line || [[ -n $line ]]; do
  lineno=$((lineno + 1))
  [[ $line =~ ^[[:space:]]*(#|$) ]] && continue
  read -r rule path reason <<<"$line"
  case $rule in
    any | deleted | go-mod | add-lines) ;;
    *) die "$FORK_PATHS_FILE:$lineno: unknown rule '$rule'" ;;
  esac
  [[ -n ${reason:-} ]] || die "$FORK_PATHS_FILE:$lineno: '$path' has no reason"
  [[ $path != /* && $path != *'*'* && $path != *..* ]] || die "$FORK_PATHS_FILE:$lineno: '$path' must be a plain relative path, no globs"
  if [[ $path == */ && $rule != any ]]; then
    die "$FORK_PATHS_FILE:$lineno: '$rule' takes a file, not a directory"
  fi
  rules+=("$rule") paths+=("$path") used+=(0)
done <"$FORK_PATHS_FILE"

# --- The rules -----------------------------------------------------------

# go.mod: every directive but require unchanged, and every upstream
# requirement kept at its version.
check_go_mod() {
  local path=$1 status=$2 tmp diff
  [[ $status == M ]] || { echo "the fork ${status_word[$status]:-changes} it; only additions to its requirements are allowed"; return 1; }
  tmp=$(mktemp -d)
  git show "$base:$path" >"$tmp/base.mod"
  git show "$head:$path" >"$tmp/head.mod"
  go mod edit -json "$tmp/base.mod" >"$tmp/base.json"
  go mod edit -json "$tmp/head.mod" >"$tmp/head.json"
  diff=$(jq -rn --slurpfile b "$tmp/base.json" --slurpfile h "$tmp/head.json" '
    ($b[0]) as $b | ($h[0]) as $h
    | ([$h.Require[]? | {(.Path): .Version}] | add // {}) as $hv
    | ($b | del(.Require)) as $bd | ($h | del(.Require)) as $hd
    | [$b.Require[]? | select($hv[.Path] != .Version)
        | "requires \(.Path) \($hv[.Path] // "(removed)") where upstream requires \(.Version)"]
      + [[$bd, $hd | keys[]] | unique[] | select($bd[.] != $hd[.])
        | "changes its \(.) directive"]
    | .[]')
  rm -rf "$tmp"
  [[ -z $diff ]] || { echo "$diff" | paste -sd ';' - | sed 's/;/; /g'; return 1; }
}

# add-lines: no line of upstream's removed or changed.
check_add_lines() {
  local path=$1 status=$2 removed
  [[ $status == M ]] || { echo "the fork ${status_word[$status]:-changes} it; only added lines are allowed"; return 1; }
  removed=$(git diff --no-ext-diff --numstat "$base" "$head" -- "$path" | cut -f2)
  [[ $removed =~ ^[0-9]+$ ]] || { echo "is binary; only added lines of a text file are allowed"; return 1; }
  ((removed == 0)) || { echo "removes or changes $removed of upstream's lines; only added lines are allowed"; return 1; }
}

declare -A status_word=([A]=adds [D]=deletes [M]=changes [T]="changes the type of")

offending=()
while IFS=$'\t' read -r status path; do
  status=${status:0:1}
  match=-
  for i in "${!paths[@]}"; do
    p=${paths[$i]}
    if [[ $path == "$p" || ($p == */ && $path == "$p"*) ]]; then
      match=$i
      break
    fi
  done
  if [[ $match == - ]]; then
    offending+=("$path: ${status_word[$status]:-changes} a path no entry allows")
    continue
  fi
  used[$match]=1
  why=""
  case ${rules[$match]} in
    any) ;;
    deleted) [[ $status == D ]] || why="the entry allows deleting it, and the fork ${status_word[$status]:-changes} it" ;;
    go-mod) why=$(check_go_mod "$path" "$status") || true ;;
    add-lines) why=$(check_add_lines "$path" "$status") || true ;;
  esac
  [[ -z $why ]] || offending+=("$path: $why")
done < <(git diff --no-renames --name-status "$base" "$head")

stale=()
for i in "${!paths[@]}"; do
  ((used[i])) || stale+=("${rules[$i]} ${paths[$i]}")
done

changed=$(git diff --no-renames --name-only "$base" "$head" | wc -l)
echo "Changed paths: $changed; allowlist entries: ${#paths[@]}"

if ((${#offending[@]} == 0 && ${#stale[@]} == 0)); then
  echo "OK: every path the fork changes is allowed by $FORK_PATHS_FILE."
  exit 0
fi

if ((${#offending[@]})); then
  echo
  echo "The fork changes paths outside its allowlist:"
  printf '  %s\n' "${offending[@]}"
  cat <<EOF

Changes outside the service's paths bring back merge conflicts with upstream
and make the service harder to extract (zerobull-consulting/dagger#117, D7).
To fix, either:
  - move the change under cmd/cacheservice/, hack/remote-cache/ or the sidecar
    action, or into one of the fork's own workflows;
  - upstream it to dagger/dagger and drop it here; or
  - if the fork must carry it, add an entry with its reason to
    $FORK_PATHS_FILE, and say why in the pull request.
EOF
fi
if ((${#stale[@]})); then
  echo
  echo "Allowlist entries no changed path needs (remove them from $FORK_PATHS_FILE):"
  printf '  %s\n' "${stale[@]}"
fi
exit 1
