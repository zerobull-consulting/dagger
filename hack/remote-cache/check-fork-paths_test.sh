#!/usr/bin/env bash
# Tests check-fork-paths.sh against throwaway repositories: a fake upstream
# and a fork of it. No network.
#
# Usage: hack/remote-cache/check-fork-paths_test.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
check=$here/check-fork-paths.sh
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

export GIT_AUTHOR_NAME=test GIT_AUTHOR_EMAIL=test@example.com
export GIT_COMMITTER_NAME=test GIT_COMMITTER_EMAIL=test@example.com
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
g() { git -c init.defaultBranch=main -c commit.gpgsign=false "$@"; }

# Upstream: an engine file, a workflow, go.mod and go.sum, and a release tag.
up=$work/upstream
g init -q "$up"
mkdir -p "$up/engine" "$up/.github/workflows"
echo 'package engine' >"$up/engine/engine.go"
echo 'name: changelog' >"$up/.github/workflows/changelog.yml"
cat >"$up/go.mod" <<'EOF'
module example.com/up

go 1.22

require (
	example.com/a v1.0.0
	example.com/b v1.0.0 // indirect
)

replace example.com/sdk => ./sdk
EOF
printf 'example.com/a v1.0.0 h1:a=\nexample.com/b v1.0.0 h1:b=\n' >"$up/go.sum"
printf '.env\n' >"$up/.gitignore"
g -C "$up" add -A
g -C "$up" commit -qm 'upstream 1'
g -C "$up" tag v1.0.0-beta.1

allowlist='any cmd/cacheservice/ the service
deleted .github/workflows/changelog.yml never runs here
go-mod go.mod the service'"'"'s dependencies
add-lines go.sum their checksums'

# The fork: a service file, the workflow deleted, and a requirement added.
fork=$work/fork
g clone -q "$up" "$fork"
mkdir -p "$fork/cmd/cacheservice"
echo 'package main' >"$fork/cmd/cacheservice/main.go"
g -C "$fork" rm -q .github/workflows/changelog.yml
sed -i 's#^\texample.com/a v1.0.0$#\texample.com/a v1.0.0\n\texample.com/s3 v1.2.0#' "$fork/go.mod"
echo 'example.com/s3 v1.2.0 h1:s=' >>"$fork/go.sum"
echo "$allowlist" >"$fork/cmd/cacheservice/allow.txt"
g -C "$fork" add -A
g -C "$fork" commit -qm 'fork 1'
forkbase=$(g -C "$fork" rev-parse HEAD)

pass=0 fail=0
# expect <name> <0|1|2> <output pattern> [-- <commands to run in the fork first>]
expect() {
  local name=$1 want=$2 pattern=$3 out rc
  shift 3
  g -C "$fork" checkout -q -B "case-$pass-$fail" "$forkbase"
  if (($#)); then
    (cd "$fork" && eval "$*") >/dev/null
    g -C "$fork" add -A
    g -C "$fork" commit -qm "$name" --allow-empty
  fi
  rc=0
  out=$(cd "$fork" && UPSTREAM_URL=$up FORK_PATHS_FILE=cmd/cacheservice/allow.txt "$check" 2>&1) || rc=$?
  if [[ $rc == "$want" ]] && grep -qE -- "$pattern" <<<"$out"; then
    pass=$((pass + 1))
    echo "ok   $name"
  else
    fail=$((fail + 1))
    echo "FAIL $name: exit $rc, want $want and /$pattern/"
    sed 's/^/     | /' <<<"$out"
  fi
}

expect 'the fork as it is passes' 0 'OK: every path'
expect 'reports the base it found' 0 "Base: $(g -C "$up" rev-parse HEAD)"
expect 'a change under the service passes' 0 'OK' \
  'echo "// more" >>cmd/cacheservice/main.go'
expect 'a change to engine/ fails and names it' 1 'engine/engine\.go: changes a path no entry allows' \
  'echo "// fork" >>engine/engine.go'
expect 'a new file outside the allowlist fails' 1 'dagql/x\.go: adds' \
  'mkdir -p dagql && echo x >dagql/x.go'
expect 'the failure says how to fix it' 1 'upstream it to dagger/dagger' \
  'echo "// fork" >>engine/engine.go'
expect 'a deleted-only entry refuses a re-added file' 1 'changelog\.yml: the entry allows deleting it' \
  'mkdir -p .github/workflows && echo again >.github/workflows/changelog.yml'
expect 'go.mod: changing an upstream version fails' 1 'requires example\.com/a v1\.1\.0 where upstream requires v1\.0\.0' \
  'sed -i "s#example.com/a v1.0.0#example.com/a v1.1.0#" go.mod'
expect 'go.mod: removing an upstream requirement fails' 1 'requires example\.com/b \(removed\)' \
  'sed -i "/example.com\/b/d" go.mod'
expect 'go.mod: changing a replace fails' 1 'changes its Replace directive' \
  'sed -i "s#=> ./sdk#=> ./sdk2#" go.mod'
expect 'go.mod: changing the go version fails' 1 'changes its Go directive' \
  'sed -i "s#^go 1.22#go 1.23#" go.mod'
expect 'go.mod: dropping // indirect passes' 0 'OK' \
  'sed -i "s#example.com/b v1.0.0 // indirect#example.com/b v1.0.0#" go.mod'
expect 'go.sum: removing an upstream line fails' 1 'go\.sum: removes or changes 1 of upstream' \
  'sed -i "/example.com\/b/d" go.sum'
expect 'an entry without a reason is refused' 2 "'cmd/cacheservice/' has no reason" \
  'sed -i "s#^any cmd/cacheservice/ the service#any cmd/cacheservice/#" cmd/cacheservice/allow.txt'
expect 'a glob is refused' 2 'no globs' \
  'echo "any engine/* nope" >>cmd/cacheservice/allow.txt'
expect 'an entry nothing needs fails' 1 'entries no changed path needs.*|any \.gitignore' \
  'echo "any .gitignore not changed" >>cmd/cacheservice/allow.txt'
expect 'an unknown rule is refused' 2 "unknown rule 'maybe'" \
  'echo "maybe engine/ hmm" >>cmd/cacheservice/allow.txt'

# Upstream moves on, and the fork merges it: the base moves to the merge.
echo 'package engine // 2' >"$up/engine/engine.go"
g -C "$up" commit -qam 'upstream 2'
g -C "$fork" checkout -q --detach "$forkbase"
g -C "$fork" fetch -q origin main
g -C "$fork" merge -q --no-edit origin/main
forkbase=$(g -C "$fork" rev-parse HEAD)
expect "after merging upstream, upstream's change isn't the fork's" 0 "Base: $(g -C "$up" rev-parse HEAD)"

# A release tag on main, merged before main moves on: the base is the tag.
echo 'package engine // 3' >"$up/engine/engine.go"
g -C "$up" commit -qam 'upstream 3'
g -C "$up" tag v1.0.0-beta.3
tag3=$(g -C "$up" rev-parse HEAD)
echo 'package engine // 4' >"$up/engine/engine.go"
g -C "$up" commit -qam 'upstream 4'
g -C "$fork" checkout -q --detach "$forkbase"
g -C "$fork" fetch -q origin tag v1.0.0-beta.3
g -C "$fork" merge -q --no-edit v1.0.0-beta.3
forkbase=$(g -C "$fork" rev-parse HEAD)
expect 'after merging a release tag on main, the base is the tag' 0 "Base: $tag3"

# A release tag from off main, merged into the fork: two bases, so it stops.
g -C "$up" checkout -q -b release v1.0.0-beta.1
echo 'package engine // hotfix' >"$up/engine/hotfix.go"
g -C "$up" add -A
g -C "$up" commit -qm 'hotfix'
g -C "$up" tag v1.0.0-beta.2
g -C "$up" checkout -q main
g -C "$fork" checkout -q --detach "$forkbase"
g -C "$fork" fetch -q origin tag v1.0.0-beta.2
g -C "$fork" merge -q --no-edit v1.0.0-beta.2
forkbase=$(g -C "$fork" rev-parse HEAD)
expect 'a tag merged from off main stops the check' 2 'no single base contains'

echo "$pass passed, $fail failed"
((fail == 0))
