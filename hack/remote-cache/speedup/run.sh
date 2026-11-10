#!/usr/bin/env bash
# Times one Dagger pipeline that is slow to run and quick to fetch from a remote
# cache, so a speedup shows in the job's wall time: it builds cmd/cacheservice
# in a golang container, from a fixed set of the repository's directories. The
# build is one cacheable step, whose result is only the binary (the module and
# build caches are temporary mounts, so they aren't part of it).
#
#   run.sh <label> <dir> [<service container>]
#
# It runs the pipeline from the repository root with the dagger CLI first on
# PATH (or DAGGER), and writes <dir>/<label>.seconds and <dir>/<label>.size.
# With a service container (the sidecar's), it also waits for the service's
# cache spans of the run and writes <dir>/<label>.spans ("<hit> <executed>").
#
# The same commit gives the same cache key on any runner: the step's inputs are
# the image, the files under the included paths, and the command.
set -euo pipefail

[[ $# -ge 2 && $# -le 3 ]] || { sed -n '8,9p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
label="$1" dir="$2" service="${3:-}"
[[ "$label" =~ ^[a-z0-9-]+$ ]] || { echo "run.sh: label: letters, digits and '-' only" >&2; exit 2; }
timeout_s="${SPEEDUP_SPAN_TIMEOUT:-120}"

dagger="$(command -v "${DAGGER:-dagger}")" || { echo "run.sh: no dagger CLI: ${DAGGER:-dagger}" >&2; exit 1; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
mkdir -p "$dir"

# golang at go.mod's version, the image the service's Dockerfile builds with.
# The paths are what ./cmd/cacheservice imports from this module, plus the go.mod
# files of the modules go.mod replaces with local directories.
read -r -d '' pipeline <<'EOF' || true
container |
  from golang:1.26.6@sha256:0d1d3a794be25f809dd2cb3160d8c73276c4056a9f8242a138e908ddeee7b6b6 |
  with-workdir /src |
  with-directory /src $(host | directory .) --include=go.mod,go.sum,sdk/go/go.mod,sdk/go/go.sum,engine/distconsts/go.mod,internal/fastbolt/go.mod,cmd/cacheservice,dagql,engine,internal,util |
  with-mounted-temp /gomod |
  with-mounted-temp /gocache |
  with-env-variable GOMODCACHE /gomod |
  with-env-variable GOCACHE /gocache |
  with-env-variable CGO_ENABLED 0 |
  with-exec -- go build -trimpath -o /out/cacheservice ./cmd/cacheservice |
  file /out/cacheservice |
  size
EOF

start=$SECONDS
size="$(cd "$root" && "$dagger" --progress=plain --no-load-module -c "$pipeline" 2>"$dir/$label.log")" ||
	{ echo "run.sh: the pipeline failed: see $dir/$label.log" >&2; tail -n 30 "$dir/$label.log" >&2; exit 1; }
seconds=$((SECONDS - start))
[[ "$size" =~ ^[0-9]+$ && "$size" -gt 1000000 ]] || { echo "run.sh: not a binary's size: '$size'" >&2; exit 1; }
echo "$seconds" >"$dir/$label.seconds"
echo "$size" >"$dir/$label.size"
echo "$label: built cmd/cacheservice ($size bytes) in ${seconds}s"

[[ -n "$service" ]] || exit 0

# withExec cache spans of the service's whole log, as demo/check.sh counts them;
# the service has one engine, and this is the only pipeline it runs.
spans() {
	timeout 30 docker logs "$service" 2>&1 | awk '
		/msg="cache span"/ && / name=Container\.withExec / {
			if ($0 ~ / outcome=hit /) hit++
			if ($0 ~ / outcome=executed /) executed++
		}
		END { printf "%d %d\n", hit, executed }'
}
deadline=$((SECONDS + timeout_s))
while :; do
	read -r hit executed <<<"$(spans)"
	((hit + executed >= 1)) && break
	((SECONDS < deadline)) || { echo "run.sh: no withExec cache span reached the service within ${timeout_s}s (does the CLI inherit an unsampled TRACEPARENT?)" >&2; exit 1; }
	sleep 1
done
sleep 2
read -r hit executed <<<"$(spans)"
echo "$hit $executed" >"$dir/$label.spans"
echo "$label: Container.withExec spans: $hit hit, $executed executed"
