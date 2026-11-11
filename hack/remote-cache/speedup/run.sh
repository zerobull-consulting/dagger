#!/usr/bin/env bash
# Runs one of the Dagger pipelines the remote cache speeds up: pipelines whose
# steps are slow to run and quick to fetch from a remote cache.
#
#   run.sh <pipeline> <label> <dir> [<service container>]
#
#   build   a build of cmd/cacheservice in a golang container: one cacheable
#           step, whose result is only the binary (the module and build caches
#           are temporary mounts, so they aren't part of it)
#   unit    the unit job's "Service, client and protocol" tests
#           (go test -race ./cmd/cacheservice/... ./engine/remotecache/...),
#           in three cacheable layers; see unit_pipeline below
#
# It runs the pipeline from the repository root with the dagger CLI first on
# PATH (or DAGGER), and writes its progress to <dir>/<label>.log. With a service
# container (the sidecar's), it also waits for the service's cache spans of the
# run and writes <dir>/<label>.spans ("<hit> <executed>"): the run's own, so a
# service can run several pipelines one after another.
#
# The same commit gives the same cache keys on any runner: each step's inputs
# are the image, the files under the paths it includes, and the command.
#
# SPEEDUP_CA_FILE, a CA bundle, replaces the containers' own, for a host
# behind a TLS-intercepting proxy (the steps download Go modules). It is part
# of every key: such a run shares no cache entry with one without it, CI's.
set -euo pipefail

[[ $# -ge 3 && $# -le 4 ]] || { sed -n '6,6p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
pipeline="$1" label="$2" dir="$3" service="${4:-}"
[[ "$pipeline" == build || "$pipeline" == unit ]] || { echo "run.sh: pipeline: build or unit, not '$pipeline'" >&2; exit 2; }
[[ "$label" =~ ^[a-z0-9-]+$ ]] || { echo "run.sh: label: letters, digits and '-' only" >&2; exit 2; }
timeout_s="${SPEEDUP_SPAN_TIMEOUT:-120}"

dagger="$(command -v "${DAGGER:-dagger}")" || { echo "run.sh: no dagger CLI: ${DAGGER:-dagger}" >&2; exit 1; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
mkdir -p "$dir"
: >"$dir/$label.log"

# golang at go.mod's version: the image the service's Dockerfile builds with,
# read from it so the two can't drift.
golang="$(sed -nE 's/^FROM --platform=\$BUILDPLATFORM (golang:[^ ]+) AS build$/\1/p' "$root/cmd/cacheservice/Dockerfile")"
[[ "$golang" =~ ^golang:[0-9.]+@sha256:[0-9a-f]{64}$ ]] || { echo "run.sh: no golang image in cmd/cacheservice/Dockerfile: '$golang'" >&2; exit 1; }
# The base of every pipeline.
from="from $golang"
if [[ -n "${SPEEDUP_CA_FILE:-}" ]]; then
	ca="$(cd "$(dirname "$SPEEDUP_CA_FILE")" && pwd)/$(basename "$SPEEDUP_CA_FILE")"
	[[ -f "$ca" && "$ca" =~ ^[A-Za-z0-9._/-]+$ ]] || { echo "run.sh: SPEEDUP_CA_FILE: not a file, or an unusual path: '$SPEEDUP_CA_FILE'" >&2; exit 2; }
	from+=" | with-file /etc/ssl/certs/ca-certificates.crt \$(host | file $ca)"
fi

# withExec cache spans of the service's whole log, as demo/check.sh counts them
# ("<hit> <executed>"); the service has one engine.
spans() {
	timeout 30 docker logs "$service" 2>&1 | awk '
		/msg="cache span"/ && / name=Container\.withExec / {
			if ($0 ~ / outcome=hit /) hit++
			if ($0 ~ / outcome=executed /) executed++
		}
		END { printf "%d %d\n", hit, executed }'
}
base_hit=0 base_executed=0
[[ -z "$service" ]] || read -r base_hit base_executed <<<"$(spans)"

# Runs a dagger shell script; prints its result.
dagger_c() {
	local out
	out="$(cd "$root" && "$dagger" --progress=plain --no-load-module -c "$1" 2>>"$dir/$label.log")" || {
		echo "run.sh: the $pipeline pipeline failed: see $dir/$label.log" >&2
		tail -n 60 "$dir/$label.log" >&2
		# The CLI's error trailer can push the failing tests out of the tail.
		if grep -qE -- '--- FAIL|\| (FAIL|panic:)' "$dir/$label.log"; then
			echo "run.sh: the failures, from the log:" >&2
			grep -E -- '--- FAIL|\| (FAIL|panic:)' "$dir/$label.log" | head -n 50 >&2
		fi
		exit 1
	}
	printf '%s\n' "$out"
}

# The paths are what ./cmd/cacheservice imports from this module, plus the go.mod
# files of the modules go.mod replaces with local directories. A new import root
# must be added here, or the build fails in the container.
build_pipeline() {
	local size
	size="$(dagger_c "
container |
  $from |
  with-workdir /src |
  with-directory /src \$(host | directory . --include=go.mod,go.sum,sdk/go/go.mod,sdk/go/go.sum,engine/distconsts/go.mod,internal/fastbolt/go.mod,cmd/cacheservice,dagql,engine,internal,util) |
  with-mounted-temp /gomod |
  with-mounted-temp /gocache |
  with-env-variable GOMODCACHE /gomod |
  with-env-variable GOCACHE /gocache |
  with-env-variable CGO_ENABLED 0 |
  with-exec -- go build -trimpath -o /out/cacheservice ./cmd/cacheservice |
  file /out/cacheservice |
  size")"
	# The dagger CLI prints an Int result of 1e6 or more in exponent notation
	# (6.3700575e+07): normalize it to a plain integer before checking it.
	if [[ "$size" =~ ^[0-9]+(\.[0-9]+)?[eE]\+?[0-9]+$ ]]; then size="$(LC_ALL=C printf '%.0f' "$size")"; fi
	[[ "$size" =~ ^[0-9]+$ && "$size" -gt 1000000 ]] || { echo "run.sh: not a binary's size: '$size'" >&2; exit 1; }
	echo "$label: built cmd/cacheservice ($size bytes) in $((SECONDS - start))s"
}

# The tests, in layers ordered from what changes least to what changes most, so
# a pull request's run fetches the layers its change leaves alone and runs only
# the rest. The modules and the build cache are in the container's filesystem
# (not mounts), so each layer's result carries them to the next, and to the
# remote cache.
#
#   1. toolchain: the modules, and every package outside this module the tests
#      import (the standard library included), compiled with -race. Its key is
#      the go.mod files, the replaced modules' directories, and the package
#      list: it reruns when a dependency, or the set of imported packages,
#      changes. The module zips are deleted (the extracted modules and their
#      hashes are what a build reads), which keeps the layer smaller.
#   2. shared code: this module's packages outside cmd/cacheservice that the
#      tests import (dagql, engine, internal, util), compiled with -race. Its
#      key is exactly their files: it reruns when one of them changes.
#   3. tests: cmd/cacheservice, engine/remotecache and the files the tests read
#      (the remote-cache workflows, actions and scripts, two engine/server
#      files), then go test. Any change to them reruns it.
#
# The lists come from a go list run first, in a step of its own: it reruns on
# any change to the source, and its modules are a temporary mount, so it
# downloads them each time it runs.
unit_pipeline() {
	local tests="./cmd/cacheservice/... ./engine/remotecache/..."
	local tmpl='{{if not .ForTest}}{{if and .Module .Module.Main}}M {{.ImportPath}}{{range .GoFiles}} {{$.Dir}}/{{.}}{{end}}{{range .EmbedFiles}} {{$.Dir}}/{{.}}{{end}}{{else}}E {{.ImportPath}}{{end}}{{end}}'
	local list ext main files
	list="$(dagger_c "
container |
  $from |
  with-workdir /src |
  with-directory /src \$(host | directory . --include=go.mod,go.sum,sdk/go/go.mod,sdk/go/go.sum,cmd/cacheservice,dagql,engine,internal,util) |
  with-mounted-temp /go/pkg/mod |
  with-mounted-temp /root/.cache/go-build |
  with-exec -- go list -deps -test -f '$tmpl' $tests |
  stdout")"
	# E: a package outside this module. M: this module's, with its files. The
	# test variants (ForTest) are skipped, and so are the test mains (.test),
	# whose files are generated.
	ext="$(awk '$1 == "E" && $2 !~ /\.test$/ { print $2 }' <<<"$list" | LC_ALL=C sort -u | paste -sd' ')"
	main="$(awk '$1 == "M" && $2 !~ /\.test$/ && $2 !~ /^github\.com\/dagger\/dagger\/cmd\/cacheservice(\/|$)/ { print $2 }' <<<"$list" | LC_ALL=C sort -u | paste -sd' ')"
	files="$(awk '$1 == "M" && $2 !~ /\.test$/ && $2 !~ /^github\.com\/dagger\/dagger\/cmd\/cacheservice(\/|$)/ {
		for (i = 3; i <= NF; i++) if ($i ~ /^\/src\//) print substr($i, 6)
	}' <<<"$list" | LC_ALL=C sort -u | paste -sd,)"
	[[ -n "$ext" && -n "$main" && -n "$files" ]] || { echo "run.sh: go list named no packages: see $dir/$label.log" >&2; exit 1; }
	# Package paths and file names are plain: no quote, space or comma to escape.
	[[ "$ext $main $files" =~ ^[A-Za-z0-9._/,\ ~+-]+$ ]] || { echo "run.sh: an unexpected character in go list's output" >&2; exit 1; }
	echo "$label: $(wc -w <<<"$ext") packages outside the module, $(wc -w <<<"$main") of its own ($(tr , '\n' <<<"$files" | wc -l) files)"

	dagger_c "
container |
  $from |
  with-workdir /src |
  with-env-variable CGO_ENABLED 1 |
  with-directory /src \$(host | directory . --include=go.mod,go.sum,sdk/go/go.mod,sdk/go/go.sum,engine/distconsts,internal/fastbolt) |
  with-exec -- sh -c 'go build -race \"\$@\" && find /go/pkg/mod/cache/download -name \"*.zip\" -delete' toolchain $ext |
  with-directory /src \$(host | directory . --include=$files) |
  with-exec -- go build -race $main |
  with-directory /src \$(host | directory . --include=cmd/cacheservice,engine/remotecache,engine/server/remote_cache.go,engine/server/engine_status.go,.github/actions/remote-cache,.github/workflows,hack/remote-cache) |
  with-exec -- go test -race -count=1 -cover $tests |
  stdout" >"$dir/$label.test.log"
	cat "$dir/$label.test.log"
	echo "$label: the tests passed in $((SECONDS - start))s (output: $dir/$label.test.log)"
}

start=$SECONDS
"${pipeline}_pipeline"

[[ -n "$service" ]] || exit 0

# The spans arrive a moment after the run: wait for one per withExec of the
# pipeline (build: 1; unit: go list and three layers, 4), each a hit or an
# execution. Polls every 3s, since the verbose log is long. Fewer by the
# deadline is reported as it is; none fails.
expect=1
[[ "$pipeline" == unit ]] && expect=4
deadline=$((SECONDS + timeout_s))
while :; do
	read -r hit executed <<<"$(spans)"
	hit=$((hit - base_hit)) executed=$((executed - base_executed))
	((hit + executed >= expect)) && break
	if ((SECONDS >= deadline)); then
		((hit + executed > 0)) || { echo "run.sh: no withExec cache span reached the service within ${timeout_s}s (does the CLI inherit an unsampled TRACEPARENT?)" >&2; exit 1; }
		echo "run.sh: $((hit + executed)) of the $expect withExec cache spans reached the service within ${timeout_s}s" >&2
		break
	fi
	sleep 3
done
echo "$hit $executed" >"$dir/$label.spans"
echo "$label: Container.withExec spans: $hit hit, $executed executed"
