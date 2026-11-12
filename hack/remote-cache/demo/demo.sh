#!/usr/bin/env bash
# The remote cache demo: two steps, run with the dagger CLI against an engine
# that has a remote cache sidecar (see README.md in this directory).
#
#   demo.sh            run both steps
#   demo.sh build      run the cacheable step only
#   demo.sh app        run the cache-volume step only (it runs the first
#                      step's pipeline too, as its base)
#
# DAGGER is the CLI to run (default: dagger, from PATH). Run the steps on a
# fresh engine after a writer stored them: the first comes back at once,
# with the writer's stamp, while the second reruns its work.
set -euo pipefail

# By its full path: the CLI runs from another directory (see run).
dagger="$(command -v "${DAGGER:-dagger}")" || { echo "demo.sh: no dagger CLI: ${DAGGER:-dagger}" >&2; exit 1; }
[[ "$dagger" == /* ]] || dagger="$PWD/$dagger"

# The cacheable step. It stands in for an expensive build: it writes 4 MiB
# of random bytes and a stamp with a random ID, then sleeps 20s. Random
# output makes a rerun visible: a step that ran again prints another stamp.
#
# Like a Gradle or ccache build, it also leaves state in a cache volume,
# /deps: a copy of what it built, which later steps reuse.
#
# Its result (the container's filesystem, and so the stamp) is a cache
# entry: a writer sidecar stores it, and a fresh engine with a sidecar gets
# it back without running it. What it left in the cache volume isn't
# stored.
read -r -d '' build <<'EOF' || true
container |
  from alpine:3.22 |
  with-mounted-cache /deps $(cache-volume remote-cache-demo-deps) |
  with-exec -- sh -c '
      head -c 4194304 /dev/urandom > /build.bin &&
      cp /build.bin /deps/build.bin &&
      echo "build $(cat /proc/sys/kernel/random/uuid) $(sha256sum /build.bin | cut -c1-16)" > /stamp &&
      sleep 20'
EOF

# The cache-volume step. It builds on the first step's container, with a
# change of its own on every run (CHANGE, a random value, like an edited
# source file), so it is never a cache hit. It reuses what the first step left in the cache volume
# when that is there ("warm"), and redoes that work when it isn't ("cold":
# here, a 10s sleep).
#
# On the engine that ran the first step, the cache volume holds its copy,
# and this step is warm. On a fresh engine, the first step is a cache hit,
# so it never ran there and the cache volume is empty: this step is cold,
# and reruns work the writer had already done. That is the limit the
# service's README describes: cache volumes stay local. To carry such
# state, make it an output of a cacheable step (a file or a directory in its
# container) instead of a cache volume.
read -r -d '' app <<'EOF' || true
with-env-variable CHANGE @CHANGE@ |
  with-exec -- sh -c '
      if cmp -s /build.bin /deps/build.bin; then
        echo "app: cache volume warm: reused the build step state";
      else
        echo "app: cache volume cold: redoing the build step work";
        sleep 10;
      fi' |
  stdout
EOF

run() {
	local what="$1" pipeline="$2" start=$SECONDS
	echo "== $what" >&2
	# The CLI's progress goes to stderr, or to DEMO_LOG_DIR/<step>.log.
	local err=/dev/stderr
	[[ -n "${DEMO_LOG_DIR:-}" ]] && err="$DEMO_LOG_DIR/${what%%:*}.log"
	# From an empty directory, outside any workspace: the CLI would
	# otherwise load the workspace it's in, and pin the demo's image in its
	# dagger.lock.
	local work
	work="$(mktemp -d)"
	(cd "$work" && "$dagger" --progress=plain --no-load-module -c "$pipeline") 2>"$err" ||
		{ rmdir "$work"; echo "== $what failed${DEMO_LOG_DIR:+ (see $err)}" >&2; return 1; }
	rmdir "$work"
	echo "== $what took $((SECONDS - start))s" >&2
}

case "${1:-all}" in
build) run "build: the cacheable step" "$build | file /stamp | contents" ;;
app)
	# The change: new on every run, so the step's inputs differ each time.
	change="$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
	run "app: the cache-volume step" "$build |
  ${app//@CHANGE@/$change}"
	;;
all)
	"$0" build
	"$0" app
	;;
*)
	sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
	;;
esac
