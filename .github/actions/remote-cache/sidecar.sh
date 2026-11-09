#!/usr/bin/env bash
# The remote cache sidecar: the cache service (cmd/cacheservice) and an
# engine with the remote cache client, side by side on one Docker host, and
# the dagger CLI set up to use them. See README.md in this directory.
#
#   sidecar.sh start   start the service, then the engine; set up the CLI
#   sidecar.sh stop    stop the service (it drains), then, once its drain
#                      is done, the engine, then wait for the service to
#                      exit; keep their logs; remove the containers, the
#                      engine's state and the network
#
# The actions start/ and stop/ run it (start through a small JavaScript
# shim, start/run, which also runs the stop at the end of the job), and so
# does hack/remote-cache/demo/run.sh, locally. Its settings are environment
# variables:
#
#   SIDECAR_ROLE            writer | read-only (required)
#   SIDECAR_VERSION         the engine version, such as v1.0.0-beta.25: an
#                           accept line of the service's manifest
#                           (cmd/cacheservice/compat/engines.txt, at this
#                           script's own commit; default: its first). It
#                           selects both images: the stock engine of that
#                           version, pinned by the manifest's digest, and the service
#                           image that accepts it,
#                           ghcr.io/zerobull-consulting/dagger-remote-cache:<version>
#   SIDECAR_CLI             the dagger CLI to use: an absolute path to the one
#                           the job installed (such as dagger/dagger-for-github
#                           does). Its version must be the engine's, or the
#                           start fails before it creates or pulls anything.
#                           Without it (a local run), the CLI is copied out of
#                           the engine image, which always matches
#   SIDECAR_SERVICE_IMAGE   overrides the service image (such as a build of
#                           your own, or one pinned by digest); the version
#                           still selects the engine, and the start fails if
#                           the service refuses it
#   SIDECAR_ENGINE_IMAGE    deprecated: the engine image, in place of
#                           SIDECAR_VERSION, which it can't be given with;
#                           it needs SIDECAR_SERVICE_IMAGE, and warns
#   CACHESERVICE_S3_*       the store: ENDPOINT, REGION, BUCKET, ACCESS_KEY_ID
#                           and SECRET_ACCESS_KEY (required), PREFIX and
#                           PATH_STYLE (optional); passed to the service by
#                           name, never on a command line
#   SIDECAR_NAMESPACE       the service's -scope (default: default)
#   SIDECAR_DRAIN_TIMEOUT   the service's -drain-timeout (default: 10m)
#   SIDECAR_NAME            prefix of the containers, volume and network
#                           (default: remote-cache)
#   SIDECAR_DEBUG_PORT      publish the debug API on 127.0.0.1:<port>
#                           (default: not published), behind a token
#                           generated for this start, in
#                           <dir>/secrets/debug-token (the debug-token-file
#                           output)
#   SIDECAR_VERBOSE         true: the service's -v, which logs every cache span
#   SIDECAR_ENGINE_CONFIG   the engine's engine.json (default: engine.json in
#                           this directory: GC off, docker.io through
#                           mirror.gcr.io)
#   SIDECAR_DIR             where the state, the CLI and the logs go (default:
#                           $RUNNER_TEMP/remote-cache-sidecar, or a temporary
#                           directory)
#   SIDECAR_START_TIMEOUT   seconds to wait for the service to listen, and
#                           then for it to welcome or refuse the engine
#                           (default: 120)
#   SIDECAR_WELCOME_GRACE   with a service image given explicitly: seconds
#                           after the engine connected with neither a
#                           welcome nor a refusal logged, after which the
#                           service is taken for one from before the welcome
#                           line, with a warning (default: 10)
#   SIDECAR_ENGINE_CA_FILE  local use only: a CA bundle to mount as the
#                           engine's, for a host behind a TLS-intercepting
#                           proxy. The actions never set it.
#
# On GitHub Actions (GITHUB_ENV set), start adds the CLI to GITHUB_PATH, sets
# the CLI's variables in GITHUB_ENV, and writes the step's outputs.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

die() {
	echo "::error::remote-cache-sidecar: $*" >&2
	exit 1
}

warn() {
	if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
		echo "::warning::remote-cache-sidecar: $*"
	else
		echo "remote-cache-sidecar: warning: $*" >&2
	fi
}

# The service's images, one floating tag per engine version it accepts
# (zerobull-consulting/dagger#117, D4): :<engine version> is the newest
# verified build that accepts that engine.
service_repository=ghcr.io/zerobull-consulting/dagger-remote-cache

# The manifest at this script's own commit, and its reader: the one source
# of the engine versions and their digests (zerobull-consulting/dagger#117,
# D1). Never a caller's ENGINES_FILE.
engines_sh="$here/../../../hack/remote-cache/engines.sh"
engines_file="$here/../../../cmd/cacheservice/compat/engines.txt"

# --- validation: everything that becomes a name, a path or a flag ---

# Container, volume and network names; a file name too.
valid_name() { [[ "$1" =~ ^[a-z0-9][a-z0-9_.-]{0,39}$ ]]; }

# The service's own rule for a namespace (layout.ValidateNamespace).
valid_namespace() { [[ "$1" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ && "$1" != *..* ]]; }

# An image reference: registry, path, tag and digest characters only, and
# not an option.
valid_image() { [[ "$1" =~ ^[A-Za-z0-9][A-Za-z0-9._/:@+-]{0,254}$ ]]; }

# select_images sets version, engine_image and service_image from the
# settings (SIDECAR_VERSION, SIDECAR_SERVICE_IMAGE, SIDECAR_ENGINE_IMAGE):
#
#   version alone (or nothing: the manifest's first accept line): the
#     stock engine of that version by the manifest's digest, and
#     dagger-remote-cache:<version>;
#   service-image too: that service image, the same engine;
#   engine-image (deprecated), with service-image and without version:
#     both as given, with a warning.
#
# A version that isn't an accept line of this commit's manifest fails, as
# does engine-image with version, or without service-image; engines.sh
# checks the version, and never repeats one that isn't of a release's
# shape. Nothing is created or pulled before this. service_floating says
# whether the service image is the version's floating tag, not one given.
select_images() {
	version="${SIDECAR_VERSION:-}" service_image="${SIDECAR_SERVICE_IMAGE:-}" engine_image="${SIDECAR_ENGINE_IMAGE:-}"
	service_floating=false
	if [[ -n "$engine_image" ]]; then
		[[ -z "$version" ]] ||
			die "engine-image and version are both set: give version alone, which selects the engine by the manifest's digest (engine-image is deprecated)"
		[[ -n "$service_image" ]] ||
			die "engine-image (deprecated) needs service-image: give version instead, which selects both images"
		warn "engine-image is deprecated and will be removed: give version (such as $(ENGINES_FILE="$engines_file" "$engines_sh" pin | sed -n 's/^STOCK_ENGINE_TAG=//p')), which selects the engine by the manifest's digest and the service image that accepts it"
		return 0
	fi
	local pin
	pin="$(ENGINES_FILE="$engines_file" "$engines_sh" pin ${version:+"$version"} 2>&1)" ||
		die "version: ${pin#engines.sh: }"
	engine_image="$(sed -n 's/^STOCK_ENGINE=//p' <<<"$pin")"
	version="$(sed -n 's/^STOCK_ENGINE_TAG=//p' <<<"$pin")"
	[[ -n "$version" && -n "$engine_image" ]] || die "reading the manifest $engines_file failed"
	if [[ -z "$service_image" ]]; then
		service_image="$service_repository:$version"
		service_floating=true
	fi
}

# A bucket prefix: key segments of letters, digits, '.', '_' and '-',
# separated by '/', none of them '.' or '..'; or empty (the whole bucket).
valid_prefix() {
	local p="$1" seg
	[[ -z "$p" ]] && return 0
	[[ ${#p} -le 256 && "$p" =~ ^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$ ]] || return 1
	IFS=/ read -r -a segs <<<"$p"
	for seg in "${segs[@]}"; do
		[[ "$seg" != . && "$seg" != .. ]] || return 1
	done
}

# A Go duration of hours, minutes and seconds, in that order, such as 10m,
# 90s or 1h30m; or 0.
duration_seconds() {
	local d="$1" h=0 m=0 s=0
	[[ "$d" == 0 ]] && { echo 0; return; }
	[[ "$d" =~ ^(([0-9]{1,4})h)?(([0-9]{1,4})m)?(([0-9]{1,6})s)?$ && -n "$d" ]] || return 1
	h="${BASH_REMATCH[2]:-0}" m="${BASH_REMATCH[4]:-0}" s="${BASH_REMATCH[6]:-0}"
	echo $((10#$h * 3600 + 10#$m * 60 + 10#$s))
}

# The service's -drain-linger, which start passes: how long, after its
# drain, it keeps answering the engine's events at most, so that the engine's
# stop delivers its last one (engine.stop) and ends in seconds. It ends
# sooner, once that event arrived. 15s is the service's default too.
linger_seconds=15

# The version a dagger CLI says it is, such as v1.0.0-beta.25, from the line
# of `dagger version` that reads "version:     v1.0.0-beta.25" (the CLI's
# output: version, commit, dirty, platform and runner-host lines), or, in the
# older form, "dagger v1.0.0-beta.25 (image://...) linux/amd64"; nothing, and
# a failure, for anything else (a dev build included). The CLI runs as the
# wrapper runs it: none of the caller's trace or DAGGER_* settings, no engine
# needed, and bounded.
cli_version() {
	local out line semver='v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?'
	out="$(cli_run "$1" version 2>/dev/null)" || return 1
	while IFS= read -r line; do
		if [[ "$line" =~ ^version:[[:space:]]+($semver)[[:space:]]*$ || "$line" =~ ^dagger\ ($semver)(\ |$) ]]; then
			echo "${BASH_REMATCH[1]}"
			return 0
		fi
	done <<<"$out"
	return 1
}

# cli_run CLI ARGS...: the CLI as the wrapper runs it, and bounded.
cli_run() {
	local v
	for v in $(compgen -e); do
		case "$v" in TRACEPARENT | TRACESTATE | OTEL_* | DAGGER_* | _EXPERIMENTAL_DAGGER_*) unset "$v" ;; esac
	done
	export DAGGER_NO_NAG=1
	timeout 30 "$@" </dev/null
}

# check_cli CLI VERSION: fails unless CLI is an executable that says it is
# VERSION, the engine's. The engine is pinned by digest and the CLI only by
# its release tag, so this is what keeps the two matched.
check_cli() {
	local cli="$1" want="$2" got
	[[ "$cli" == /* ]] || die "SIDECAR_CLI '$cli' isn't an absolute path"
	[[ -f "$cli" && -x "$cli" ]] || die "SIDECAR_CLI $cli isn't an executable file"
	[[ -n "$want" ]] ||
		die "a CLI is given, but no version selects the engine: give version, which the CLI must match (engine-image, deprecated, can't be checked)"
	got="$(cli_version "$cli")" ||
		die "the CLI $cli didn't say its version (dagger version printed: '$(cli_run "$cli" version 2>&1 | head -c 300 | tr '\n' ' ' || true)'): a release of the CLI is needed, to match the engine $want"
	[[ "$got" == "$want" ]] ||
		die "the CLI ($cli) is $got but the engine is $want: they must be the same version. Install CLI $want (the version input of the action does)"
}

# --- state, kept between start and stop ---

sidecar_dir() {
	if [[ -n "${SIDECAR_DIR:-}" ]]; then
		echo "$SIDECAR_DIR"
	elif [[ -n "${RUNNER_TEMP:-}" ]]; then
		echo "$RUNNER_TEMP/remote-cache-sidecar"
	else
		echo "${TMPDIR:-/tmp}/remote-cache-sidecar"
	fi
}

# The state file holds only values start built and validated: names, a
# number, paths under the sidecar's directory. It is read line by line, never
# sourced.
read_state() {
	local file="$1" key value
	[[ -f "$file" ]] || return 1
	while IFS='=' read -r key value; do
		case "$key" in
		service) st_service="$value" ;;
		engine) st_engine="$value" ;;
		volume) st_volume="$value" ;;
		network) st_network="$value" ;;
		stop_seconds) st_stop_seconds="$value" ;;
		linger_seconds) st_linger_seconds="$value" ;;
		logs) st_logs="$value" ;;
		role) st_role="$value" ;;
		# What this sidecar's start created, one line each as it did: the
		# stop touches nothing else.
		created) case "$value" in service | engine | volume | network | debug-token) st_created+=" $value " ;; esac ;;
		esac
	done <"$file"
	valid_name "$st_service" && valid_name "$st_engine" && valid_name "$st_volume" &&
		valid_name "$st_network" && [[ "$st_stop_seconds" =~ ^[0-9]+$ && "$st_linger_seconds" =~ ^[0-9]+$ ]] ||
		die "the state file $file isn't one start wrote"
}

# A container's exit code, or "unknown" when docker fails or doesn't answer
# within the bound (a second argument, in seconds; default 30): never a hang,
# and never a different failure.
exit_code() {
	local c
	c="$(timeout "${2:-30}" docker inspect -f '{{.State.ExitCode}}' "$1" 2>/dev/null || true)"
	[[ "$c" =~ ^[0-9]+$ ]] && echo "$c" || echo unknown
}

running() { [[ "$(timeout 30 docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || true)" == true ]]; }

# Whether docker says a container has stopped. Unlike ! running, a docker
# that fails or doesn't answer says nothing: a wait on it goes on.
# A second argument bounds the inspect, in seconds (default 30).
exited() { [[ "$(timeout "${2:-30}" docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || true)" == false ]]; }

# A container's state, as docker gives it: running, stopped, or unknown when
# docker fails or doesn't answer three times in a row. A second argument
# bounds the three tries and the 1s pauses between them, in seconds
# (default 10, at least 5; zerobull-consulting/dagger#125). The first try
# gets all of it but 2s for each later try and its pause (6s of 10), so a
# daemon that is slow but answers is still heard; the later ones, for a
# docker that failed at once, get at least 1s each. A try cut off by that
# bound prints nothing, so it reads as unknown, never as stopped.
container_state() {
	local budget="${2:-10}" tries v b end
	((budget < 5)) && budget=5
	end=$((SECONDS + budget))
	for tries in 1 2 3; do
		b=$((end - SECONDS - 2 * (3 - tries)))
		((b < 1)) && b=1
		v="$(timeout "$b" docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || true)"
		case "$v" in
		true) echo running && return ;;
		false) echo stopped && return ;;
		esac
		((tries < 3 && SECONDS < end)) && sleep 1
	done
	echo unknown
}

# The bound for one docker call in a wait that ends at the time $1 (in
# SECONDS): at most 10s, and no more than what is left of the wait (at least 1).
step_bound() {
	local left=$(($1 - SECONDS))
	((left > 10)) && left=10
	((left < 1)) && left=1
	echo "$left"
}

# Whether this sidecar's start created a resource (service, engine, volume,
# network), from the state it read.
created() { [[ "$st_created" == *" $1 "* ]]; }

# Waits until the service's log since a Unix time says its drain is done, or
# docker says the service has stopped, for at most a number of seconds;
# returns 1 when that bound passed first. A docker that fails meanwhile ends
# nothing: the engine must not be stopped before the drain is done.
wait_drained() {
	local container="$1" since="$2" end=$((SECONDS + $3)) log
	while ((SECONDS < end)); do
		exited "$container" "$(step_bound "$end")" && return 0
		log="$(timeout "$(step_bound "$end")" docker logs --since "$since" "$container" 2>&1 || true)"
		grep -q 'msg="drain: done"' <<<"$log" && return 0
		sleep 0.5
	done
	return 1
}

# Waits until docker says a container has stopped, for at most a number of
# seconds (at least 1); returns 1 when that bound passed first, a docker that
# failed throughout included.
wait_exited() {
	local container="$1" end=$((SECONDS + ($2 > 1 ? $2 : 1)))
	while ((SECONDS < end)); do
		exited "$container" "$(step_bound "$end")" && return 0
		sleep 0.5
	done
	exited "$container" 1
}

# Whether a container's log has a line matching a pattern. The log is read
# whole first: grep -q stopping early would fail the pipeline (pipefail).
log_has() {
	local log
	log="$(timeout 30 docker logs "$1" 2>&1 || true)"
	grep -q -- "$2" <<<"$log"
}

# Presigned URLs carry the key ID and a signature that works until they
# expire: drop both from what is kept or shown.
redact() { sed -E 's/(X-Amz-(Credential|Signature|Security-Token)=)[^&" ]*/\1REDACTED/g'; }

show_log() {
	local container="$1" lines="${2:-100}"
	echo "--- the last $lines lines of $container's log ---" >&2
	timeout 30 docker logs --tail "$lines" "$container" 2>&1 | redact >&2 || true
	echo "---" >&2
}

# --- start ---

# pull_one IMAGE FORCE: pulls an image unless it is here already; FORCE=true
# pulls it anyway. Bounded: a daemon that doesn't answer reads as "not here",
# and the pull's own bound then fails it.
pull_one() {
	local image="$1" force="$2"
	if [[ "$force" == true ]] || ! timeout "${SIDECAR_INSPECT_TIMEOUT:-30}" docker image inspect "$image" >/dev/null 2>&1; then
		echo "remote-cache-sidecar: pulling $image"
		timeout 900 docker pull -q "$image" >/dev/null
	fi
}

# pull_images SERVICE_IMAGE SERVICE_FORCE ENGINE_IMAGE: both at once, then
# waits for both and fails naming each that failed.
pull_images() {
	local service_image="$1" service_force="$2" engine_image="$3" failed=""
	local service_pid engine_pid
	pull_one "$service_image" "$service_force" &
	service_pid=$!
	pull_one "$engine_image" false &
	engine_pid=$!
	wait "$service_pid" || failed+=" pulling $service_image failed or took over 15m;"
	wait "$engine_pid" || failed+=" pulling $engine_image failed or took over 15m;"
	[[ -z "$failed" ]] || die "${failed# }"
}

start() {
	local role="${SIDECAR_ROLE:-}"
	local name="${SIDECAR_NAME:-remote-cache}"
	local namespace="${SIDECAR_NAMESPACE:-default}"
	local drain="${SIDECAR_DRAIN_TIMEOUT:-10m}"
	local debug_port="${SIDECAR_DEBUG_PORT:-}"
	local wait="${SIDECAR_START_TIMEOUT:-120}" grace="${SIDECAR_WELCOME_GRACE:-10}"
	local version service_image engine_image service_floating

	case "$role" in
	writer | read-only) ;;
	*) die "role must be writer or read-only, not '$role'" ;;
	esac
	valid_name "$name" || die "name '$name' isn't valid: 1-40 of a-z, 0-9, '.', '_' and '-', starting with a letter or digit"
	valid_namespace "$namespace" || die "namespace '$namespace' isn't valid: 1-128 letters, digits, '.', '_' or '-', starting with a letter or digit, without '..'"
	select_images
	valid_image "$service_image" || die "service-image '$service_image' isn't an image reference"
	valid_image "$engine_image" || die "engine-image '$engine_image' isn't an image reference"
	local drain_seconds
	drain_seconds="$(duration_seconds "$drain")" || die "drain-timeout '$drain' isn't a duration such as 10m, 90s or 1h30m"
	[[ -z "$debug_port" || ("$debug_port" =~ ^[0-9]{1,5}$ && "$debug_port" -ge 1 && "$debug_port" -le 65535) ]] ||
		die "debug-port '$debug_port' isn't a port number"
	[[ "$wait" =~ ^[0-9]{1,4}$ ]] || die "SIDECAR_START_TIMEOUT '$wait' isn't a number of seconds"
	[[ "$grace" =~ ^[0-9]{1,4}$ ]] || die "SIDECAR_WELCOME_GRACE '$grace' isn't a number of seconds"
	valid_prefix "${CACHESERVICE_S3_PREFIX:-}" ||
		die "s3-prefix '${CACHESERVICE_S3_PREFIX:-}' isn't valid: segments of letters, digits, '.', '_' or '-', separated by '/', none '.' or '..', at most 256 characters"
	case "${CACHESERVICE_S3_PATH_STYLE:-}" in
	"" | true | false) ;;
	*) die "s3-path-style must be true, false or empty, not '$CACHESERVICE_S3_PATH_STYLE'" ;;
	esac
	case "${SIDECAR_VERBOSE:-false}" in
	true | false | "") ;;
	*) die "verbose must be true or false, not '$SIDECAR_VERBOSE'" ;;
	esac
	# Checked here, before the network or any container exists.
	local config="${SIDECAR_ENGINE_CONFIG:-$here/engine.json}"
	[[ -f "$config" && "$config" != *:* ]] || die "engine-config '$config' isn't a file (or its path has a ':')"
	local var
	for var in ENDPOINT REGION BUCKET ACCESS_KEY_ID SECRET_ACCESS_KEY; do
		[[ -n "$(printenv "CACHESERVICE_S3_$var" || true)" ]] || die "the S3 setting CACHESERVICE_S3_$var is empty: pass it from the caller's secrets or variables"
	done
	if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
		# Masked in this job's log from here on, whatever their source.
		echo "::add-mask::$CACHESERVICE_S3_ACCESS_KEY_ID"
		echo "::add-mask::$CACHESERVICE_S3_SECRET_ACCESS_KEY"
	fi

	local cli="${SIDECAR_CLI:-}"
	# The wrapper runs the CLI: it must never be the wrapper itself, and it
	# is refused before the CLI is run.
	[[ -z "$cli" || "$cli" != "$(sidecar_dir)/$name/bin/dagger" ]] || die "SIDECAR_CLI is this sidecar's own wrapper ($cli): give the CLI the job installed"
	[[ -z "$cli" ]] || check_cli "$cli" "$version"

	local dir
	dir="$(sidecar_dir)/$name"
	local state="$dir/state" logs="$dir/logs" bin="$dir/bin"
	if [[ -f "$state" ]]; then
		die "a sidecar named $name is already set up ($state): stop it first, or give this one another name"
	fi
	mkdir -p "$logs" "$bin"
	local service="$name-service" engine="$name-engine" volume="$name-engine-state" network="$name-net"
	# The stop's own bound: the service's whole stop takes at most the drain
	# plus its linger plus 5s (README, "Stopping and draining"); the stop
	# allows 5s more.
	local stop_seconds=$((drain_seconds + linger_seconds + 10))
	# Every name must be free before anything is written or created: the
	# stop removes what the start created, and must never take a container,
	# volume or network that was already there (such as a crashed job's on
	# a self-hosted runner) for this sidecar's.
	local taken=""
	timeout 30 docker container inspect "$service" >/dev/null 2>&1 && taken+=" container $service"
	timeout 30 docker container inspect "$engine" >/dev/null 2>&1 && taken+=" container $engine"
	timeout 30 docker volume inspect "$volume" >/dev/null 2>&1 && taken+=" volume $volume"
	timeout 30 docker network inspect "$network" >/dev/null 2>&1 && taken+=" network $network"
	[[ -z "$taken" ]] ||
		die "already there, and not this sidecar's:$taken. Remove them (docker rm -f, docker volume rm, docker network rm), or give this sidecar another name"
	printf '%s\n' "service=$service" "engine=$engine" "volume=$volume" "network=$network" \
		"stop_seconds=$stop_seconds" "linger_seconds=$linger_seconds" "logs=$logs" "role=$role" >"$state"

	# Pulled only when not here already: a job may have built them, and the
	# engine is pinned by digest. The version's floating service tag is
	# always pulled: it moves to newer builds, and a copy left from an
	# earlier job (a self-hosted runner, a dev machine) would be stale. The
	# two are pulled at the same time, each bounded; the start fails if
	# either fails.
	local pulled_at=$SECONDS
	pull_images "$service_image" "$service_floating" "$engine_image"
	echo "remote-cache-sidecar: images ready in $((SECONDS - pulled_at))s"

	timeout 60 docker network create --label remote-cache-sidecar="$name" "$network" >/dev/null ||
		die "creating the network $network failed"
	echo "created=network" >>"$state"
	# The service's port is published on the network's gateway: the engine
	# reaches it there, and so does the CLI on this host, at one URL. The
	# engine sends the session's spans to the CLI's DAGGER_CLOUD_URL, so both
	# must be able to use it. Nothing beyond this host reaches the gateway.
	local gateway
	gateway="$(timeout 30 docker network inspect -f '{{(index .IPAM.Config 0).Gateway}}' "$network")"
	[[ "$gateway" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]] || die "the network $network has no IPv4 gateway ('$gateway')"

	local flags=(-scope "$namespace" -drain-timeout "$drain" -drain-linger "${linger_seconds}s")
	if [[ "$role" == writer ]]; then
		# Scheduled GC belongs to its own job (zerobull-consulting/dagger#61).
		flags+=(-gc-interval=0)
	else
		# A pull-request job: never writes to or deletes from the bucket.
		flags+=(-export=false)
	fi
	local publish=(-p "$gateway::8090") token_mount=() token_file=""
	if [[ -n "$debug_port" ]]; then
		# The debug API, on this host's loopback only, and behind a token
		# generated for this start: :8091 isn't loopback in the container,
		# and the service refuses it without one. The token goes in a file
		# mounted read-only, never on a command line or in the container's
		# environment. The file is readable by the service's UID (65532);
		# its directory only by this user.
		# Absolute: docker takes a relative -v source for a volume name.
		local secrets token
		secrets="$(cd "$dir" && pwd)/secrets"
		token_file="$secrets/debug-token"
		[[ "$token_file" != *:* ]] || die "the sidecar's directory $dir has a ':'"
		# Never change, overwrite or later delete what this start didn't
		# create: the directory is new, so the token file is too.
		[[ ! -e "$secrets" && ! -L "$secrets" ]] ||
			die "$secrets is already there, and not this start's: remove it, or give this sidecar another name"
		mkdir -m 700 "$secrets" || die "creating $secrets failed"
		token="$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')"
		[[ "$token" =~ ^[0-9a-f]{64}$ ]] || die "generating the debug token failed"
		[[ -n "${GITHUB_ACTIONS:-}" ]] && echo "::add-mask::$token"
		# Recorded first: the stop removes the token file only when this
		# start wrote it.
		echo "created=debug-token" >>"$state"
		(umask 022 && printf '%s\n' "$token" >"$token_file") || die "writing $token_file failed"
		chmod 644 "$token_file"
		token_mount=(-v "$token_file:/run/secrets/debug-token:ro")
		flags+=(-debug-listen :8091 -debug-token-file /run/secrets/debug-token)
		publish+=(-p "127.0.0.1:$debug_port:8091")
	fi
	[[ "${SIDECAR_VERBOSE:-}" == true ]] && flags+=(-v)
	local s3env=() var
	for var in ENDPOINT REGION BUCKET PREFIX ACCESS_KEY_ID SECRET_ACCESS_KEY PATH_STYLE; do
		# By name: docker reads the value from this environment.
		[[ -n "$(printenv "CACHESERVICE_S3_$var" || true)" ]] && s3env+=(-e "CACHESERVICE_S3_$var")
	done

	echo "remote-cache-sidecar: starting the $role service ($service_image)"
	timeout 120 docker run -d --name "$service" --network "$network" \
		--label remote-cache-sidecar="$name" "${publish[@]}" "${token_mount[@]}" "${s3env[@]}" \
		"$service_image" "${flags[@]}" >/dev/null || die "starting the service failed"
	# From here on the stop expects a service, and fails without one.
	echo "created=service" >>"$state"
	local hostport
	hostport="$(timeout 30 docker port "$service" 8090/tcp | head -n1)"
	[[ "$hostport" =~ ^[0-9.]+:[0-9]+$ ]] || { show_log "$service"; die "the service's port isn't published ('$hostport')"; }
	local url="http://$hostport"

	# Up once it listens: it checked the bucket's ownership before that, and
	# exits 1 or 2 on a refusal or a bad flag.
	# Deadlines by the clock, not by tries: each try reads the log.
	local deadline=$((SECONDS + wait)) up=false
	while ((SECONDS < deadline)); do
		running "$service" || { show_log "$service" 200; die "the service exited at startup (code $(exit_code "$service"))"; }
		log_has "$service" 'msg=listening addr=:8090' && { up=true; break; }
		sleep 1
	done
	[[ "$up" == true ]] || { show_log "$service" 200; die "the service didn't listen within ${wait}s"; }
	echo "remote-cache-sidecar: the service listens at $url"

	# GC off, as cmd/cacheservice/e2e runs its engines: the engine's state
	# ends with the job, and its GC can prune entries right after their
	# session, before the service has exported them (a host whose disk looks
	# full prunes everything). docker.io through mirror.gcr.io, as in the
	# release image's own engine.json.
	config="$(cd "$(dirname "$config")" && pwd)/$(basename "$config")"
	local mounts=(-v "$config:/etc/dagger/engine.json:ro")
	if [[ -n "${SIDECAR_ENGINE_CA_FILE:-}" ]]; then
		[[ -f "$SIDECAR_ENGINE_CA_FILE" ]] || die "SIDECAR_ENGINE_CA_FILE $SIDECAR_ENGINE_CA_FILE isn't a file"
		[[ "$SIDECAR_ENGINE_CA_FILE" != *:* ]] || die "SIDECAR_ENGINE_CA_FILE's path has a ':'"
		mounts+=(-v "$SIDECAR_ENGINE_CA_FILE:/etc/ssl/certs/ca-certificates.crt:ro")
	fi
	echo "remote-cache-sidecar: starting the engine ($engine_image)"
	# The welcome or the refusal comes after this: the wait below reads the
	# service's log from here on only.
	local since
	since="$(date +%s)"
	timeout 60 docker volume create --label remote-cache-sidecar="$name" "$volume" >/dev/null ||
		die "creating the volume $volume failed"
	echo "created=volume" >>"$state"
	# The engine's settings, as cmd/cacheservice/e2e/engine.go starts it: the
	# remote cache URL, and DAGGER_CLOUD_URL for its cache events. The token
	# is a placeholder: the service runs without a tokens file, and takes any
	# token but "oidc".
	timeout 120 docker run -d --privileged --name "$engine" --network "$network" \
		--label remote-cache-sidecar="$name" -v "$volume:/var/lib/dagger" "${mounts[@]}" \
		-e _EXPERIMENTAL_DAGGER_REMOTE_CACHE_URL="$url" \
		-e DAGGER_CLOUD_URL="$url" \
		-e DAGGER_CLOUD_TOKEN=sidecar-engine \
		"$engine_image" --debug >/dev/null || die "starting the engine failed"
	echo "created=engine" >>"$state"
	# Ready once the service has welcomed the engine: "engine connected" is
	# logged at the WebSocket upgrade, before the hello says whether the
	# engine's version is accepted (zerobull-consulting/dagger#146). A
	# refusal fails the start: builds would run on without the cache, and a
	# service that refuses this engine is a misconfiguration (a service-image
	# that doesn't accept the version), not a fleet mid-upgrade, which is
	# what the service's own refusal leaves builds running for. The two
	# lines are the service's LogEngineWelcomed and LogEngineRefused
	# (TestSidecarWaitsForTheseLines).
	#
	# A service image given explicitly (service-image, or the deprecated
	# engine-image's pair) may be from before the welcome line: one that
	# logs neither line within the grace after the engine connected is taken
	# for that, with a warning. A refusal comes within milliseconds of the
	# connection. The version's own floating tag always logs the welcome.
	local log welcomed=false connected_at=""
	deadline=$((SECONDS + wait))
	while ((SECONDS < deadline)); do
		running "$service" || { show_log "$engine" 100; show_log "$service" 200; die "the service exited while the engine started"; }
		running "$engine" || { show_log "$engine" 200; die "the engine exited at startup"; }
		log="$(timeout "$(step_bound "$deadline")" docker logs --since "$since" "$service" 2>&1 || true)"
		if grep -q 'msg="engine refused: its version is not accepted"' <<<"$log"; then
			show_log "$engine" 50
			show_log "$service" 100
			die "the service ($service_image) refused the engine ($engine_image): $(grep -m1 'msg="engine refused: its version is not accepted"' <<<"$log" | redact | sed -E 's/^time=[^ ]* //'). Give a version the service accepts, or a service-image that accepts this version"
		fi
		grep -q 'msg="engine welcomed"' <<<"$log" && { welcomed=true; break; }
		if [[ "$service_floating" != true ]] && grep -q 'msg="engine connected"' <<<"$log"; then
			[[ -n "$connected_at" ]] || connected_at=$SECONDS
			if ((SECONDS - connected_at >= grace)); then
				warn "the service ($service_image) logged neither a welcome nor a refusal ${grace}s after the engine connected: taking it for an image from before the welcome line (zerobull-consulting/dagger#146), which can't say whether it refused the engine. Use the version input's service image, or a newer one"
				welcomed=old
				break
			fi
		fi
		sleep 1
	done
	if [[ "$welcomed" == false ]]; then
		show_log "$engine" 100
		show_log "$service" 100
		if grep -q 'msg="engine connected"' <<<"$log"; then
			die "the engine connected, but the service neither welcomed nor refused it within ${wait}s (a service image older than the version input logs no welcome)"
		fi
		die "the engine didn't connect to the service within ${wait}s"
	fi
	[[ "$welcomed" == true ]] && echo "remote-cache-sidecar: the service welcomed the engine (${version:-$engine_image})"

	# The CLI the job installed (its version checked against the engine's
	# above), or, in a local run, the engine image's own, which matches.
	if [[ -n "$cli" ]]; then
		echo "remote-cache-sidecar: using the CLI $cli ($version)"
	else
		cli="$dir/dagger"
		timeout 120 docker cp "$engine:/usr/local/bin/dagger" "$cli" >/dev/null ||
			die "copying the CLI out of the engine image failed"
		chmod +x "$cli"
	fi
	# A wrapper, so whatever a later step's environment holds, the CLI uses
	# this engine and sends its telemetry to this service, and never
	# continues an inherited trace: a CLI that continues an unsampled
	# TRACEPARENT records no spans, and the service learns of no entry and
	# exports nothing (cmd/cacheservice/README.md, "Real-engine run"). The
	# CLI also stops early, with the service's log, if the service is gone.
	local q_service q_dagger
	q_service="$(printf %q "$service")" q_dagger="$(printf %q "$cli")"
	cat >"$bin/dagger" <<EOF
#!/usr/bin/env bash
# Written by the remote cache sidecar (.github/actions/remote-cache).
if [[ "\$(timeout 30 docker inspect -f '{{.State.Running}}' $q_service 2>/dev/null)" != true ]]; then
	echo "remote-cache-sidecar: the cache service $q_service isn't running" >&2
	timeout 30 docker logs --tail 100 $q_service 2>&1 | sed -E 's/(X-Amz-(Credential|Signature|Security-Token)=)[^&" ]*/\1REDACTED/g' >&2
	exit 1
fi
# As cmd/cacheservice/e2e runs its CLI: no inherited trace, and none of the
# caller's OTEL_*, DAGGER_* or _EXPERIMENTAL_DAGGER_* settings (DAGGER_ENGINE
# would take another engine), then the sidecar's own.
for v in \$(compgen -e); do
	case "\$v" in TRACEPARENT | TRACESTATE | OTEL_* | DAGGER_* | _EXPERIMENTAL_DAGGER_*) unset "\$v" ;; esac
done
export _EXPERIMENTAL_DAGGER_RUNNER_HOST=docker-container://$engine
export DAGGER_CLOUD_URL=$url
export DAGGER_CLOUD_TOKEN=sidecar-cli
export DAGGER_NO_NAG=1
exec $q_dagger "\$@"
EOF
	chmod +x "$bin/dagger"
	"$bin/dagger" version || die "the CLI doesn't run"

	if [[ -n "${GITHUB_ENV:-}" ]]; then
		echo "$bin" >>"$GITHUB_PATH"
		{
			echo "_EXPERIMENTAL_DAGGER_RUNNER_HOST=docker-container://$engine"
			echo "DAGGER_CLOUD_URL=$url"
			echo "DAGGER_CLOUD_TOKEN=sidecar-cli"
		} >>"$GITHUB_ENV"
	fi
	if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
		{
			echo "dagger=$bin/dagger"
			echo "version=$version"
			echo "service-image=$service_image"
			echo "engine-image=$engine_image"
			echo "runner-host=docker-container://$engine"
			echo "service-url=$url"
			echo "service-container=$service"
			echo "engine-container=$engine"
			echo "logs=$logs"
			echo "state=$state"
			if [[ -n "$token_file" ]]; then
				echo "debug-token-file=$token_file"
			fi
		} >>"$GITHUB_OUTPUT"
	fi
	echo "remote-cache-sidecar: ready ($role, namespace $namespace). CLI: $bin/dagger"
}

# --- stop ---

stop() {
	local name="${SIDECAR_NAME:-remote-cache}"
	valid_name "$name" || die "name '$name' isn't valid"
	local state
	state="$(sidecar_dir)/$name/state"
	# A state without linger_seconds is a start's from before the linger,
	# whose stop_seconds has no room for one.
	local st_service="" st_engine="" st_volume="" st_network="" st_stop_seconds="" st_linger_seconds=0 st_logs="" st_role="" st_created=" "
	if ! read_state "$state"; then
		echo "remote-cache-sidecar: nothing to stop: no state at $state (did the start step run?)"
		return 0
	fi
	local failed=""

	# The service first: the drain's exports run over the engine's
	# connection, and the engine uploads the layers itself. Then, once the
	# drain is done, the engine, while the service still answers its events:
	# the engine's stop sends its last one (engine.stop) to the service, and
	# ends in seconds rather than retry against a closed port for 30s. Then
	# the service, which exits once that event arrived (or at its linger's
	# bound). Every wait is bounded, and the cleanup below runs whatever
	# they found.
	local was_running=false code="" still_running=false overran=false engine_code=""
	# The stop's bound counts from the SIGTERM, set below: the probe before
	# it doesn't eat into the service's time.
	local deadline=$((SECONDS + st_stop_seconds))
	# A docker that fails here says nothing about the service: it is taken
	# for running, so the engine still waits for the drain. The probe is
	# bounded at about 10s in all, so a daemon that hangs here keeps the
	# step inside its documented bound (zerobull-consulting/dagger#125).
	local service_state=""
	created service && service_state="$(container_state "$st_service" 10)"
	[[ "$service_state" == unknown ]] && echo "remote-cache-sidecar: docker didn't say whether the service is running; stopping it as if it is" >&2
	if [[ "$service_state" == running || "$service_state" == unknown ]]; then
		was_running=true
		local since
		since="$(($(date +%s) - 1))"
		echo "remote-cache-sidecar: stopping the service; it drains for up to $((st_stop_seconds - st_linger_seconds - 10))s, then answers the engine's last event for up to ${st_linger_seconds}s; the stop waits ${st_stop_seconds}s for it"
		deadline=$((SECONDS + st_stop_seconds))
		timeout 30 docker kill -s TERM "$st_service" >/dev/null 2>&1 || true
		if ! wait_drained "$st_service" "$since" $((st_stop_seconds - st_linger_seconds - 5)); then
			echo "remote-cache-sidecar: the service didn't report its drain done in time; stopping the engine anyway" >&2
		fi
	fi
	if created engine && timeout 30 docker container inspect "$st_engine" >/dev/null 2>&1; then
		# The engine's own bound: a kill before it closed its cache would
		# leave its store unclean.
		timeout 90 docker stop --time 30 "$st_engine" >/dev/null 2>&1 || true
		if running "$st_engine"; then
			echo "remote-cache-sidecar: the engine was still running after docker stop" >&2
		else
			# 0 for a clean stop; 137 when docker stop killed it.
			engine_code="$(timeout 30 docker inspect -f '{{.State.ExitCode}}' "$st_engine" 2>/dev/null || true)"
			echo "remote-cache-sidecar: the engine stopped (exit ${engine_code:-unknown})"
		fi
		timeout 60 docker logs "$st_engine" 2>&1 | redact >"$st_logs/engine.log" || true
	fi
	if [[ "$was_running" == true ]] && ! wait_exited "$st_service" $((deadline - SECONDS)); then
		# Past the service's own bound: stop it. docker stop's SIGTERM
		# ends its linger at once, or its shutdown with exit 1.
		overran=true
		timeout 60 docker stop --time 5 "$st_service" >/dev/null 2>&1 || true
		# A docker stop that gave up (or was cut off) leaves the service
		# running, with the exit code of a container that hasn't exited:
		# never take that for a clean stop.
		if running "$st_service"; then
			still_running=true
		fi
	fi
	if created service && timeout 30 docker container inspect "$st_service" >/dev/null 2>&1; then
		code="$(exit_code "$st_service")"
		[[ "$code" != unknown ]] || code=""
		timeout 60 docker logs "$st_service" 2>&1 | redact >"$st_logs/service.log" || true
	fi
	# Only what this sidecar's start created.
	# A removal that fails is reported, with its name and kind, but doesn't
	# change the stop's exit status: an ephemeral runner discards what is
	# left, and a self-hosted one's next start says what is in its way.
	local leftovers=""
	created service && { timeout 60 docker rm -f "$st_service" >/dev/null 2>&1 || leftovers+=" container $st_service"; }
	created engine && { timeout 60 docker rm -f "$st_engine" >/dev/null 2>&1 || leftovers+=" container $st_engine"; }
	created volume && { timeout 60 docker volume rm -f "$st_volume" >/dev/null 2>&1 || leftovers+=" volume $st_volume"; }
	created network && { timeout 60 docker network rm "$st_network" >/dev/null 2>&1 || leftovers+=" network $st_network"; }
	[[ -z "$leftovers" ]] || echo "remote-cache-sidecar: couldn't remove, so left behind:$leftovers; remove them by hand if the runner is kept" >&2
	rm -f "$state"
	# The debug token, if this start generated it: it opened only this
	# service. Its directory goes only if nothing else is in it.
	if created debug-token; then
		rm -f -- "$(sidecar_dir)/$name/secrets/debug-token"
		rmdir -- "$(sidecar_dir)/$name/secrets" 2>/dev/null || true
	fi
	[[ -n "${GITHUB_OUTPUT:-}" ]] && echo "logs=$st_logs" >>"$GITHUB_OUTPUT"

	if [[ -f "$st_logs/service.log" ]]; then
		echo "remote-cache-sidecar: the drain, from the service's log:"
		grep -E 'msg="?(stopping|drain|linger|stopped|export stored)' "$st_logs/service.log" | tail -n 20 || true
	fi
	if [[ "$still_running" == true ]]; then
		failed="the service was still running after docker stop gave up: it was killed, and what it was exporting is lost"
	elif [[ -z "$code" ]] && ! created service; then
		# The start failed before it ran the service; its step said why.
		echo "remote-cache-sidecar: the start didn't get as far as the service; cleaned up what it left"
		return 0
	elif [[ -z "$code" ]]; then
		failed="the service's exit code is unknown (its container is gone, or docker didn't say)"
	elif [[ "$was_running" != true ]]; then
		failed="the service exited during the job, with code $code, before the stop"
	elif [[ "$overran" == true ]]; then
		failed="the service was still stopping after ${st_stop_seconds}s, past its own bound, and was stopped (exit $code): what it was exporting is lost"
	elif [[ "$code" == 137 ]]; then
		failed="the service was killed (137) before its stop ended, not by this stop (such as out of memory): what it was exporting is lost"
	elif [[ "$code" != 0 ]]; then
		failed="the service's stop wasn't clean (exit $code): the shutdown's bound cut work off, or a server failed; see its log"
	fi
	if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
		{
			echo "### remote-cache-sidecar: $st_role stopped"
			echo
			echo "- service exit code: ${code:-unknown}"
			echo "- engine exit code: ${engine_code:-unknown}"
			echo "- logs: \`$st_logs\`"
			[[ -n "$leftovers" ]] && echo "- **left behind (couldn't be removed):**$leftovers"
			[[ -n "$failed" ]] && echo "- **failed:** $failed"
		} >>"$GITHUB_STEP_SUMMARY"
	fi
	if [[ -n "$failed" ]]; then
		[[ -f "$st_logs/service.log" ]] && { echo "--- the last 100 lines of the service's log ---" >&2; tail -n 100 "$st_logs/service.log" >&2; }
		die "$failed"
	fi
	echo "remote-cache-sidecar: stopped cleanly (exit 0). Logs: $st_logs"
}

# Sourced (by sidecar_test.sh), it only defines the functions.
[[ "${BASH_SOURCE[0]}" == "$0" ]] || return 0

case "${1:-}" in
start) start ;;
stop) stop ;;
*)
	sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
	;;
esac
