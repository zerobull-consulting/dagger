#!/usr/bin/env bash
# The remote cache sidecar: the cache service (cmd/cacheservice) and an
# engine with the remote cache client, side by side on one Docker host, and
# the dagger CLI set up to use them. See README.md in this directory.
#
#   sidecar.sh start   start the service, then the engine; set up the CLI
#   sidecar.sh stop    stop the service (it drains), then the engine; keep
#                      their logs; remove the containers, the engine's state
#                      and the network
#
# The composite actions in this directory and in stop/ run it, and so does
# hack/remote-cache/demo/run.sh, locally. Its settings are environment
# variables:
#
#   SIDECAR_ROLE            writer | read-only (required)
#   SIDECAR_SERVICE_IMAGE   the cache service image (required)
#   SIDECAR_ENGINE_IMAGE    the engine image with the remote cache client
#                           (required); its own dagger CLI is the one used
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
#                           then for the engine to connect (default: 120)
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

# --- validation: everything that becomes a name, a path or a flag ---

# Container, volume and network names; a file name too.
valid_name() { [[ "$1" =~ ^[a-z0-9][a-z0-9_.-]{0,39}$ ]]; }

# The service's own rule for a namespace (layout.ValidateNamespace).
valid_namespace() { [[ "$1" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ && "$1" != *..* ]]; }

# An image reference: registry, path, tag and digest characters only, and
# not an option.
valid_image() { [[ "$1" =~ ^[A-Za-z0-9][A-Za-z0-9._/:@+-]{0,254}$ ]]; }

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
		logs) st_logs="$value" ;;
		role) st_role="$value" ;;
		# What this sidecar's start created, one line each as it did: the
		# stop touches nothing else.
		created) case "$value" in service | engine | volume | network) st_created+=" $value " ;; esac ;;
		esac
	done <"$file"
	valid_name "$st_service" && valid_name "$st_engine" && valid_name "$st_volume" &&
		valid_name "$st_network" && [[ "$st_stop_seconds" =~ ^[0-9]+$ ]] ||
		die "the state file $file isn't one start wrote"
}

running() { [[ "$(timeout 30 docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || true)" == true ]]; }

# Whether this sidecar's start created a resource (service, engine, volume,
# network), from the state it read.
created() { [[ "$st_created" == *" $1 "* ]]; }

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

start() {
	local role="${SIDECAR_ROLE:-}"
	local name="${SIDECAR_NAME:-remote-cache}"
	local namespace="${SIDECAR_NAMESPACE:-default}"
	local drain="${SIDECAR_DRAIN_TIMEOUT:-10m}"
	local debug_port="${SIDECAR_DEBUG_PORT:-}"
	local wait="${SIDECAR_START_TIMEOUT:-120}"
	local service_image="${SIDECAR_SERVICE_IMAGE:-}" engine_image="${SIDECAR_ENGINE_IMAGE:-}"

	case "$role" in
	writer | read-only) ;;
	*) die "role must be writer or read-only, not '$role'" ;;
	esac
	valid_name "$name" || die "name '$name' isn't valid: 1-40 of a-z, 0-9, '.', '_' and '-', starting with a letter or digit"
	valid_namespace "$namespace" || die "namespace '$namespace' isn't valid: 1-128 letters, digits, '.', '_' or '-', starting with a letter or digit, without '..'"
	valid_image "$service_image" || die "service-image '$service_image' isn't an image reference"
	valid_image "$engine_image" || die "engine-image '$engine_image' isn't an image reference"
	local drain_seconds
	drain_seconds="$(duration_seconds "$drain")" || die "drain-timeout '$drain' isn't a duration such as 10m, 90s or 1h30m"
	[[ -z "$debug_port" || ("$debug_port" =~ ^[0-9]{1,5}$ && "$debug_port" -ge 1 && "$debug_port" -le 65535) ]] ||
		die "debug-port '$debug_port' isn't a port number"
	[[ "$wait" =~ ^[0-9]{1,4}$ ]] || die "SIDECAR_START_TIMEOUT '$wait' isn't a number of seconds"
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
	local var
	for var in ENDPOINT REGION BUCKET ACCESS_KEY_ID SECRET_ACCESS_KEY; do
		[[ -n "$(printenv "CACHESERVICE_S3_$var" || true)" ]] || die "the S3 setting CACHESERVICE_S3_$var is empty: pass it from the caller's secrets or variables"
	done
	if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
		# Masked in this job's log from here on, whatever their source.
		echo "::add-mask::$CACHESERVICE_S3_ACCESS_KEY_ID"
		echo "::add-mask::$CACHESERVICE_S3_SECRET_ACCESS_KEY"
	fi

	local dir
	dir="$(sidecar_dir)/$name"
	local state="$dir/state" logs="$dir/logs" bin="$dir/bin"
	if [[ -f "$state" ]]; then
		die "a sidecar named $name is already set up ($state): stop it first, or give this one another name"
	fi
	mkdir -p "$logs" "$bin"
	local service="$name-service" engine="$name-engine" volume="$name-engine-state" network="$name-net"
	# The stop's own bound: the service's whole stop takes at most the drain
	# plus 5s (README, "Stopping and draining"); docker stop waits 5s more
	# before it sends SIGKILL.
	local stop_seconds=$((drain_seconds + 10))
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
		"stop_seconds=$stop_seconds" "logs=$logs" "role=$role" >"$state"

	# Pulled only when not here already: a job may have built them.
	local image
	for image in "$service_image" "$engine_image"; do
		# Bounded too: a daemon that doesn't answer reads as "not here", and
		# the pull's own bound then fails the step.
		if ! timeout "${SIDECAR_INSPECT_TIMEOUT:-30}" docker image inspect "$image" >/dev/null 2>&1; then
			echo "remote-cache-sidecar: pulling $image"
			timeout 900 docker pull -q "$image" >/dev/null || die "pulling $image failed or took over 15m"
		fi
	done

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

	local flags=(-scope "$namespace" -drain-timeout "$drain")
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
		mkdir -p "$secrets" && chmod 700 "$secrets" || die "creating $secrets failed"
		token="$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')"
		[[ "$token" =~ ^[0-9a-f]{64}$ ]] || die "generating the debug token failed"
		[[ -n "${GITHUB_ACTIONS:-}" ]] && echo "::add-mask::$token"
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
		running "$service" || { show_log "$service" 200; die "the service exited at startup (code $(docker inspect -f '{{.State.ExitCode}}' "$service"))"; }
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
	local config="${SIDECAR_ENGINE_CONFIG:-$here/engine.json}"
	[[ -f "$config" && "$config" != *:* ]] || die "engine-config '$config' isn't a file (or its path has a ':')"
	config="$(cd "$(dirname "$config")" && pwd)/$(basename "$config")"
	local mounts=(-v "$config:/etc/dagger/engine.json:ro")
	if [[ -n "${SIDECAR_ENGINE_CA_FILE:-}" ]]; then
		[[ -f "$SIDECAR_ENGINE_CA_FILE" ]] || die "SIDECAR_ENGINE_CA_FILE $SIDECAR_ENGINE_CA_FILE isn't a file"
		[[ "$SIDECAR_ENGINE_CA_FILE" != *:* ]] || die "SIDECAR_ENGINE_CA_FILE's path has a ':'"
		mounts+=(-v "$SIDECAR_ENGINE_CA_FILE:/etc/ssl/certs/ca-certificates.crt:ro")
	fi
	echo "remote-cache-sidecar: starting the engine ($engine_image)"
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
	deadline=$((SECONDS + wait)) up=false
	while ((SECONDS < deadline)); do
		running "$service" || { show_log "$engine" 100; show_log "$service" 200; die "the service exited while the engine started"; }
		running "$engine" || { show_log "$engine" 200; die "the engine exited at startup"; }
		log_has "$service" 'msg="engine connected"' && { up=true; break; }
		sleep 1
	done
	[[ "$up" == true ]] || { show_log "$engine" 100; show_log "$service" 100; die "the engine didn't connect to the service within ${wait}s"; }
	echo "remote-cache-sidecar: the engine is connected"

	# The engine image's own CLI, which matches the engine.
	timeout 120 docker cp "$engine:/usr/local/bin/dagger" "$dir/dagger" >/dev/null ||
		die "copying the CLI out of the engine image failed"
	chmod +x "$dir/dagger"
	# A wrapper, so whatever a later step's environment holds, the CLI uses
	# this engine and sends its telemetry to this service, and never
	# continues an inherited trace: a CLI that continues an unsampled
	# TRACEPARENT records no spans, and the service learns of no entry and
	# exports nothing (cmd/cacheservice/README.md, "Real-engine run"). The
	# CLI also stops early, with the service's log, if the service is gone.
	local q_service q_dagger
	q_service="$(printf %q "$service")" q_dagger="$(printf %q "$dir/dagger")"
	cat >"$bin/dagger" <<EOF
#!/usr/bin/env bash
# Written by remote-cache-sidecar (.github/actions/remote-cache-sidecar).
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
	local st_service="" st_engine="" st_volume="" st_network="" st_stop_seconds="" st_logs="" st_role="" st_created=" "
	if ! read_state "$state"; then
		echo "remote-cache-sidecar: nothing to stop: no state at $state (did the start step run?)"
		return 0
	fi
	local failed=""

	local was_running=false code="" still_running=false
	if created service && running "$st_service"; then
		was_running=true
		echo "remote-cache-sidecar: stopping the service; it drains for up to $((st_stop_seconds - 10))s, and docker stop waits ${st_stop_seconds}s"
		# Bounded twice: docker stop sends SIGKILL at its --time, and the
		# command itself can't outlive that by more than a minute.
		timeout $((st_stop_seconds + 60)) docker stop --time "$st_stop_seconds" "$st_service" >/dev/null 2>&1 || true
		# A docker stop that gave up (or was cut off) leaves the service
		# running, with the exit code of a container that hasn't exited:
		# never take that for a clean stop.
		if running "$st_service"; then
			still_running=true
		fi
	fi
	if created service && timeout 30 docker container inspect "$st_service" >/dev/null 2>&1; then
		code="$(docker inspect -f '{{.State.ExitCode}}' "$st_service")"
		timeout 60 docker logs "$st_service" 2>&1 | redact >"$st_logs/service.log" || true
	fi
	if created engine && timeout 30 docker container inspect "$st_engine" >/dev/null 2>&1; then
		# After the service: the drain's exports run over the engine's
		# connection, and the engine uploads the layers itself. The engine's
		# own stop then waits to send its last cache event to the service,
		# which is gone, so docker stop usually kills it at 30s; its state
		# is thrown away with the job, so that costs only the time.
		timeout 90 docker stop --time 30 "$st_engine" >/dev/null 2>&1 || true
		timeout 60 docker logs "$st_engine" 2>&1 | redact >"$st_logs/engine.log" || true
	fi
	# Only what this sidecar's start created.
	created service && { timeout 60 docker rm -f "$st_service" >/dev/null 2>&1 || true; }
	created engine && { timeout 60 docker rm -f "$st_engine" >/dev/null 2>&1 || true; }
	created volume && { timeout 60 docker volume rm -f "$st_volume" >/dev/null 2>&1 || true; }
	created network && { timeout 60 docker network rm "$st_network" >/dev/null 2>&1 || true; }
	rm -f "$state"
	# The debug token, if start generated one: it opened only this service.
	rm -rf -- "$(sidecar_dir)/$name/secrets"
	[[ -n "${GITHUB_OUTPUT:-}" ]] && echo "logs=$st_logs" >>"$GITHUB_OUTPUT"

	if [[ -f "$st_logs/service.log" ]]; then
		echo "remote-cache-sidecar: the drain, from the service's log:"
		grep -E 'msg="?(stopping|drain|stopped|export stored)' "$st_logs/service.log" | tail -n 20 || true
	fi
	if [[ "$still_running" == true ]]; then
		failed="the service was still running after docker stop gave up: it was killed, and what it was exporting is lost"
	elif [[ -z "$code" ]] && ! created service; then
		# The start failed before it ran the service; its step said why.
		echo "remote-cache-sidecar: the start didn't get as far as the service; cleaned up what it left"
		return 0
	elif [[ -z "$code" ]]; then
		failed="the service's container is gone, so its exit code is unknown"
	elif [[ "$was_running" != true ]]; then
		failed="the service exited during the job, with code $code, before the stop"
	elif [[ "$code" == 137 ]]; then
		failed="the service was still stopping after ${st_stop_seconds}s, and docker killed it (137): what it was exporting is lost"
	elif [[ "$code" != 0 ]]; then
		failed="the service's stop wasn't clean (exit $code): the shutdown's bound cut work off, or a server failed; see its log"
	fi
	if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
		{
			echo "### remote-cache-sidecar: $st_role stopped"
			echo
			echo "- service exit code: ${code:-unknown}"
			echo "- logs: \`$st_logs\`"
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
	sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
	;;
esac
