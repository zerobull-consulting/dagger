#!/usr/bin/env bash
# Set up and tear down a Spaces bucket for the remote cache: Terraform does it
# all (the bucket, its keys, the prefix's ownership marker, the lifecycle
# rules, and the GitHub environments, secrets and variables). This runs it.
#
#   spaces.sh up [terraform args]    create or update all of it; safe to rerun
#   spaces.sh env admin|write|read   print one key's CACHESERVICE_S3_* settings as exports
#   spaces.sh lifecycle plan|check [flags]
#                                    run `cacheservice lifecycle plan` or `check` with the admin
#                                    key and the namespaces Terraform set, from your own shell
#   spaces.sh down [terraform args]  destroy all of it, the bucket's objects included
#
# For a bucket per GitHub repository, give each its own workspace, and the
# same TF_WORKSPACE and -var arguments to every command (best in a .tfvars):
#
#   export TF_WORKSPACE=dagger
#   spaces.sh up -var-file=dagger.tfvars
#
# Needs terraform (or tofu via TF=tofu), one DigitalOcean API token as do_token
# (DIGITALOCEAN_TOKEN, TF_VAR_do_token, or a terraform.tfvars here), go
# (Terraform runs `cacheservice provision` with go run, unless
# cacheservice_command names a binary), and,
# with github_repository set, a GitHub token with admin rights on it in
# GITHUB_TOKEN. lifecycle builds ./cmd/cacheservice with go unless
# CACHESERVICE names a binary, and runs it in the directory spaces.sh was run
# from. See README.md.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
caller="$PWD"
tf="${TF:-terraform}"
cd "$here"

die() {
	echo "spaces.sh: $*" >&2
	exit 2
}

# exports <role>: one key's CACHESERVICE_S3_* settings as export lines.
exports() {
	"$tf" output -json "$1" |
		python3 -I -c 'import json, shlex, sys
for k, v in sorted(json.load(sys.stdin).items()):
    print("export %s=%s" % (k, shlex.quote(v)))'
}

# The DigitalOcean token: DIGITALOCEAN_TOKEN when set, else whatever
# Terraform finds (TF_VAR_do_token, terraform.tfvars, -var); Terraform says
# when there is none.
need() {
	if [[ -z "${TF_VAR_do_token:-}" && -n "${DIGITALOCEAN_TOKEN:-}" ]]; then
		export TF_VAR_do_token="$DIGITALOCEAN_TOKEN"
	fi
}

# has_namespace_flag <flags>: whether they already choose the namespaces.
has_namespace_flag() {
	local a
	for a in "$@"; do
		case "$a" in -tokens-file | --tokens-file | -tokens-file=* | --tokens-file=* | -scope | --scope | -scope=* | --scope=*) return 0 ;; esac
	done
	return 1
}

cmd="${1:-}"
[[ $# -gt 0 ]] && shift
case "$cmd" in
up)
	need
	"$tf" init -input=false
	"$tf" apply -input=false -auto-approve "$@"
	echo
	echo "Settings for a key: ./spaces.sh env admin|write|read"
	;;
env)
	case "${1:-}" in admin | write | read) ;; *) die "usage: spaces.sh env admin|write|read" ;; esac
	exports "$1"
	;;
lifecycle)
	action="${1:-}"
	case "$action" in plan | check) ;; *)
		die "usage: spaces.sh lifecycle plan|check [flags] (Terraform claims the prefix and applies the rules: spaces.sh up)"
		;;
	esac
	shift
	tmp="$(mktemp -d)"
	trap 'rm -rf "$tmp"' EXIT
	args=("$@")
	if ! has_namespace_flag "$@"; then
		# The namespaces Terraform set the rules for, as a tokens file:
		# lifecycle reads a tokens file for its namespaces only, so the
		# tokens are throwaway.
		"$tf" output -json namespaces |
			python3 -I -c 'import json, secrets, sys
for ns in json.load(sys.stdin):
    print(secrets.token_hex(16), ns)' >"$tmp/namespaces"
		args=(-tokens-file "$tmp/namespaces" ${args[@]+"${args[@]}"})
	fi
	(
		# The admin key (Spaces: full access) is the one that can read
		# lifecycle rules. It is exported into this subshell only.
		settings="$(exports admin)"
		eval "$settings"
		bin="${CACHESERVICE:-}"
		if [[ -z "$bin" ]]; then
			go build -C "$root" -o "$tmp/cacheservice" ./cmd/cacheservice
			bin="$tmp/cacheservice"
		fi
		cd "$caller"
		"$bin" lifecycle "$action" "${args[@]}"
	)
	;;
down)
	need
	"$tf" init -input=false
	"$tf" destroy -input=false -auto-approve "$@"
	;;
*)
	die "usage: spaces.sh up|env <role>|lifecycle plan|check|down"
	;;
esac
