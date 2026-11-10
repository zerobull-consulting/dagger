#!/usr/bin/env bash
# Set up and tear down the Spaces test bucket and its three keys.
#
#   spaces.sh up                  create the bucket and the admin, write and read keys
#   spaces.sh env admin|write|read  print one key's CACHESERVICE_S3_* settings as exports
#   spaces.sh github write|read   store the write or the read key's settings in GitHub with gh
#   spaces.sh lifecycle <action> [flags]
#                                 run `cacheservice lifecycle` (claim, plan, apply, check)
#                                 with the admin key, from your own shell
#   spaces.sh down                destroy the keys and the bucket, objects included
#
# For a bucket per GitHub repository, give each its own workspace, and the
# same TF_WORKSPACE and -var arguments to every command:
#
#   export TF_WORKSPACE=dagger-ci
#   spaces.sh up -var github_repository=owner/name \
#     -var github_write_environment=main -var github_read_environment=pull-requests \
#     -var github_read_name_prefix=CACHE_S3_
#   spaces.sh github write
#   spaces.sh github read
#
# Needs terraform (or tofu via TF=tofu) and one DigitalOcean API token in
# DIGITALOCEAN_TOKEN (or TF_VAR_do_token). The bucket is named
# <bucket_name_prefix>-<random slug> unless -var bucket_name=... is given.
# Extra arguments to up and down go to terraform, and to lifecycle go to
# cacheservice lifecycle. lifecycle builds ./cmd/cacheservice with go unless
# CACHESERVICE names a binary, and runs it in the directory spaces.sh was
# run from, so a relative -tokens-file works.
# See README.md.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
caller="$PWD"
tf="${TF:-terraform}"
cd "$here"

# exports <role>: one key's CACHESERVICE_S3_* settings as export lines.
exports() {
	"$tf" output -json "$1" |
		python3 -I -c 'import json, shlex, sys
for k, v in sorted(json.load(sys.stdin).items()):
    print("export %s=%s" % (k, shlex.quote(v)))'
}

need() {
	export TF_VAR_do_token="${TF_VAR_do_token:-${DIGITALOCEAN_TOKEN:-}}"
	[[ -n "$TF_VAR_do_token" ]] || {
		echo "spaces.sh: set DIGITALOCEAN_TOKEN (or TF_VAR_do_token)" >&2
		exit 2
	}
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
	echo "Into GitHub: ./spaces.sh github write, ./spaces.sh github read"
	echo "Lifecycle rules: ./spaces.sh lifecycle claim|plan|apply|check"
	;;
env)
	role="${1:-}"
	case "$role" in admin | write | read) ;; *)
		echo "usage: spaces.sh env admin|write|read" >&2
		exit 2
		;;
	esac
	exports "$role"
	;;
github)
	role="${1:-}"
	case "$role" in write | read) ;; *)
		echo "usage: spaces.sh github write|read (the admin key never goes to GitHub)" >&2
		exit 2
		;;
	esac
	command -v gh >/dev/null || {
		echo "spaces.sh: github needs the gh CLI, logged in (gh auth login)" >&2
		exit 2
	}
	# The script pipes each value into gh from printf, a bash builtin, so no
	# secret is on a command line, and nothing is written to disk.
	"$tf" output -raw "gh_secret_set_$role" | bash
	;;
lifecycle)
	action="${1:-}"
	case "$action" in claim | plan | apply | check) ;; *)
		echo "usage: spaces.sh lifecycle claim|plan|apply|check [flags]" >&2
		echo "  a fresh bucket: claim, then plan, then apply, then check" >&2
		exit 2
		;;
	esac
	# The admin key (Spaces: full access) is the one that can read and write
	# lifecycle rules. It is exported into this process only, for this one
	# command, never into your shell.
	settings="$(exports admin)"
	eval "$settings"
	bin="${CACHESERVICE:-}"
	if [[ -z "$bin" ]]; then
		tmp="$(mktemp -d)"
		trap 'rm -rf "$tmp"' EXIT
		go build -C "$root" -o "$tmp/cacheservice" ./cmd/cacheservice
		bin="$tmp/cacheservice"
	fi
	echo "bucket and prefix: $CACHESERVICE_S3_BUCKET/$CACHESERVICE_S3_PREFIX (claim and apply need -confirm=$CACHESERVICE_S3_BUCKET/$CACHESERVICE_S3_PREFIX)" >&2
	cd "$caller"
	"$bin" lifecycle "$@"
	;;
down)
	need
	"$tf" init -input=false
	"$tf" destroy -input=false -auto-approve "$@"
	;;
*)
	echo "usage: spaces.sh up|env <role>|github write|read|lifecycle <action>|down" >&2
	exit 2
	;;
esac
