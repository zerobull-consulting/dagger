#!/usr/bin/env bash
# Set up and tear down the Spaces test bucket and its three keys.
#
#   spaces.sh up                  create the bucket and the admin, write and read keys
#   spaces.sh env admin|write|read  print one key's CACHESERVICE_S3_* settings as exports
#   spaces.sh github              store the write (or read) key's settings in GitHub with gh
#   spaces.sh down                destroy the keys and the bucket, objects included
#
# For a bucket per GitHub repository and environment, give each its own
# workspace, and the same TF_WORKSPACE and -var arguments to every command:
#
#   export TF_WORKSPACE=dagger-ci
#   spaces.sh up -var github_repository=owner/name -var github_environment=ci
#   spaces.sh github
#
# Needs terraform (or tofu via TF=tofu) and one DigitalOcean API token in
# DIGITALOCEAN_TOKEN (or TF_VAR_do_token). The bucket is named
# <bucket_name_prefix>-<random slug> unless -var bucket_name=... is given.
# Extra arguments go to terraform.
# See README.md.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tf="${TF:-terraform}"
cd "$here"

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
	echo "Into GitHub: ./spaces.sh github"
	;;
env)
	role="${1:-}"
	case "$role" in admin | write | read) ;; *)
		echo "usage: spaces.sh env admin|write|read" >&2
		exit 2
		;;
	esac
	"$tf" output -json "$role" |
		python3 -I -c 'import json, shlex, sys
for k, v in sorted(json.load(sys.stdin).items()):
    print("export %s=%s" % (k, shlex.quote(v)))'
	;;
github)
	command -v gh >/dev/null || {
		echo "spaces.sh: github needs the gh CLI, logged in (gh auth login)" >&2
		exit 2
	}
	# The script pipes each value into gh from printf, a bash builtin, so no
	# secret is on a command line, and nothing is written to disk.
	"$tf" output -raw gh_secret_set | bash
	;;
down)
	need
	"$tf" init -input=false
	"$tf" destroy -input=false -auto-approve "$@"
	;;
*)
	echo "usage: spaces.sh up|env <role>|github|down" >&2
	exit 2
	;;
esac
