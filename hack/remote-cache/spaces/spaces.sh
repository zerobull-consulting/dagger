#!/usr/bin/env bash
# Set up and tear down the Spaces test bucket and its three keys.
#
#   spaces.sh up                  create the bucket and the admin, write and read keys
#   spaces.sh env admin|write|read  print one key's CACHESERVICE_S3_* settings as exports
#   spaces.sh down                destroy the keys and the bucket, objects included
#
# Needs terraform (or tofu via TF=tofu), DIGITALOCEAN_TOKEN, and
# SPACES_ACCESS_KEY_ID / SPACES_SECRET_ACCESS_KEY of a key that can manage
# buckets. Extra arguments go to terraform: spaces.sh up -var bucket_name=...
# See README.md.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tf="${TF:-terraform}"
cd "$here"

need() {
	local v
	for v in DIGITALOCEAN_TOKEN SPACES_ACCESS_KEY_ID SPACES_SECRET_ACCESS_KEY; do
		[[ -n "${!v:-}" ]] || {
			echo "spaces.sh: $v is not set" >&2
			exit 2
		}
	done
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
down)
	need
	"$tf" init -input=false
	"$tf" destroy -input=false -auto-approve "$@"
	;;
*)
	echo "usage: spaces.sh up|env <role>|down" >&2
	exit 2
	;;
esac
