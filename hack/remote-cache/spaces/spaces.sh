#!/usr/bin/env bash
# Set up and tear down a Spaces bucket for the remote cache, its three keys,
# its tokens file, its lifecycle rules, and the GitHub secrets of its jobs.
#
#   spaces.sh setup [terraform args]   all of it, in order: up, lifecycle claim,
#                                 apply and check, then github write and read
#                                 (when github_repository is set). Safe to rerun.
#
#   spaces.sh up [terraform args] create the bucket and the admin, write and read keys
#   spaces.sh env admin|write|read  print one key's CACHESERVICE_S3_* settings as exports
#   spaces.sh github write|read   store the write or the read key's settings in GitHub with gh
#   spaces.sh tokens add <namespace>    add a new random token; prints it
#   spaces.sh tokens list               the tokens' namespaces and fingerprints (no tokens)
#   spaces.sh tokens revoke <fingerprint|namespace>
#   spaces.sh tokens path               the tokens file of this workspace
#   spaces.sh lifecycle <action> [flags]
#                                 run `cacheservice lifecycle` (claim, plan, apply, check)
#                                 with the admin key, from your own shell
#   spaces.sh down [terraform args]  destroy the keys and the bucket, objects included
#
# For a bucket per GitHub repository, give each its own workspace, and the
# same TF_WORKSPACE and -var arguments to every command (best in a .tfvars):
#
#   export TF_WORKSPACE=dagger
#   spaces.sh setup -var-file=dagger.tfvars
#
# Needs terraform (or tofu via TF=tofu) and one DigitalOcean API token in
# DIGITALOCEAN_TOKEN (or TF_VAR_do_token); gh for github; go for lifecycle
# (unless CACHESERVICE names a cacheservice binary). Each workspace's tokens
# file is tokens/<workspace>.tokens here (git-ignored, mode 0600), or
# $TOKENS_FILE. lifecycle plan, apply and check use it when it exists and no
# -tokens-file or -scope is given, and run in the directory spaces.sh was run
# from, so a relative -tokens-file works.
# See README.md.
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

need() {
	export TF_VAR_do_token="${TF_VAR_do_token:-${DIGITALOCEAN_TOKEN:-}}"
	[[ -n "$TF_VAR_do_token" ]] || die "set DIGITALOCEAN_TOKEN (or TF_VAR_do_token)"
}

tokens_file() {
	if [[ -n "${TOKENS_FILE:-}" ]]; then
		# Absolute, since lifecycle runs from the caller's directory.
		case "$TOKENS_FILE" in /*) echo "$TOKENS_FILE" ;; *) echo "$caller/$TOKENS_FILE" ;; esac
	else
		echo "$here/tokens/$("$tf" workspace show).tokens"
	fi
}

# The service's tokenFingerprint: the first 4 bytes of the SHA-256, in hex.
fingerprint() {
	printf '%s' "$1" | sha256sum | cut -c1-8
}

# The service's namespace rule (README.md, "The tokens file").
valid_namespace() {
	[[ ${#1} -le 128 && "$1" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ && "$1" != *..* ]]
}

# Whether the lifecycle flags already choose the namespaces.
has_namespace_flag() {
	local a
	for a in "$@"; do
		case "$a" in -tokens-file | --tokens-file | -tokens-file=* | --tokens-file=* | -scope | --scope | -scope=* | --scope=*) return 0 ;; esac
	done
	return 1
}

# lifecycle <action> [flags]: run cacheservice lifecycle with the admin key.
lifecycle() {
	local action="${1:-}"
	case "$action" in claim | plan | apply | check) ;; *)
		echo "usage: spaces.sh lifecycle claim|plan|apply|check [flags]" >&2
		echo "  a fresh bucket: claim, then plan, then apply, then check (or: spaces.sh setup)" >&2
		exit 2
		;;
	esac
	shift
	local file
	file="$(tokens_file)"
	local args=("$@")
	if [[ "$action" != claim && -f "$file" ]] && ! has_namespace_flag "$@"; then
		args=(-tokens-file "$file" "${args[@]}")
	fi
	(
		# The admin key (Spaces: full access) is the one that can read and
		# write lifecycle rules. It is exported into this subshell only, for
		# this one command, never into your shell.
		settings="$(exports admin)"
		eval "$settings"
		bin="${CACHESERVICE:-}"
		if [[ -z "$bin" ]]; then
			tmp="$(mktemp -d)"
			trap 'rm -rf "$tmp"' EXIT
			go build -C "$root" -o "$tmp/cacheservice" ./cmd/cacheservice
			bin="$tmp/cacheservice"
		fi
		echo "lifecycle $action on $CACHESERVICE_S3_BUCKET/$CACHESERVICE_S3_PREFIX (claim and apply need -confirm=$CACHESERVICE_S3_BUCKET/$CACHESERVICE_S3_PREFIX)" >&2
		cd "$caller"
		"$bin" lifecycle "$action" ${args[@]+"${args[@]}"}
	)
}

github() {
	local role="${1:-}"
	case "$role" in write | read) ;; *)
		die "usage: spaces.sh github write|read (the admin key never goes to GitHub)"
		;;
	esac
	command -v gh >/dev/null || die "github needs the gh CLI, logged in (gh auth login)"
	# The script pipes each value into gh from printf, a bash builtin, so no
	# secret is on a command line, and nothing is written to disk.
	"$tf" output -raw "gh_secret_set_$role" | bash
}

up() {
	need
	"$tf" init -input=false
	"$tf" apply -input=false -auto-approve "$@"
}

cmd="${1:-}"
[[ $# -gt 0 ]] && shift
case "$cmd" in
setup)
	up "$@"
	confirm="$("$tf" output -raw bucket)/$("$tf" output -raw prefix)"
	# This state just created (or already owns) the bucket, so setup repeats
	# its name for -confirm itself. claim is a no-op on a claimed prefix.
	lifecycle claim -confirm="$confirm"
	lifecycle apply -confirm="$confirm"
	lifecycle check
	repo="$("$tf" output -raw github_repository)"
	if [[ -n "$repo" ]]; then
		echo "storing the write and read keys' settings in $repo" >&2
		github write
		github read
	else
		echo "github_repository isn't set: skipped GitHub. Set it, or run ./spaces.sh github write|read in the repository" >&2
	fi
	;;
up)
	up "$@"
	echo
	echo "Settings for a key: ./spaces.sh env admin|write|read"
	echo "Into GitHub: ./spaces.sh github write, ./spaces.sh github read"
	echo "Lifecycle rules: ./spaces.sh lifecycle claim|plan|apply|check"
	;;
env)
	case "${1:-}" in admin | write | read) ;; *) die "usage: spaces.sh env admin|write|read" ;; esac
	exports "$1"
	;;
github)
	github "$@"
	;;
tokens)
	file="$(tokens_file)"
	case "${1:-}" in
	path)
		echo "$file"
		;;
	add)
		ns="${2:-}"
		valid_namespace "$ns" || die "usage: spaces.sh tokens add <namespace>: 1-128 letters, digits, . _ or -, starting with a letter or digit, without .."
		mkdir -p "$(dirname "$file")"
		(umask 077 && touch "$file")
		token="$(openssl rand -hex 32)"
		printf '%s %s\n' "$token" "$ns" >>"$file"
		echo "added a token for $ns (fingerprint $(fingerprint "$token")) to $file" >&2
		echo "run ./spaces.sh lifecycle apply to give a new namespace its rule" >&2
		echo "$token"
		;;
	list)
		[[ -f "$file" ]] || die "no tokens file: $file (spaces.sh tokens add <namespace>)"
		while read -r token ns _; do
			[[ -z "$token" || "$token" == \#* ]] && continue
			printf '%s  %s\n' "$(fingerprint "$token")" "$ns"
		done <"$file"
		;;
	revoke)
		which="${2:-}"
		[[ -n "$which" ]] || die "usage: spaces.sh tokens revoke <fingerprint|namespace>"
		[[ -f "$file" ]] || die "no tokens file: $file"
		tmp="$(mktemp "$file.XXXXXX")"
		n=0
		while IFS= read -r line; do
			read -r token ns _ <<<"$line" || true
			if [[ -n "$token" && "$token" != \#* ]] && [[ "$(fingerprint "$token")" == "$which" || "$ns" == "$which" ]]; then
				n=$((n + 1))
				continue
			fi
			printf '%s\n' "$line"
		done <"$file" >"$tmp"
		mv "$tmp" "$file"
		echo "revoked $n token(s) matching $which in $file" >&2
		echo "a running service drops them on SIGHUP; a namespace with no tokens left keeps its rule until lifecycle apply -allow-removals" >&2
		;;
	*)
		die "usage: spaces.sh tokens add <namespace>|list|revoke <fingerprint|namespace>|path"
		;;
	esac
	;;
lifecycle)
	lifecycle "$@"
	;;
down)
	need
	"$tf" init -input=false
	"$tf" destroy -input=false -auto-approve "$@"
	;;
*)
	die "usage: spaces.sh setup|up|env <role>|github write|read|tokens ...|lifecycle <action>|down"
	;;
esac
