#!/usr/bin/env bash
# Loads an image tarball the cacheservice module exported (`dagger call image
# ... as-tarball export`) into the local Docker and tags it, so the smoke test
# and the e2e can run it:
#
#   load-image.sh <tarball> <tag>
#
# The tarball's image has no name, so `docker load` reports only its ID.
set -euo pipefail

[[ $# -eq 2 ]] || { sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
tarball="$1" tag="$2"

out="$(timeout 300 docker load -i "$tarball")"
id="$(sed -n 's/^Loaded image ID: //p' <<<"$out")"
[[ -n "$id" ]] || { echo "load-image.sh: no image ID in docker load's output: $out" >&2; exit 1; }
docker tag "$id" "$tag"
echo "loaded $id as $tag"
