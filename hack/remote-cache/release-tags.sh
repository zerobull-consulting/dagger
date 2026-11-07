#!/usr/bin/env bash
# Moves the release image's floating engine-version tags
# (zerobull-consulting/dagger#117, D4), as cacheservice-publish.yml's
# engine-tags job does. Run it in a checkout of the branch's head, with
# its history, logged in to the registry:
#
#   IMAGE=ghcr.io/zerobull-consulting/dagger-remote-cache \
#   OLD_IMAGE=ghcr.io/zerobull-consulting/cacheservice release-tags.sh
#
# It picks the newest verified build: the newest commit of the branch,
# following first parents from HEAD (at most RELEASE_TAGS_DEPTH, default
# 50), whose $IMAGE:<commit> is pushed. The publish workflow pushes that tag
# only once the commit's matrix is green. Then it points $IMAGE:<version> at
# that build for each accept line of that commit's own manifest, and
# $OLD_IMAGE:feature-remote-cache too (the old name, for the transition).
# A version that build no longer accepts keeps its tag where it is. Every
# copy is registry-side, with the same digest. With no verified build in
# reach, it moves nothing and succeeds. A registry error other than "not
# found" fails it before it moves anything.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${IMAGE:?IMAGE is not set}" "${OLD_IMAGE:?OLD_IMAGE is not set}"
depth="${RELEASE_TAGS_DEPTH:-50}"
[[ "$depth" =~ ^[0-9]{1,4}$ ]] || { echo "release-tags.sh: RELEASE_TAGS_DEPTH '$depth' isn't a number" >&2; exit 1; }

target=""
for c in $(git rev-list --first-parent --max-count="$depth" HEAD); do
	if out="$(timeout 60 docker buildx imagetools inspect "$IMAGE:$c" 2>&1)"; then
		target="$c"
		break
	fi
	if ! grep -q 'not found' <<<"$out"; then
		echo "$out" >&2
		echo "release-tags.sh: looking up $IMAGE:$c failed: moving nothing" >&2
		exit 1
	fi
done
if [[ -z "$target" ]]; then
	echo "no commit among the branch's last $depth has a pushed $IMAGE tag: nothing to move"
	exit 0
fi

# That build's own accept list, checked as the service's parser checks it;
# each version matched its pattern, so it is a valid tag.
manifest="$(mktemp)"
trap 'rm -f "$manifest"' EXIT
git show "$target:cmd/cacheservice/compat/engines.txt" >"$manifest"
versions="$(ENGINES_FILE="$manifest" "$here/engines.sh" versions | tr -d '[]"' | tr ',' ' ')"
for v in $versions; do
	timeout 300 docker buildx imagetools create --prefer-index=false -t "$IMAGE:$v" "$IMAGE:$target"
	echo "$IMAGE:$v -> $target"
done
timeout 300 docker buildx imagetools create --prefer-index=false -t "$OLD_IMAGE:feature-remote-cache" "$IMAGE:$target"
echo "$OLD_IMAGE:feature-remote-cache -> $target"
