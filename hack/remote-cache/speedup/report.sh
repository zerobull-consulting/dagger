#!/usr/bin/env bash
# Puts the speedup workflow's timings side by side in the step summary.
#
#   report.sh <dir with baseline.seconds, writer.seconds, reader.seconds, reader.size> <engine version>
set -euo pipefail
[[ $# -eq 2 ]] || { sed -n '4,4p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
dir="$1" version="$2"
for f in baseline.seconds writer.seconds reader.seconds reader.size; do
	[[ -f "$dir/$f" ]] || { echo "report.sh: no $f in $dir" >&2; exit 1; }
done
base="$(<"$dir/baseline.seconds")" writer="$(<"$dir/writer.seconds")" reader="$(<"$dir/reader.seconds")"
size="$(<"$dir/reader.size")"
for n in "$base" "$writer" "$reader" "$size"; do
	[[ "$n" =~ ^[0-9]+$ ]] || { echo "report.sh: '$n' isn't a number" >&2; exit 1; }
done
speedup="$(awk -v b="$base" -v r="$reader" 'BEGIN { if (r < 1) r = 1; printf "%.1f", b / r }')"
out="$(
	echo "### Remote cache speedup (engine $version)"
	echo
	echo "Building \`cmd/cacheservice\` ($size bytes) with one Dagger pipeline, each on a fresh engine:"
	echo
	echo "| Run | Seconds |"
	echo "| --- | ---: |"
	echo "| baseline, no cache | $base |"
	echo "| writer sidecar (fills the cache; a hit if this commit ran before) | $writer |"
	echo "| read-only sidecar, from the cache | $reader |"
	echo
	echo "**${speedup}x** faster than the baseline."
)"
echo "$out"
[[ -z "${GITHUB_STEP_SUMMARY:-}" ]] || echo "$out" >>"$GITHUB_STEP_SUMMARY"
