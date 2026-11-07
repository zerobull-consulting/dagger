#!/usr/bin/env bash
# Reports how long a remote-cache/start took, to the step summary: from the
# time $2 (seconds since the epoch, with a fraction) to now. CI's sidecar demo
# job runs it after each start (zerobull-consulting/dagger#161).
#
#   report-start-time.sh <what> <start time>
set -euo pipefail
what="${1:?what}" t0="${2:?start time}"
[[ "$t0" =~ ^[0-9]+(\.[0-9]+)?$ ]] || { echo "report-start-time.sh: '$t0' isn't a time" >&2; exit 2; }
took="$(awk -v a="$t0" -v b="$(date +%s.%N)" 'BEGIN { printf "%.1f", b - a }')"
echo "the $what start took ${took}s"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
	echo "- remote-cache/start ($what): **${took}s** (install the CLI, pull the images, start, welcome)" >>"$GITHUB_STEP_SUMMARY"
fi
