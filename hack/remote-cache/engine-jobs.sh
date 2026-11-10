# The engine jobs of remote-cache-tests.yml, shared by release-check.sh and
# accept-line-only.sh, which source this file (it runs nothing on its own).
#
# A run's verdict on an engine is these jobs' alone
# (zerobull-consulting/dagger#166): any other job (unit tests, the MinIO
# tests) doesn't depend on the engine, and a run whose engine jobs were
# skipped still ends green (zerobull-consulting/dagger#223).
engine_jobs=("real engines (e2e)" "sidecar demo")

engine_json="$(printf '%s\n' "${engine_jobs[@]}" | jq -R . | jq -s -c .)"
# is_engine: jq's filter for "this job is an engine job", $e being the list
# (jq --argjson e "$engine_json").
is_engine='(.name as $n | $e | index($n)) != null'

# engine_jobs_unlisted <jobs JSON>: why a run's jobs listing (one page of
# GET .../runs/<id>/jobs?per_page=100) can't be decided on, or nothing. It
# can't when it isn't a jobs list, when the run has more jobs than were
# listed (one page is all that's read, so the rest are refused, not
# fetched), or when an engine job isn't there exactly once.
engine_jobs_unlisted() {
	local n
	jq -e '.jobs | type == "array"' <<<"$1" >/dev/null 2>&1 || {
		echo "its jobs didn't read as a list of jobs"
		return
	}
	jq -e '(.total_count // (.jobs | length)) <= (.jobs | length)' <<<"$1" >/dev/null 2>&1 || {
		echo "it has more jobs than were listed"
		return
	}
	for n in "${engine_jobs[@]}"; do
		[[ "$(jq -r --arg n "$n" '[.jobs[] | select(.name == $n)] | length' <<<"$1")" == 1 ]] || {
			echo "it has no single '$n' job"
			return
		}
	done
}
