# UTILITY FUNCS

# Scrape a Refinery node's Prometheus metrics endpoint.
#
# Arguments:
#   $1 - the host port the node's metrics endpoint is published on
metrics_for() {
	PORT=${1:?metrics port is a required parameter}
	curl --fail --silent --show-error "http://localhost:${PORT}/metrics"
}

# Read a single gauge or counter from a node's metrics.
#
# Returns empty if it can't find the metric. Callers should treat empty as a failure.
#
# Arguments:
#   $1 - the host port the node's metrics endpoint is published on
#   $2 - the metric name
metric_value_for() {
	PORT=${1:?metrics port is a required parameter}
	METRIC=${2:?metric name is a required parameter}
	metrics_for "${PORT}" | awk -v metric="${METRIC}" '$1 == metric { print $2 }'
}

# The number of peers a node currently sees in its cluster.
#
# Arguments:
#   $1 - the host port the node's metrics endpoint is published on
num_peers_for() {
	metric_value_for "${1:?metrics port is a required parameter}" "num_peers"
}

# How long the assert_eventually* helpers keep trying, and how long they pause
# between attempts. Both in seconds. To give one assertion longer, prefix it:
#
#   EVENTUALLY_TIMEOUT=60 assert_eventually_equal ...
EVENTUALLY_TIMEOUT=${EVENTUALLY_TIMEOUT:-30}
EVENTUALLY_TICK=${EVENTUALLY_TICK:-1}

# Poll until a command succeeds, or the timeout passes.
#
# The bash cousin of testify's assert.Eventually, which the Go suite leans on
# heavily. Timeout and tick come from the constants above, not the call site.
#
# Arguments:
#   $@ - the command to poll
assert_eventually() {
	local deadline=$(( SECONDS + EVENTUALLY_TIMEOUT ))
	echo -n "# ⏳ Waiting up to ${EVENTUALLY_TIMEOUT}s for: $*" >&3
	until "$@" >/dev/null 2>&1
	do
		if (( SECONDS >= deadline )); then
			echo "" >&3
			{
				echo
				echo "-- 💥 condition never became true 💥 --"
				echo "command : $*"
				echo "waited  : ${EVENTUALLY_TIMEOUT}s"
				echo "--"
				echo
			} >&2 # output error to STDERR
			return 1
		fi
		echo -n "." >&3
		sleep "${EVENTUALLY_TICK}"
	done
	echo "" >&3
}

# Poll until a command's output equals an expected value, or the timeout
# passes. Fails showing both values, reporting the last value seen.
#
# Arguments:
#   $1 - the expected output
#   $@ - the command to poll
assert_eventually_equal() {
	local expected=${1:?expected value is a required parameter}
	shift
	local deadline=$(( SECONDS + EVENTUALLY_TIMEOUT ))
	local actual
	echo -n "# ⏳ Waiting up to ${EVENTUALLY_TIMEOUT}s for '$*' to be ${expected}" >&3
	while true
	do
		actual=$("$@" 2>/dev/null)
		[[ ${actual} == "${expected}" ]] && break
		(( SECONDS >= deadline )) && break
		echo -n "." >&3
		sleep "${EVENTUALLY_TICK}"
	done
	echo "" >&3
	assert_equal "${actual}" "${expected}"
}

# If values are not equal, fail and show both.
#
# Lifted and then drastically simplified from bats-assert * bats-support
assert_equal() {
	if [[ $1 != "$2" ]]; then
		{
			echo
			echo "-- 💥 values are not equal 💥 --"
			echo "expected : $2"
			echo "actual   : $1"
			echo "--"
			echo
		} >&2 # output error to STDERR
		return 1
	fi
}
