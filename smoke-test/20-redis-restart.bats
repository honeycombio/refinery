#!/usr/bin/env bats

# Smoke test: Refinery's Redis subscriptions recover after Redis restarts.
#
# Stands on its own: brings up the same stack as 10-peers.bats if it isn't
# already running, and always leaves Redis running afterwards.
#
# These watch peer_messages rather than num_peers. The num_peers gauge only
# updates when a message arrives, so a dead subscription leaves it frozen at
# its last value. peer_messages counts every message a node's subscription
# delivers, including the node's own, so it goes flat the moment delivery
# stops.

load test_helpers/utilities

COMPOSE="docker compose"

# Published ports, from docker-compose.yaml
NODE1_METRICS=2112
NODE2_METRICS=2113

EXPECTED_PEERS=2

# Longer than a node's peer refresh interval (3s plus up to 20% jitter), so
# every node publishes at least once while we watch.
REFRESH_WINDOW=5

setup_file() {
	echo "# 🚧 Spinning up two Refinery nodes and a TLS'd Redis" >&3
	${COMPOSE} up --detach --wait --wait-timeout 120
	assert_eventually_equal "${EXPECTED_PEERS}" num_peers_for "${NODE1_METRICS}"
	assert_eventually_equal "${EXPECTED_PEERS}" num_peers_for "${NODE2_METRICS}"
}

teardown_file() {
	# Whatever happened during the test suite, leave the stack whole for poking at.
	${COMPOSE} up --detach --wait redis
}

# TESTS

@test "Refinery node 1's subscription stops delivering while Redis is down" {
	${COMPOSE} stop redis
	sleep 1 # let anything already buffered on the subscription drain
	local before
	before=$(peer_messages_for "${NODE1_METRICS}")
	[[ -n ${before} ]] # confirm there's a value to assert against later
	sleep "${REFRESH_WINDOW}"
	assert_equal "$(peer_messages_for "${NODE1_METRICS}")" "${before}"
}

@test "Redis starts back up successfully" {
	${COMPOSE} up --detach --wait redis
}

@test "Refinery node 1's subscription delivers again after Redis restarts" {
	local baseline
	baseline=$(peer_messages_for "${NODE1_METRICS}")
	[[ -n ${baseline} ]]
	assert_eventually peer_messages_exceed "${NODE1_METRICS}" "${baseline}"
}

@test "Refinery node 2's subscription delivers again after Redis restarts" {
	local baseline
	baseline=$(peer_messages_for "${NODE2_METRICS}")
	[[ -n ${baseline} ]]
	assert_eventually peer_messages_exceed "${NODE2_METRICS}" "${baseline}"
}
