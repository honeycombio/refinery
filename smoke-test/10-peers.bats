#!/usr/bin/env bats

# Smoke test: two Refinery nodes against a Redis requiring TLS and AUTH.
#
# See docker-compose.yaml for why that is the configuration under test.

load test_helpers/utilities

COMPOSE="docker compose"

# Published ports, from docker-compose.yaml
NODE1_API=8080
NODE1_METRICS=2112
NODE2_API=8081
NODE2_METRICS=2113

EXPECTED_PEERS=2

setup_file() {
	echo "# 🚧 Spinning up two Refinery nodes and a TLS'd Redis" >&3
	${COMPOSE} up --detach --wait --wait-timeout 120
}

teardown_file() {
	echo "# 🔥 Still smoldering: the stack is left running so you can poke at it if you want." >&3
	echo "#    'make unsmoke' to spin it down. 'make resmoke' for a clean run." >&3
}

# TESTS

@test "Refinery node 1 reports ready" {
	assert_eventually curl --fail --silent --output /dev/null \
		"http://localhost:${NODE1_API}/ready"
}

@test "Refinery node 2 reports ready" {
	assert_eventually curl --fail --silent --output /dev/null \
		"http://localhost:${NODE2_API}/ready"
}

@test "Refinery node 1 sees every peer in the cluster" {
	assert_eventually_equal "${EXPECTED_PEERS}" num_peers_for "${NODE1_METRICS}"
}

@test "Refinery node 2 sees every peer in the cluster" {
	assert_eventually_equal "${EXPECTED_PEERS}" num_peers_for "${NODE2_METRICS}"
}
