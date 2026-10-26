#!/usr/bin/env bats

# Smoke test: Refinery stays alive and ready while Redis stops answering.
#
# Stands on its own: brings up the same stack as 10-peers.bats if it isn't
# already running, and always leaves Redis running afterwards.
#
# `docker compose pause` freezes Redis without closing its sockets. The
# kernel still completes TCP handshakes, but nothing answers after that, so
# Refinery's TLS handshakes and commands hang rather than fail fast. That is
# closer to a Redis pod disappearing behind a stale address than
# `docker compose stop`, which refuses connections at once.

load test_helpers/utilities

COMPOSE="docker compose"

# Published ports, from docker-compose.yaml
NODE1_HTTP=8080
NODE2_HTTP=8081

EXPECTED_PEERS=2
NODE1_METRICS=2112
NODE2_METRICS=2113

# Longer than stress relief's 15s health timeout plus go-redis's 5s read
# timeout, so a stalled subsystem has time to be marked dead.
UNRESPONSIVE_WINDOW=30

setup_file() {
	echo "# 🚧 Spinning up two Refinery nodes and a TLS'd Redis" >&3
	${COMPOSE} up --detach --wait --wait-timeout 120
	assert_eventually_equal "${EXPECTED_PEERS}" num_peers_for "${NODE1_METRICS}"
	assert_eventually_equal "${EXPECTED_PEERS}" num_peers_for "${NODE2_METRICS}"
}

teardown_file() {
	# Whatever happened during the test suite, leave the stack whole for poking at.
	${COMPOSE} unpause redis 2>/dev/null || true
	assert_eventually redis_healthy
}

# Compose's healthcheck stays "unhealthy" for a few probes after unpausing,
# and `up --wait` fails at once on an unhealthy container. Poll instead.
redis_healthy() {
	[[ $(${COMPOSE} ps redis --format '{{.Health}}') == "healthy" ]]
}

both_nodes_ok() {
	http_ok_for "${NODE1_HTTP}" "$1" && http_ok_for "${NODE2_HTTP}" "$1"
}

# TESTS

@test "Refinery nodes are alive and ready before Redis stops answering" {
	both_nodes_ok /alive
	both_nodes_ok /ready
}

@test "Refinery nodes stay alive while Redis is unresponsive" {
	${COMPOSE} pause redis
	assert_always "${UNRESPONSIVE_WINDOW}" both_nodes_ok /alive
}

@test "Refinery nodes are still ready while Redis is unresponsive" {
	both_nodes_ok /ready
}

@test "Redis answers again after unpausing" {
	${COMPOSE} unpause redis
	assert_eventually redis_healthy
}

@test "Refinery nodes are alive again after Redis answers" {
	assert_eventually both_nodes_ok /alive
}
