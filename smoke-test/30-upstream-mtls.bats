#!/usr/bin/env bats

# Smoke test: Refinery presents its client certificate to an upstream that
# requires mutual TLS (Network.UpstreamTLS* in config.yaml).

load test_helpers/utilities

COMPOSE="docker compose"
NODE1_API=8080

# Succeeds when the upstream has logged a request on a path prefix from a client
# whose certificate it verified as CN=refinery.
upstream_saw_refinery_cert_on() {
	${COMPOSE} logs upstream | grep -q "mtls verify=SUCCESS dn=\"CN=refinery\" ${1:?method is required} ${2:?path is required}"
}

@test "the upstream rejects a client without a certificate" {
	# nginx answers a missing client certificate with HTTP 400, not a handshake failure.
	run ${COMPOSE} exec upstream wget -q -O /dev/null --no-check-certificate https://127.0.0.1:8443/1/auth
	[[ ${status} -ne 0 && ${output} == *"400 Bad Request"* ]]
}

@test "Refinery sends events upstream with its client certificate" {
	curl --fail --silent --show-error --output /dev/null \
		-X POST "http://localhost:${NODE1_API}/1/events/smoke" \
		-H "X-Honeycomb-Team: SmokeTestKeyWith22chr" \
		-H "Content-Type: application/json" \
		-d '{"trace.trace_id":"mtls-smoke","trace.span_id":"root","name":"mtls smoke"}'

	assert_eventually upstream_saw_refinery_cert_on GET /1/auth
	assert_eventually upstream_saw_refinery_cert_on POST /1/batch/smoke
}
