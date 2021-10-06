MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules
MAKEFLAGS += --no-builtin-variables

.PHONY: test
#: run all tests
test: test_with_race test_all

.PHONY: test_with_race
#: run only tests tagged with potential race conditions
test_with_race: wait_for_redis
	@echo
	@echo "+++ testing - race conditions?"
	@echo
	go test -tags race --race --timeout 60s -v ./...

.PHONY: test_all
#: run all tests, but with no race condition detection
test_all: wait_for_redis
	@echo
	@echo "+++ testing - all the tests"
	@echo
	go test -tags all --timeout 60s -v ./...

.PHONY: wait_for_redis
# wait for Redis to become available for test suite
wait_for_redis: dockerize
	@echo
	@echo "+++ We need a Redis running to run the tests."
	@echo
	@echo "Checking with dockerize $(shell ./dockerize --version)"
	@./dockerize -wait tcp://localhost:6379 -timeout 30s

# ensure the dockerize command is available
dockerize: dockerize.tar.gz
	tar xzvmf dockerize.tar.gz

HOST_OS := $(shell uname -s | tr A-Z a-z)
# You can override this version from an environment variable.
DOCKERIZE_VERSION ?= v0.6.1
DOCKERIZE_RELEASE_ASSET := dockerize-${HOST_OS}-amd64-${DOCKERIZE_VERSION}.tar.gz

dockerize.tar.gz:
	@echo
	@echo "+++ Retrieving dockerize tool for Redis readiness check."
	@echo
	curl --location --silent --show-error \
		--output dockerize.tar.gz \
		https://github.com/jwilder/dockerize/releases/download/${DOCKERIZE_VERSION}/${DOCKERIZE_RELEASE_ASSET} \
	&& file dockerize.tar.gz | grep --silent gzip

.PHONY: clean
clean:
	rm -f dockerize.tar.gz
	rm -f dockerize
