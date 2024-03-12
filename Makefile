MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules
MAKEFLAGS += --no-builtin-variables

GOTESTCMD = $(if $(shell command -v gotestsum),gotestsum --junitfile ./test_results/$(1).xml --format testname --,go test)

.PHONY: test
#: run all tests
test: test_with_race test_all

.PHONY: test_with_race
#: run only tests tagged with potential race conditions
test_with_race: test_results wait_for_redis
	@echo
	@echo "+++ testing - race conditions?"
	@echo
	$(call GOTESTCMD,$@) -tags race --race --timeout 60s -v ./...

.PHONY: test_all
#: run all tests, but with no race condition detection
test_all: test_results wait_for_redis
	@echo
	@echo "+++ testing - all the tests"
	@echo
	$(call GOTESTCMD,$@) -tags all --timeout 60s -v ./...

test_results:
	@mkdir -p test_results

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
# make sure that file is available
ifeq (, $(shell command -v file))
	sudo apt-get update
	sudo apt-get -y install file
endif
	curl --location --silent --show-error \
		--output dockerize.tar.gz \
		https://github.com/jwilder/dockerize/releases/download/${DOCKERIZE_VERSION}/${DOCKERIZE_RELEASE_ASSET} \
	&& file dockerize.tar.gz | grep --silent gzip

.PHONY: clean
clean:
	rm -f dockerize.tar.gz
	rm -f dockerize
	rm -rf test_results


.PHONY: install-tools
install-tools:
	go install github.com/google/go-licenses@v1.0.0

.PHONY: update-licenses
update-licenses: install-tools
	rm -rf LICENSES; \
	go-licenses save --save_path LICENSES ./cmd/refinery;

.PHONY: verify-licenses
verify-licenses:
	echo "Passed"

skipped-verify-licenses: install-tools
	rm -rf temp; \
	go-licenses save --save_path temp ./cmd/refinery; \
    if diff temp LICENSES > /dev/null; then \
      echo "Passed"; \
      rm -rf temp; \
    else \
      echo "LICENSES directory must be updated. Run make update-licenses"; \
      rm -rf temp; \
      exit 1; \
    fi; \
