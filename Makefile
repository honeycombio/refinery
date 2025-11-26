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

local_image: export PRIMARY_DOCKER_REPO=ko.local
local_image: export CIRCLE_TAG=$(shell git describe --always --match "v[0-9]*" --tags)
local_image: export CIRCLE_BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
local_image: export CIRCLE_SHA1=$(shell git rev-parse HEAD)
local_image: export SOURCE_DATE_EPOCH=$(call __latest_modification_time)
#: build the release image locally, available as "ko.local/refinery:<commit>"
local_image: ko crane
	./build-docker.sh

.PHONY: wait_for_redis
# wait for Redis to become available for test suite
wait_for_redis: dockerize
	@echo
	@echo "+++ We need a Redis running to run the tests."
	@echo
	@echo "Checking with dockerize $(shell ./dockerize --version)"
	@./dockerize -wait tcp://localhost:6379 -timeout 30s

# You can override this version from an environment variable.
HOST_OS := $(shell uname -s | tr A-Z a-z)
# You can override this version from an environment variable.
KO_VERSION ?= 0.11.2
KO_RELEASE_ASSET := ko_${KO_VERSION}_${HOST_OS}_x86_64.tar.gz
# ensure the ko command is available
ko: ko_${KO_VERSION}.tar.gz
	tar xzvmf $< ko
	chmod u+x ./ko

ko_${KO_VERSION}.tar.gz:
	@echo
	@echo "+++ Retrieving ko tool for container building."
	@echo
# make sure that file is available
ifeq (, $(shell command -v file))
	sudo apt-get update
	sudo apt-get -y install file
endif
	curl --location --silent --show-error \
	    --output ko_tmp.tar.gz \
	    https://github.com/ko-build/ko/releases/download/v${KO_VERSION}/${KO_RELEASE_ASSET} \
	&& file ko_tmp.tar.gz | grep --silent gzip \
	&& mv ko_tmp.tar.gz $@ || (echo "Failed to download ko. Got:"; cat ko_tmp.tar.gz ; echo "" ; exit 1)

# You can override this version from an environment variable.
CRANE_VERSION ?= 0.19.1
CRANE_RELEASE_ASSET := go-containerregistry_${HOST_OS}_x86_64.tar.gz
# ensure the crane command is available
crane: crane_${CRANE_VERSION}.tar.gz
	tar xzvmf $< crane
	chmod u+x ./crane

crane_${CRANE_VERSION}.tar.gz:
	@echo
	@echo "+++ Retrieving crane tool for container registry operations."
	@echo
# make sure that file is available
ifeq (, $(shell command -v file))
	sudo apt-get update
	sudo apt-get -y install file
endif
	curl --location --silent --show-error \
	    --output crane_tmp.tar.gz \
	    https://github.com/google/go-containerregistry/releases/download/v${CRANE_VERSION}/${CRANE_RELEASE_ASSET} \
	&& file crane_tmp.tar.gz | grep --silent gzip \
	&& mv crane_tmp.tar.gz $@ || (echo "Failed to download crane. Got:"; cat crane_tmp.tar.gz ; echo "" ; exit 1)

__latest_modification_time := $(strip $(if $(shell git diff --quiet && echo $$?), \
$(shell git log --max-count=1 --pretty=format:"%ct"), \
$(shell git status --short --untracked-files=no --no-column | cut -w -f 3 | xargs ls -ltr -D "%s" | tail -n 1 | cut -w -f 6)))

.PHONY: latest_modification_time
latest_modification_time:
	@echo $(call __latest_modification_time)

# ensure the dockerize command is available
dockerize: dockerize.tar.gz
	tar xzvmf dockerize.tar.gz

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
	rm -f ko_*.tar.gz
	rm -f ko
	rm -f crane_*.tar.gz
	rm -f crane
	rm -rf test_results


.PHONY: install-tools
install-tools:
	@echo "\n+++ Retrieving license checker tool."
	go mod tidy
# We install the tool so the executable for the current runtime OS/arch is available on PATH.
	go install $(shell grep "go-licenses.*\sv" go.mod | awk '{ print $$1 "@" $$2}')

LICENSES_DIR := LICENSES
# We ignore the standard library (go list std) as a workaround for "https://github.com/google/go-licenses/issues/244."
# The awk script converts the output of `go list std` (line separated modules)
# to the input that `--ignore` expects (comma separated modules).
STD_LIBS := $(shell go list std | awk 'NR > 1 { printf(",") } { printf("%s",$$0) } END { print "" }')

.PHONY: update-licenses
update-licenses: install-tools
	@echo "\n+++ Updating ${LICENSES_DIR} with licenses of current dependencies."
	rm -rf ${LICENSES_DIR}
# save dependency licenses for builds on supported OSes
	GOOS=linux go-licenses save \
		--save_path ${LICENSES_DIR} \
		--ignore "github.com/honeycombio/refinery" \
		--ignore ${STD_LIBS} \
		./cmd/refinery

.PHONY: verify-licenses
verify-licenses: update-licenses
	@echo "\n+++ Verifying ${LICENSES_DIR} directory is up to date."
	@if [ 0 -eq `git status --short -- ${LICENSES_DIR} | wc -l` ]; then \
		echo "✅  Passed: no dependency license changes detected."; \
	else \
		echo "⚠️  Licenses for dependencies appear to have changed since last recorded."; \
		echo ""; \
		git status --short -- ${LICENSES_DIR}; \
		echo "\nReview these licenses for compatibility and commit changes if acceptable."; \
		exit 1; \
	fi

.PHONY: smoke
smoke: dockerize local_image
	@echo ""
	@echo "+++ Smoking all the tests."
	@echo ""
	@echo ""
	@echo "+++ Spin up Refinery and Redis."
	@echo ""
	cd smoke-test && docker compose up --detach --wait-timeout 10
	@echo ""
	@echo "+++ Verify Refinery is ready within the timeout."
	@echo ""
	./dockerize -wait http://localhost:8080/ready -timeout 5s

.PHONY: unsmoke
unsmoke:
	@echo ""
	@echo "+++ Spinning down the smokers."
	@echo ""
	cd smoke-test && docker-compose down --volumes




