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

local_image: export KO_DOCKER_REPO=ko.local
local_image: export CIRCLE_TAG=$(shell git describe --always --match "v[0-9]*" --tags)
local_image: export CIRCLE_BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
local_image: export CIRCLE_SHA1=$(shell git rev-parse HEAD)
local_image: export CIRCLE_BUILD_NUM=''
local_image: export SOURCE_DATE_EPOCH=$(call __latest_modification_time)
#: build the release image locally, available as "ko.local/refinery:<commit>"
local_image:
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
# ensure the dockerize command is available
ko: ko_${KO_VERSION}.tar.gz
	tar xzvmf $< ko
	chmod u+x ./ko

ko_${KO_VERSION}.tar.gz:
	@echo
	@echo "+++ Retrieving dockerize tool for Redis readiness check."
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
	rm -rf test_results


.PHONY: install-tools
install-tools:
	go get -tool github.com/google/go-licenses/v2@v2.0.0-alpha.1
	go mod tidy

.PHONY: update-licenses
update-licenses: install-tools
	rm -rf LICENSES; \
	#: We ignore the standard library (go list std) as a workaround for \
	"https://github.com/google/go-licenses/issues/244." The awk script converts the output \
  of `go list std` (line separated modules) to the input that `--ignore` expects (comma separated modules).
	go tool go-licenses save --save_path LICENSES --ignore "github.com/honeycombio/refinery" \
		--ignore $(shell go list std | awk 'NR > 1 { printf(",") } { printf("%s",$$0) } END { print "" }') ./cmd/refinery;

.PHONY: verify-licenses
verify-licenses: install-tools
	go tool go-licenses save --save_path temp --ignore "github.com/honeycombio/refinery" \
		--ignore $(shell go list std | awk 'NR > 1 { printf(",") } { printf("%s",$$0) } END { print "" }') ./cmd/refinery; \
	chmod +r temp; \
    if diff temp LICENSES; then \
      echo "Passed"; \
      rm -rf temp; \
    else \
      echo "LICENSES directory must be updated. Run make update-licenses"; \
      rm -rf temp; \
      exit 1; \
    fi; \

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




