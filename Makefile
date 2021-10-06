.PHONY: test
#: run all tests
test: test_with_race test_all

.PHONY: test_with_race
#: run only tests tagged with potential race conditions
test_with_race:
	@echo
	@echo "+++ testing - race conditions?"
	@echo
	go test -tags race --race --timeout 60s -v ./...

.PHONY: test_all
#: run all tests, but with no race condition detection
test_all:
	@echo
	@echo "+++ testing - all the tests"
	@echo
	go test -tags all --timeout 60s -v ./...
