WETSOCKS_TEST_MIN_COV ?= 97

.PHONY: test
test:
	./scripts/gocov_test "${WETSOCKS_TEST_MIN_COV}"

.PHONY: lint
lint:
	@golangci-lint run --timeout 5m

.PHONY: qa
qa: test lint
