WETSOCKS_TEST_MIN_COV ?= 98.96

.PHONY: test
test:
	./scripts/gocov_test "${WETSOCKS_TEST_MIN_COV}"

.PHONY: lint
lint:
	@golangci-lint run --timeout 5m

.PHONY: qa
qa: test lint
