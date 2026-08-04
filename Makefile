ifeq ($(strip $(WETSOCKS_TEST_MIN_COV)),)
WETSOCKS_TEST_MIN_COV = 97
endif

.PHONY: test
test:
	./scripts/gocov_test "${WETSOCKS_TEST_MIN_COV}"

.PHONY: lint
lint:
	@golangci-lint run --timeout 5m

.PHONY: qa
qa: test lint
