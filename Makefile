GO_DIR     := ./go
INT_TAGS   := -tags integration
INT_TIMEOUT := -timeout 10m

.PHONY: test-unit test-api test-all

## Run fast unit tests (no containers required).
test-unit:
	cd $(GO_DIR) && go test ./internal/... -v -count=1

## Run API integration tests (spins up containers automatically).
## Requires Docker to be running. First run may take a few minutes to pull images and build.
test-api:
	cd $(GO_DIR) && go test ./tests/integration/... $(INT_TAGS) -v -count=1 $(INT_TIMEOUT)

## Run all tests: unit first, then integration.
test-all: test-unit test-api
