.PHONY: help proto-gen proto-lint build test run-local migrate-up migrate-down docker-build

BIN_DIR := bin
GEN_DIR := gen
PROTO_DIR := proto
GO_MODULE := github.com/gateway-fm/chain-indexer

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

proto-gen: ## Generate Go stubs from .proto files
	@which buf > /dev/null || (echo "buf not installed: https://buf.build/docs/installation"; exit 1)
	buf generate

proto-lint: ## Lint .proto files
	@which buf > /dev/null || (echo "buf not installed"; exit 1)
	buf lint

proto-breaking: ## Check for breaking proto changes vs main
	buf breaking --against '.git#branch=main'

build: ## Build the indexer binary
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/indexer ./cmd/indexer

test: ## Run unit tests
	go test ./...

test-integration: ## Run integration tests (requires docker)
	go test -tags=integration ./...

run-local: ## Run the indexer against a local docker-compose dev stack
	docker compose -f scripts/docker-compose.dev.yml up -d postgres anvil
	INDEXER_CONFIG=scripts/config.dev.yaml go run ./cmd/indexer

migrate-up: ## Apply pending migrations to the configured DB
	@echo "TODO: wire tern migrations in Phase 2"

migrate-down: ## Revert last migration (dev only)
	@echo "TODO: wire tern migrations in Phase 2"

docker-build: ## Build the indexer docker image
	docker build -t gatewayfm/chain-indexer:latest .

clean: ## Remove build artifacts
	rm -rf $(BIN_DIR) $(GEN_DIR)
