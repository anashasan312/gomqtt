# GoMQTT — developer entry points.
#
# Every command a contributor needs lives here, so nobody has to reconstruct a
# twelve-flag `go test` invocation from memory or from CI logs.

SHELL        := /bin/bash
BINARY       := gomqtt
BUILD_DIR    := ./bin
CMD_DIR      := ./cmd
CONFIG_PATH  ?= ./config/config.local.yaml
COMPOSE      := docker compose -f deployment/docker-compose.yml

# Version metadata is stamped in at link time, so a running process can be
# traced back to the exact commit that produced it.
VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT       ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_TIME   := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS      := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildTime=$(BUILD_TIME)

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

.PHONY: deps
deps: ## Download dependencies and generate go.sum (run this first after cloning)
	@echo "==> resolving dependencies"
	@go mod tidy
	@go mod download
	@echo "==> go.sum is ready"

.PHONY: build
build: ## Build the broker and the load generator into ./bin
	@echo "==> building $(BINARY) $(VERSION)"
	@mkdir -p $(BUILD_DIR)
	@go build -trimpath -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY) $(CMD_DIR)
	@go build -trimpath -o $(BUILD_DIR)/loadgen $(CMD_DIR)/loadgen
	@echo "==> $(BUILD_DIR)/$(BINARY), $(BUILD_DIR)/loadgen"

.PHONY: run
run: ## Run the broker against the local config
	@GOMQTT_CONFIG_PATH=$(CONFIG_PATH) go run $(CMD_DIR)

.PHONY: clean
clean: ## Remove build artefacts and coverage output
	@rm -rf $(BUILD_DIR) coverage.out coverage.html
	@echo "==> cleaned"

# ---------------------------------------------------------------------------
# Code generation
# ---------------------------------------------------------------------------

.PHONY: wire
wire: ## Regenerate the dependency injection container
	@echo "==> running wire"
	@go run github.com/google/wire/cmd/wire ./pkg/di
	@echo "==> pkg/di/wire_gen.go regenerated"

# ---------------------------------------------------------------------------
# Quality
# ---------------------------------------------------------------------------

.PHONY: fmt
fmt: ## Format all Go source
	@gofmt -s -w .
	@echo "==> formatted"

.PHONY: vet
vet: ## Run go vet, including the build-tagged suites
	@go vet ./...
	@go vet -tags=interop ./test/interop/

.PHONY: lint
lint: ## Run golangci-lint (install: make tools)
	@golangci-lint run ./...

.PHONY: tidy
tidy: ## Tidy and verify the module graph
	@go mod tidy
	@go mod verify

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

.PHONY: test
test: ## Run unit and end-to-end tests (no external dependencies required)
	@go test -race -count=1 ./...

.PHONY: test-interop
test-interop: ## Run the interop suite against Eclipse Mosquitto (needs mosquitto installed)
	@command -v mosquitto >/dev/null 2>&1 || { \
		echo "mosquitto is not installed; install it with e.g. 'apt-get install mosquitto mosquitto-clients'"; \
		exit 1; }
	@go test -tags=interop -count=1 -v ./test/interop/

.PHONY: fuzz
fuzz: ## Fuzz the packet decoder (FUZZTIME=60s to change the budget)
	@go test -run=XXX -fuzz=FuzzDecode -fuzztime=$${FUZZTIME:-30s} ./pkg/infrastructure/mqtt/packet/

.PHONY: cover
cover: ## Run tests with coverage and write the HTML report
	@go test -race -count=1 -coverprofile=coverage.out -covermode=atomic ./...
	@go tool cover -func=coverage.out | tail -1
	@go tool cover -html=coverage.out -o coverage.html
	@echo "==> coverage.html"

.PHONY: bench
bench: ## Benchmark against Mosquitto and write bench/RESULTS.md
	@./scripts/bench.sh

.PHONY: bench-trie
bench-trie: ## Micro-benchmark the subscription trie
	@go test -bench=BenchmarkIndex -benchmem -run=XXX ./pkg/infrastructure/persistence/memory/

.PHONY: validate
validate: fmt tidy wire vet test ## Full pre-push check
	@echo "==> validate passed"

# ---------------------------------------------------------------------------
# Local stack
# ---------------------------------------------------------------------------

.PHONY: docker-build
docker-build: ## Build the Docker image
	@docker build -f deployment/Dockerfile -t gomqtt:latest .

.PHONY: stack-up
stack-up: ## Start the broker, Prometheus and Grafana
	@$(COMPOSE) up -d --build
	@echo ""
	@echo "  mqtt        127.0.0.1:1883"
	@echo "  admin api   http://localhost:8080/api/v1/stats"
	@echo "  metrics     http://localhost:8080/metrics"
	@echo "  prometheus  http://localhost:9090"
	@echo "  grafana     http://localhost:3000  (admin/admin)"
	@echo ""

.PHONY: stack-down
stack-down: ## Stop the stack
	@$(COMPOSE) down

.PHONY: logs
logs: ## Tail the broker logs
	@$(COMPOSE) logs -f gomqtt

# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

.PHONY: demo
demo: ## Exercise a running broker with mosquitto_pub/mosquitto_sub
	@./scripts/demo.sh

# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

.PHONY: tools
tools: ## Install the development tools
	@go install github.com/google/wire/cmd/wire@latest
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
