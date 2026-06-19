# pgmq-relay Makefile

CARGO ?= cargo
MDBOOK ?= mdbook
LYCHEE ?= lychee
DOCKER ?= docker
COMPOSE ?= docker compose
TARGET_ARG = $(if $(TARGET),--target $(TARGET),)

.PHONY: help build build-release check fmt fmt-check clippy test clean \
	docker-build docker-run docker-stop docker-compose-up docker-compose-down \
	docker-compose-logs site site-check site-serve security-audit \
	ci-quality ci-test ci-docs ci-audit ci-check

# Default target
help: ## Show this help message
	@echo "pgmq-relay - Message relay service for PGMQ to Kafka"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Build targets
build: ## Build the project in debug mode
	$(CARGO) build --locked

build-release: ## Build the project in release mode
	$(CARGO) build --release --locked $(TARGET_ARG)

check: ## Check the project for compilation errors
	$(CARGO) check --locked --all-targets --all-features

fmt: ## Format the code
	$(CARGO) fmt --all

fmt-check: ## Check formatting without changing files
	$(CARGO) fmt --all --check

clippy: ## Run clippy lints
	$(CARGO) clippy --locked --all-targets --all-features -- -D warnings

# Test targets
test: ## Run all tests
	$(CARGO) test --locked --all-targets --all-features

test-unit: ## Run unit tests only
	cargo test --lib

test-integration: ## Run integration tests only
	cargo test --test integration_tests

test-transformer: ## Run transformer tests only
	cargo test --test transformer_tests

test-circuit-breaker: ## Run circuit breaker tests only
	cargo test circuit_breaker::tests

test-verbose: ## Run tests with verbose output
	cargo test -- --nocapture

# Development targets
dev: ## Run the project in development mode
	cargo run

watch: ## Watch for changes and rebuild/restart
	cargo watch -x run

install: ## Install the binary
	cargo install --path .

clean: ## Clean build artifacts
	$(CARGO) clean
	$(MDBOOK) clean

# Docker targets
docker-build: ## Build production Docker image
	$(DOCKER) build -t pgmq-relay:local .

docker-build-dev: ## Build development Docker image
	docker build -f Dockerfile.dev -t pgmq-relay:dev .

docker-run: ## Run the Docker container
	$(DOCKER) run --rm --name pgmq-relay \
		-p 9090:9090 \
		-v "$(CURDIR)/config.toml:/etc/pgmq-relay/config.toml:ro" \
		pgmq-relay:local

docker-stop: ## Stop the running Docker container
	$(DOCKER) stop pgmq-relay || true

docker-shell: ## Get a shell in the Docker container
	$(DOCKER) run --rm -it --entrypoint /bin/sh pgmq-relay:local

# Docker Compose targets
docker-compose-up: ## Start all services with docker-compose
	$(COMPOSE) up -d

docker-compose-down: ## Stop all services with docker-compose
	$(COMPOSE) down

docker-compose-logs: ## Show logs from docker-compose services
	$(COMPOSE) logs -f

docker-compose-restart: ## Restart docker-compose services
	$(COMPOSE) restart

docker-compose-build: ## Build and start services with docker-compose
	$(COMPOSE) up --build -d

# Development aliases for main docker-compose
dev-up: docker-compose-up ## Start development environment (alias for docker-compose-up)

dev-down: docker-compose-down ## Stop development environment (alias for docker-compose-down)

dev-logs: ## Show logs from pgmq-relay container
	$(COMPOSE) logs -f pgmq-relay

dev-build: docker-compose-build ## Build and start development environment (alias for docker-compose-build)

dev-restart: ## Restart pgmq-relay container
	$(COMPOSE) restart pgmq-relay

dev-shell: ## Get a shell in the pgmq-relay container
	$(COMPOSE) exec pgmq-relay /bin/sh

# Production targets
prod: build-release ## Build production binary

docker-prod: docker-build ## Build production Docker image

deploy-prep: test clippy fmt build-release ## Prepare for deployment (run all checks)

# Utility targets
logs: ## Show application logs (when running via docker-compose)
	$(COMPOSE) logs -f pgmq-relay

status: ## Show status of docker-compose services
	$(COMPOSE) ps

env-example: ## Create example environment file
	@echo "# pgmq-relay environment variables" > .env.example
	@echo "PGMQ_CONNECTION_URL=postgres://postgres:password@localhost:5432/pgmq" >> .env.example
	@echo "KAFKA_BOOTSTRAP_SERVERS=localhost:9092" >> .env.example
	@echo "PGMQ_RELAY_DEFAULT_BATCH_SIZE=10" >> .env.example
	@echo "PGMQ_RELAY_DEFAULT_POLL_INTERVAL=250ms" >> .env.example
	@echo "PGMQ_RELAY_CIRCUIT_BREAKER_FAILURE_THRESHOLD=5" >> .env.example
	@echo "PGMQ_RELAY_CIRCUIT_BREAKER_RECOVERY_TIMEOUT=30s" >> .env.example
	@echo "Created .env.example with default environment variables"

# Quick development workflow
quick-test: fmt clippy test ## Quick development test (format, lint, test)

full-check: ## Full validation pipeline using comprehensive test script
	./scripts/test-all.sh

test-all: ## Run comprehensive test suite
	./scripts/test-all.sh

# Metrics and monitoring
metrics: ## Show metrics endpoint (requires service to be running)
	curl -s http://localhost:9090/metrics | head -20

health: ## Check health of running service
	curl -s http://localhost:9090/health || echo "Service not running or health endpoint not available"

# Database operations (requires PGMQ_CONNECTION_URL)
db-migrate: ## Run database migrations (if any)
	@echo "Database migrations would go here"

db-reset: ## Reset database (careful!)
	@echo "This would reset the database - implement as needed"

# Performance and benchmarking
bench: ## Run benchmarks
	cargo bench

profile: ## Run with profiling (requires additional setup)
	@echo "Profiling setup needed - consider using cargo-profiler or similar tools"

# Multi-platform builds
docker-buildx: ## Build multi-platform Docker images
	docker buildx build --platform linux/amd64,linux/arm64 -t pgmq-relay:latest .

# Security scanning
security-audit: ## Run security audit
	cargo-audit audit

# Documentation
docs: ## Generate and open documentation
	cargo doc --open

docs-build: ## Build documentation without opening
	cargo doc --no-deps

site: ## Build the GitHub Pages static site with mdBook
	$(MDBOOK) build

site-check: site ## Build the site and validate generated links
	$(LYCHEE) --offline --no-progress --exclude-path 'book/404.html' book

site-serve: ## Serve the mdBook site locally
	$(MDBOOK) serve --open

# Version management
version: ## Show current version
	@grep '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/'

release-validate: ## Verify TAG matches the Cargo package version
	@test -n "$(TAG)" || { echo "TAG is required, for example TAG=v0.1.0"; exit 1; }
	@test "$(TAG)" = "v$$(grep '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')" || { \
		echo "Tag $(TAG) does not match Cargo.toml version v$$(grep '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')"; \
		exit 1; \
	}

# CI/CD helpers
ci-quality: fmt-check clippy ## Run formatting and lint checks used by CI

ci-test: test ## Run the test command used by CI

ci-docs: site-check ## Build and link-check the documentation used by CI

ci-audit: security-audit ## Run the dependency audit used by CI

ci-check: ci-quality ci-test ci-docs ## Run deterministic local CI checks

# Environment setup
setup-dev: ## Setup development environment
	rustup component add rustfmt clippy
	cargo install cargo-watch cargo-audit
	@echo "Development environment setup complete"

# Troubleshooting
debug-config: ## Show configuration debug info
	cargo run -- --help

debug-env: ## Show relevant environment variables
	@echo "Current environment variables:"
	@env | grep -E "(PGMQ|KAFKA|RUST)" || echo "No relevant environment variables set"
