# pgmq-relay Makefile
# Provides build, test, and Docker operations for the pgmq-relay project

.PHONY: help build test clean docker-build docker-run docker-stop docker-compose-up docker-compose-down docker-logs fmt clippy check install dev prod

# Default target
help: ## Show this help message
	@echo "pgmq-relay - Message relay service for PGMQ to Kafka"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Build targets
build: ## Build the project in debug mode
	cargo build

build-release: ## Build the project in release mode
	cargo build --release

check: ## Check the project for compilation errors
	cargo check

fmt: ## Format the code
	cargo fmt

clippy: ## Run clippy lints
	cargo clippy -- -D warnings

# Test targets
test: ## Run all tests
	cargo test

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
	cargo clean
	docker system prune -f

# Docker targets
docker-build: ## Build production Docker image
	docker build -t pgmq-relay:latest .

docker-build-dev: ## Build development Docker image
	docker build -f Dockerfile.dev -t pgmq-relay:dev .

docker-run: ## Run the Docker container
	docker run --rm --name pgmq-relay \
		-p 9090:9090 \
		-e PGMQ_RELAY_CONNECTION_URL=${PGMQ_CONNECTION_URL:-postgres://postgres:password@localhost:5432/pgmq} \
		-e PGMQ_RELAY_KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092} \
		pgmq-relay:latest

docker-stop: ## Stop the running Docker container
	docker stop pgmq-relay || true

docker-shell: ## Get a shell in the Docker container
	docker run --rm -it --entrypoint /bin/sh pgmq-relay:latest

# Docker Compose targets
docker-compose-up: ## Start all services with docker-compose
	docker-compose up -d

docker-compose-down: ## Stop all services with docker-compose
	docker-compose down

docker-compose-logs: ## Show logs from docker-compose services
	docker-compose logs -f

docker-compose-restart: ## Restart docker-compose services
	docker-compose restart

docker-compose-build: ## Build and start services with docker-compose
	docker-compose up --build -d

# Development aliases for main docker-compose
dev-up: docker-compose-up ## Start development environment (alias for docker-compose-up)

dev-down: docker-compose-down ## Stop development environment (alias for docker-compose-down)

dev-logs: ## Show logs from pgmq-relay container
	docker-compose logs -f pgmq-relay

dev-build: docker-compose-build ## Build and start development environment (alias for docker-compose-build)

dev-restart: ## Restart pgmq-relay container
	docker-compose restart pgmq-relay

dev-shell: ## Get a shell in the pgmq-relay container
	docker-compose exec pgmq-relay /bin/bash

# Production targets
prod: build-release ## Build production binary

docker-prod: docker-build ## Build production Docker image

deploy-prep: test clippy fmt build-release ## Prepare for deployment (run all checks)

# Utility targets
logs: ## Show application logs (when running via docker-compose)
	docker-compose logs -f pgmq-relay

status: ## Show status of docker-compose services
	docker-compose ps

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
	cargo audit

# Documentation
docs: ## Generate and open documentation
	cargo doc --open

docs-build: ## Build documentation without opening
	cargo doc --no-deps

# Version management
version: ## Show current version
	@grep '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/'

# CI/CD helpers
ci-test: ## Run tests in CI environment
	cargo test --release

ci-build: ## Build in CI environment
	cargo build --release

ci-check: ## Full CI check pipeline
	cargo fmt --check
	cargo clippy -- -D warnings
	cargo test --release
	cargo build --release

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