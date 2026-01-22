# pgmq-relay Build & Development Guide

This document provides comprehensive instructions for building, testing, and deploying pgmq-relay.

## Quick Start

```bash
# Show all available commands
make help

# Run comprehensive test suite
make test-all

# Start development environment
make dev-up

# View application logs
make dev-logs

# Build production Docker image
make docker-build

# Stop environment
make dev-down
```

## Prerequisites

### Local Development
- Rust 1.75+ with Cargo
- Docker and Docker Compose
- Make
- curl (for health checks)

### Optional Tools
- `cargo-watch` for development (auto-installed by setup)
- `cargo-audit` for security scanning (auto-installed by setup)

## Development Workflow

### Initial Setup
```bash
# Setup development environment
make setup-dev

# Create environment file template
make env-example
```

### Development Commands
```bash
# Format, lint, and test (quick check)
make quick-test

# Watch for changes and rebuild
make watch

# Run comprehensive test suite
make test-all

# Start development environment with hot reload
make dev-up
make dev-logs  # View logs
make dev-shell # Get shell in container
make dev-down  # Stop development environment
```

### Testing
```bash
# Run all tests
make test

# Run specific test suites
make test-unit           # Unit tests only
make test-integration    # Integration tests only
make test-transformer    # Transformer tests only
make test-circuit-breaker # Circuit breaker tests only

# Run tests with verbose output
make test-verbose
```

### Code Quality
```bash
# Format code
make fmt

# Run linter
make clippy

# Security audit
make security-audit

# Full validation pipeline
make full-check
```

## Docker Development

### Development Environment
The docker-compose.yml is configured for development and includes:
- PostgreSQL with PGMQ extension
- Redpanda (Kafka-compatible)
- Redpanda Console (Kafka UI)
- pgmq-relay application with development configuration

```bash
# Start development environment
make dev-up              # or make docker-compose-up

# Access services:
# - Application metrics: http://localhost:9090/metrics
# - Redpanda Console: http://localhost:8080

# View application logs
make dev-logs            # pgmq-relay logs only
make docker-compose-logs # all services logs

# Get shell in development container
make dev-shell

# Stop development environment
make dev-down            # or make docker-compose-down
```

### Production Deployment
For production, use the same docker-compose.yml but with:
- Production environment variables
- Proper resource limits
- External volumes for data persistence
- Load balancer configuration

```bash
# Build production Docker image
make docker-build

# Start environment (same compose file)
make docker-compose-up

# View logs
make docker-compose-logs

# Stop environment
make docker-compose-down
```

## Build Targets

### Local Builds
```bash
make build          # Debug build
make build-release  # Release build (optimized)
make clean         # Clean build artifacts
```

### Docker Builds
```bash
make docker-build     # Production Docker image
make docker-build-dev # Development Docker image
make docker-buildx    # Multi-platform build (AMD64, ARM64)
```

## Environment Variables

pgmq-relay supports extensive configuration via environment variables:

### Database Configuration
- `PGMQ_CONNECTION_URL` - PostgreSQL connection string
- `PGMQ_RELAY_DEFAULT_MAX_CONNECTIONS` - Max DB connections (default: 10)

### Kafka Configuration
- `PGMQ_RELAY_KAFKA_BOOTSTRAP_SERVERS` - Kafka servers (default: localhost:9092)
- `PGMQ_RELAY_KAFKA_SECURITY_PROTOCOL` - Security protocol (SASL_SSL, etc.)
- `PGMQ_RELAY_KAFKA_SASL_MECHANISM` - SASL mechanism
- `PGMQ_RELAY_KAFKA_SASL_USERNAME` - SASL username
- `PGMQ_RELAY_KAFKA_SASL_PASSWORD` - SASL password

### Processing Configuration
- `PGMQ_RELAY_DEFAULT_BATCH_SIZE` - Message batch size (default: 10)
- `PGMQ_RELAY_DEFAULT_POLL_INTERVAL` - Queue poll interval (default: 250ms)
- `PGMQ_RELAY_DEFAULT_PARALLELISM` - Worker parallelism (default: 1)

### Circuit Breaker Configuration
- `PGMQ_RELAY_CIRCUIT_BREAKER_FAILURE_THRESHOLD` - Failure threshold (default: 5)
- `PGMQ_RELAY_CIRCUIT_BREAKER_RECOVERY_TIMEOUT` - Recovery timeout (default: 30s)
- `PGMQ_RELAY_CIRCUIT_BREAKER_MAX_RETRIES` - Max retries (default: 5)
- `PGMQ_RELAY_CIRCUIT_BREAKER_INITIAL_DELAY` - Initial retry delay (default: 100ms)
- `PGMQ_RELAY_CIRCUIT_BREAKER_MAX_DELAY` - Max retry delay (default: 10s)

Create `.env` file for local development:
```bash
make env-example  # Creates .env.example template
cp .env.example .env
# Edit .env with your values
```

## Monitoring & Debugging

### Metrics
```bash
# View metrics (requires running service)
make metrics

# Check service health
make health

# View service status
make status
```

### Logs
```bash
# Development environment logs
make dev-logs

# Production environment logs
make docker-compose-logs

# Application-only logs
make logs
```

### Debugging
```bash
# Get shell in development container
make dev-shell

# Get shell in production container
make docker-shell

# Show configuration debug info
make debug-config

# Show environment variables
make debug-env
```

## CI/CD Pipeline

### Local CI Simulation
```bash
# Run full CI check pipeline
make ci-check

# Individual CI steps
make ci-test   # Tests in CI environment
make ci-build  # Build in CI environment
```

### Deployment Preparation
```bash
# Prepare for deployment (runs all checks)
make deploy-prep

# Build production artifacts
make prod
```

## Performance & Security

### Benchmarking
```bash
make bench  # Run Rust benchmarks
```

### Security
```bash
make security-audit  # Security vulnerability scan
```

## Troubleshooting

### Common Issues

1. **Port conflicts**: Ensure ports 5432, 8080, 8081, 9090, 9091, 19092 are available
2. **Docker space**: Clean up with `make clean`
3. **Dependency issues**: Re-run `make setup-dev`

### Debug Commands
```bash
make debug-env     # Show environment variables
make debug-config  # Show configuration debug info
make status       # Show Docker services status
```

### Container Access
```bash
# Development container shell
make dev-shell

# Production container shell  
make docker-shell

# View container logs
make dev-logs          # Development
make docker-compose-logs  # Production
```

## Architecture

The build system provides:

- **Multi-stage Docker builds** for minimal production images
- **Development containers** with hot reload and debugging tools
- **Comprehensive testing** with security audits and linting
- **Environment-specific configurations** for development and production
- **Monitoring integration** with Prometheus and metrics endpoints
- **Circuit breaker patterns** for resilient error handling
- **Exactly-once delivery** semantics with transaction support

## File Structure

```
pgmq-relay/
├── Makefile                    # Main build commands
├── Dockerfile                  # Production Docker image
├── Dockerfile.dev             # Development Docker image
├── docker-compose.yml         # Development/Production environment
├── scripts/
│   └── test-all.sh           # Comprehensive test script
├── .env.example              # Environment variables template
└── BUILD.md                  # This file
```