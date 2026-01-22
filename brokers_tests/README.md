# PGMQ Relay Integration Tests

Integration tests for PGMQ Relay that verify message relaying from PostgreSQL PGMQ queues to Kafka (Redpanda).

## Prerequisites

- Docker and Docker Compose
- Python 3.9 or higher
- [uv](https://github.com/astral-sh/uv) - Fast Python package installer

## Setup

1. **Start the Docker services:**

```bash
cd ../
docker-compose up -d
```

This will start:
- PostgreSQL with PGMQ extension (port 5432)
- Redpanda/Kafka (port 19092)
- RabbitMQ (ports 5672, 15672)
- NATS (ports 4222, 8222)
- PGMQ Relay service

2. **Wait for services to be healthy:**

```bash
# Check service health
docker-compose ps

# Wait for relay to start processing
docker-compose logs -f pgmq-relay
```

3. **Install test dependencies:**

```bash
cd brokers_tests/

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv pip install -r requirements.txt
```

## Running Tests

### Run the Kafka test:

```bash
# Run Kafka integration test
pytest -v -s -k test_kafka_redpanda_relay

# Or run all tests (currently only Kafka is configured)
pytest -v -s
```

All services should show "healthy" status before running tests.

### Python dependencies issues

```bash
# Clear cache and reinstall
rm -rf .venv
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```
