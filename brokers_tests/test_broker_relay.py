"""
Integration tests for PGMQ Relay with multiple brokers.

Tests sending messages to PGMQ queues via SQL and consuming them from
RabbitMQ, NATS, and Kafka (Redpanda) brokers.
"""

import asyncio
import json
import time
import uuid
from typing import Any, Dict

import psycopg2
import pytest
from kafka import KafkaConsumer
from nats.aio.client import Client as NATS
from pika import BlockingConnection, ConnectionParameters, PlainCredentials


class TestBrokerRelay:
    """Test suite for PGMQ relay to multiple broker types."""

    @pytest.fixture(scope="class")
    def db_connection(self):
        """Create a PostgreSQL connection to PGMQ via Docker."""
        import subprocess
        import time

        # Use Docker exec to run psql commands to avoid local PostgreSQL conflicts
        # This is a workaround - we'll use subprocess for SQL commands
        class DockerPsqlConnection:
            def execute(self, sql):
                """Execute SQL via docker exec."""
                cmd = [
                    "docker",
                    "exec",
                    "pgmq-postgres",
                    "psql",
                    "-U",
                    "postgres",
                    "-d",
                    "pgmq_relay",
                    "-t",
                    "-c",
                    sql,
                ]
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                return result.stdout.strip()

            def close(self):
                pass

        yield DockerPsqlConnection()

    @pytest.fixture(scope="class")
    def setup_queues(self, db_connection):
        """Ensure PGMQ queues exist."""
        # Create queues if they don't exist
        queues = [
            "user_events",
            "notifications",
            "system_logs",
            "analytics_events",
            "telemetry",
        ]
        for queue in queues:
            try:
                result = db_connection.execute(f"SELECT pgmq.create('{queue}');")
                print(f"✓ Created queue '{queue}'")
            except Exception as e:
                if "already exists" not in str(e):
                    print(f"⚠ Queue '{queue}': {e}")

        yield queues

    def send_message_to_pgmq(
        self, db_connection, queue_name: str, message: Dict[str, Any]
    ) -> int:
        """Send a message to a PGMQ queue and return message ID."""
        message_json = json.dumps(message).replace("'", "''")  # Escape single quotes
        sql = f"SELECT pgmq.send('{queue_name}', '{message_json}'::jsonb);"
        result = db_connection.execute(sql)
        # Parse the result to get the message ID
        msg_id = int(result.strip())
        return msg_id

    def test_kafka_redpanda_relay(self, db_connection, setup_queues):
        """Test relaying messages from PGMQ to Kafka (Redpanda)."""
        # Generate unique message
        message_id = str(uuid.uuid4())
        message = {
            "user_id": "user_123",
            "event_type": "login",
            "message_id": message_id,
            "timestamp": time.time(),
        }

        # Send to PGMQ
        print(f"\n✓ Sending message to PGMQ queue 'user_events': {message}")
        pgmq_msg_id = self.send_message_to_pgmq(db_connection, "user_events", message)
        print(f"✓ Message sent to PGMQ with ID: {pgmq_msg_id}")

        # Wait for relay to process
        print("⏳ Waiting for relay to process message...")
        time.sleep(5)

        # Consume from Kafka
        print("📥 Attempting to consume from Kafka (Redpanda)...")
        consumer = KafkaConsumer(
            "events.users",
            bootstrap_servers=["localhost:19092"],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=10000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        found = False
        for kafka_message in consumer:
            print(f"📨 Received from Kafka: {kafka_message.value}")
            if kafka_message.value.get("message_id") == message_id:
                found = True
                assert kafka_message.value["user_id"] == message["user_id"]
                assert kafka_message.value["event_type"] == message["event_type"]
                print(f"✅ Successfully verified message from Kafka!")
                break

        consumer.close()
        assert found, f"Message with ID {message_id} not found in Kafka"

    def test_rabbitmq_relay(self, db_connection, setup_queues):
        """Test relaying messages from PGMQ to RabbitMQ."""
        # Generate unique message
        # Set up RabbitMQ consumer FIRST (before sending message to PGMQ)
        # This ensures the queue exists to receive the message when relay publishes it
        print("📥 Setting up RabbitMQ consumer...")
        credentials = PlainCredentials("admin", "admin")
        connection = BlockingConnection(
            ConnectionParameters(host="localhost", port=5672, credentials=credentials)
        )
        channel = connection.channel()

        # Declare exchange and queue (matching what relay should create)
        # The relay publishes to exchange "pgmq.relay" with routing key "notifications.email"
        exchange_name = "pgmq.relay"
        routing_key = "notifications.email"
        queue_name = f"test_queue_{uuid.uuid4().hex[:8]}"

        channel.exchange_declare(
            exchange=exchange_name, exchange_type="topic", durable=True
        )
        channel.queue_declare(queue=queue_name, exclusive=True)
        # Bind to catch all messages (or use routing_key for specific messages)
        channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key="#")
        print(f"✓ RabbitMQ queue '{queue_name}' ready to receive messages")

        # Now send message to PGMQ
        message_id = str(uuid.uuid4())
        message = {
            "notification_id": "notif_456",
            "type": "email",
            "recipient": "user@example.com",
            "message_id": message_id,
            "timestamp": time.time(),
        }

        print(f"\n✓ Sending message to PGMQ queue 'notifications': {message}")
        pgmq_msg_id = self.send_message_to_pgmq(db_connection, "notifications", message)
        print(f"✓ Message sent to PGMQ with ID: {pgmq_msg_id}")

        # Wait for relay to process
        print("⏳ Waiting for relay to process message...")
        time.sleep(5)

        found = False
        timeout = time.time() + 10  # 10 second timeout

        while time.time() < timeout:
            method_frame, properties, body = channel.basic_get(
                queue=queue_name, auto_ack=True
            )

            if method_frame:
                rabbitmq_message = json.loads(body.decode("utf-8"))
                print(f"📨 Received from RabbitMQ: {rabbitmq_message}")

                if rabbitmq_message.get("message_id") == message_id:
                    found = True
                    assert (
                        rabbitmq_message["notification_id"]
                        == message["notification_id"]
                    )
                    assert rabbitmq_message["type"] == message["type"]
                    print(f"✅ Successfully verified message from RabbitMQ!")
                    break
            else:
                time.sleep(0.5)

        connection.close()
        assert found, f"Message with ID {message_id} not found in RabbitMQ"

    @pytest.mark.asyncio
    async def test_nats_relay(self, db_connection, setup_queues):
        """Test relaying messages from PGMQ to NATS."""
        # Set up NATS consumer FIRST (before sending message to PGMQ)
        print("📥 Setting up NATS consumer...")
        nc = NATS()
        await nc.connect("nats://localhost:4222")

        messages_received = []

        async def message_handler(msg):
            nats_message = json.loads(msg.data.decode())
            print(f"📨 Received from NATS: {nats_message}")
            messages_received.append(nats_message)

        # Subscribe to the subject
        sub = await nc.subscribe("logs.system", cb=message_handler)
        print("✓ NATS subscriber ready")

        # Now send message to PGMQ
        message_id = str(uuid.uuid4())
        message = {
            "log_id": "log_789",
            "level": "INFO",
            "service": "api-gateway",
            "message_id": message_id,
            "timestamp": time.time(),
        }

        print(f"\n✓ Sending message to PGMQ queue 'system_logs': {message}")
        pgmq_msg_id = self.send_message_to_pgmq(db_connection, "system_logs", message)
        print(f"✓ Message sent to PGMQ with ID: {pgmq_msg_id}")

        # Wait for relay to process
        print("⏳ Waiting for relay to process message...")
        await asyncio.sleep(5)

        # Wait for messages
        timeout = 10
        start_time = time.time()
        while time.time() - start_time < timeout:
            await asyncio.sleep(0.5)
            for nats_message in messages_received:
                if nats_message.get("message_id") == message_id:
                    found = True
                    assert nats_message["log_id"] == message["log_id"]
                    assert nats_message["level"] == message["level"]
                    print(f"✅ Successfully verified message from NATS!")
                    break
            if found:
                break

        await sub.unsubscribe()
        await nc.close()

        assert found, f"Message with ID {message_id} not found in NATS"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
