-- Initialize PGMQ database and create test queues
-- This script runs when the PostgreSQL container starts for the first time

-- Create the PGMQ extension
CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;

-- Create test queues for different scenarios

-- 1. Standard queue for user events
SELECT pgmq.create('user_events');

-- 2. Queue for order processing
SELECT pgmq.create('order_processing');

-- 3. Queue for notifications
SELECT pgmq.create('notifications');

-- 4. Queue for system alerts
SELECT pgmq.create('alerts');

-- 5. Queue for financial transactions (critical data)
SELECT pgmq.create('financial_transactions');

-- 6. Queue for customer data changes
SELECT pgmq.create('customer_changes');

-- 7. Queue for analytics data
SELECT pgmq.create('analytics');

-- 8. Queue for logs
SELECT pgmq.create('logs');

-- Insert some test data to verify everything works

-- User events (standard queue)
SELECT pgmq.send('user_events', '{"user_id": "user123", "event": "login", "timestamp": "2024-01-01T10:00:00Z"}');
SELECT pgmq.send('user_events', '{"user_id": "user456", "event": "logout", "timestamp": "2024-01-01T10:05:00Z"}');
SELECT pgmq.send('user_events', '{"user_id": "user789", "event": "page_view", "page": "/dashboard", "timestamp": "2024-01-01T10:10:00Z"}');

-- Order processing (queue with headers)
SELECT pgmq.send('order_processing',
    '{"order_id": "ORD-001", "customer_id": "CUST-123", "amount": 99.99, "status": "pending"}'::jsonb,
    '{"x-pgmq-group": "CUST-123", "priority": "high"}'::jsonb
);
SELECT pgmq.send('order_processing',
    '{"order_id": "ORD-002", "customer_id": "CUST-123", "amount": 149.99, "status": "pending"}'::jsonb,
    '{"x-pgmq-group": "CUST-123", "priority": "normal"}'::jsonb
);
SELECT pgmq.send('order_processing',
    '{"order_id": "ORD-003", "customer_id": "CUST-456", "amount": 79.99, "status": "pending"}'::jsonb,
    '{"x-pgmq-group": "CUST-456", "priority": "normal"}'::jsonb
);

-- Notifications with payload structure
SELECT pgmq.send('notifications',
    '{"payload": {"user_id": "user123", "type": "email", "subject": "Welcome!", "body": "Welcome to our service"}, "metadata": {"priority": "normal"}}'::jsonb,
    '{"user_id": "user123"}'::jsonb
);
SELECT pgmq.send('notifications',
    '{"payload": {"user_id": "user456", "type": "sms", "message": "Your order is ready"}, "metadata": {"priority": "high"}}'::jsonb,
    '{"user_id": "user456"}'::jsonb
);

-- System alerts with template data
SELECT pgmq.send('alerts', '{"level": "ERROR", "message": "Database connection failed", "source": "app-server-1", "timestamp": "2024-01-01T10:15:00Z"}');
SELECT pgmq.send('alerts', '{"level": "WARNING", "message": "High memory usage detected", "source": "app-server-2", "timestamp": "2024-01-01T10:16:00Z"}');
SELECT pgmq.send('alerts', '{"level": "INFO", "message": "Deployment completed successfully", "source": "deployment-service", "timestamp": "2024-01-01T10:17:00Z"}');

-- Financial transactions (with transaction IDs)
SELECT pgmq.send('financial_transactions',
    '{"transaction_id": "TXN-001", "account_from": "ACC-123", "account_to": "ACC-456", "amount": 1000.00, "currency": "USD"}'::jsonb,
    '{"transaction_id": "TXN-001", "x-pgmq-group": "ACC-123"}'::jsonb
);
SELECT pgmq.send('financial_transactions',
    '{"transaction_id": "TXN-002", "account_from": "ACC-789", "account_to": "ACC-123", "amount": 500.00, "currency": "USD"}'::jsonb,
    '{"transaction_id": "TXN-002", "x-pgmq-group": "ACC-789"}'::jsonb
);

-- Customer data changes (by customer)
SELECT pgmq.send('customer_changes',
    '{"customer_id": "CUST-123", "field": "email", "old_value": "old@example.com", "new_value": "new@example.com", "timestamp": "2024-01-01T10:20:00Z"}'::jsonb,
    '{"customer_id": "CUST-123"}'::jsonb
);
SELECT pgmq.send('customer_changes',
    '{"customer_id": "CUST-456", "field": "phone", "old_value": "+1234567890", "new_value": "+1987654321", "timestamp": "2024-01-01T10:21:00Z"}'::jsonb,
    '{"customer_id": "CUST-456"}'::jsonb
);

-- Analytics data (user behavior tracking)
SELECT pgmq.send('analytics', '{"user_id": "user123", "action": "page_view", "page": "/products", "duration": 45, "timestamp": "2024-01-01T10:22:00Z"}');
SELECT pgmq.send('analytics', '{"user_id": "user456", "action": "search", "query": "laptop", "results": 42, "timestamp": "2024-01-01T10:23:00Z"}');
SELECT pgmq.send('analytics', '{"user_id": "user789", "action": "purchase", "product": "laptop-pro", "amount": 1299.99, "timestamp": "2024-01-01T10:24:00Z"}');

-- System logs (application events)
SELECT pgmq.send('logs', '{"level": "INFO", "service": "api-gateway", "message": "Request processed successfully", "request_id": "req-001", "timestamp": "2024-01-01T10:25:00Z"}');
SELECT pgmq.send('logs', '{"level": "DEBUG", "service": "user-service", "message": "User authentication successful", "user_id": "user123", "timestamp": "2024-01-01T10:26:00Z"}');
SELECT pgmq.send('logs', '{"level": "ERROR", "service": "payment-service", "message": "Payment processing failed", "error": "insufficient_funds", "timestamp": "2024-01-01T10:27:00Z"}');

-- Create a view to easily check queue status
CREATE OR REPLACE VIEW queue_summary AS
SELECT
    queue_name,
    COUNT(*) as total_messages,
    COUNT(*) FILTER (WHERE vt <= NOW()) as available_messages,
    COUNT(*) FILTER (WHERE vt > NOW()) as invisible_messages,
    MIN(enqueued_at) as oldest_message,
    MAX(enqueued_at) as newest_message
FROM (
    SELECT 'user_events' as queue_name, vt, enqueued_at FROM pgmq.q_user_events
    UNION ALL
    SELECT 'order_processing' as queue_name, vt, enqueued_at FROM pgmq.q_order_processing
    UNION ALL
    SELECT 'notifications' as queue_name, vt, enqueued_at FROM pgmq.q_notifications
    UNION ALL
    SELECT 'alerts' as queue_name, vt, enqueued_at FROM pgmq.q_alerts
    UNION ALL
    SELECT 'financial_transactions' as queue_name, vt, enqueued_at FROM pgmq.q_financial_transactions
    UNION ALL
    SELECT 'customer_changes' as queue_name, vt, enqueued_at FROM pgmq.q_customer_changes
    UNION ALL
    SELECT 'analytics' as queue_name, vt, enqueued_at FROM pgmq.q_analytics
    UNION ALL
    SELECT 'logs' as queue_name, vt, enqueued_at FROM pgmq.q_logs
) all_queues
GROUP BY queue_name;

-- Grant permissions for monitoring
GRANT SELECT ON queue_summary TO postgres;

-- Print summary
DO $$
BEGIN
    RAISE NOTICE 'PGMQ database initialization completed!';
    RAISE NOTICE 'Created queues: user_events, order_processing, notifications, alerts, financial_transactions, customer_changes, analytics, logs';
    RAISE NOTICE 'Inserted test messages in all queues';
    RAISE NOTICE 'All queues created successfully';
    RAISE NOTICE 'Use: SELECT * FROM queue_summary; to check queue status';
END $$;
