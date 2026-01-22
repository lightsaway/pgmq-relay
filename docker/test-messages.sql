-- Test message insertion scripts for PGMQ Relay
-- Run these after the system is up to add more test data

-- More user events
SELECT pgmq.send('user_events', '{"user_id": "user001", "event": "signup", "source": "web", "timestamp": "2024-01-01T11:00:00Z"}');
SELECT pgmq.send('user_events', '{"user_id": "user002", "event": "purchase", "product_id": "PROD-123", "amount": 29.99, "timestamp": "2024-01-01T11:05:00Z"}');
SELECT pgmq.send('user_events', '{"user_id": "user003", "event": "profile_update", "field": "preferences", "timestamp": "2024-01-01T11:10:00Z"}');

-- FIFO order processing (same customer - should be processed in order)
SELECT pgmq.send('order_processing',
    '{"order_id": "ORD-004", "customer_id": "CUST-789", "amount": 199.99, "status": "pending", "items": [{"sku": "SKU-001", "qty": 2}]}',
    '{"x-pgmq-group": "CUST-789", "order_sequence": "1"}'
);
SELECT pgmq.send('order_processing',
    '{"order_id": "ORD-005", "customer_id": "CUST-789", "amount": 49.99, "status": "pending", "items": [{"sku": "SKU-002", "qty": 1}]}',
    '{"x-pgmq-group": "CUST-789", "order_sequence": "2"}'
);
SELECT pgmq.send('order_processing',
    '{"order_id": "ORD-006", "customer_id": "CUST-789", "amount": 299.99, "status": "pending", "items": [{"sku": "SKU-003", "qty": 1}]}',
    '{"x-pgmq-group": "CUST-789", "order_sequence": "3"}'
);

-- Different customers (can be processed in parallel)
SELECT pgmq.send('order_processing',
    '{"order_id": "ORD-007", "customer_id": "CUST-999", "amount": 99.99, "status": "pending"}',
    '{"x-pgmq-group": "CUST-999"}'
);

-- Notifications with different payload structures
SELECT pgmq.send('notifications',
    '{"payload": {"user_id": "user001", "type": "push", "title": "New Message", "body": "You have a new message"}, "metadata": {"priority": "normal", "retry_count": 0}}',
    '{"user_id": "user001"}'
);
SELECT pgmq.send('notifications',
    '{"payload": {"user_id": "user002", "type": "email", "subject": "Order Confirmation", "body": "Your order has been confirmed"}, "metadata": {"priority": "high", "scheduled_at": "2024-01-01T12:00:00Z"}}',
    '{"user_id": "user002"}'
);

-- System alerts with various severities
SELECT pgmq.send('alerts', '{"level": "CRITICAL", "message": "Service unavailable", "source": "load-balancer", "timestamp": "2024-01-01T11:30:00Z", "affected_users": 1500}');
SELECT pgmq.send('alerts', '{"level": "WARNING", "message": "SSL certificate expires in 30 days", "source": "cert-monitor", "timestamp": "2024-01-01T11:31:00Z", "cert_domain": "api.example.com"}');
SELECT pgmq.send('alerts', '{"level": "INFO", "message": "Auto-scaling triggered", "source": "k8s-cluster", "timestamp": "2024-01-01T11:32:00Z", "new_replicas": 5}');

-- Financial transactions with different account patterns
SELECT pgmq.send('financial_transactions',
    '{"transaction_id": "TXN-003", "account_from": "ACC-111", "account_to": "ACC-222", "amount": 2500.00, "currency": "USD", "type": "transfer", "reference": "Payment for services"}',
    '{"transaction_id": "TXN-003", "x-pgmq-group": "ACC-111"}'
);
SELECT pgmq.send('financial_transactions',
    '{"transaction_id": "TXN-004", "account_from": "ACC-333", "account_to": "ACC-444", "amount": 750.50, "currency": "EUR", "type": "payment", "reference": "Invoice #12345"}',
    '{"transaction_id": "TXN-004", "x-pgmq-group": "ACC-333"}'
);

-- Customer changes for different customers
SELECT pgmq.send('customer_changes',
    '{"customer_id": "CUST-789", "field": "address", "old_value": "123 Old St", "new_value": "456 New Ave", "timestamp": "2024-01-01T11:40:00Z", "change_reason": "customer_request"}',
    '{"customer_id": "CUST-789"}'
);
SELECT pgmq.send('customer_changes',
    '{"customer_id": "CUST-999", "field": "subscription_tier", "old_value": "basic", "new_value": "premium", "timestamp": "2024-01-01T11:41:00Z", "change_reason": "upgrade"}',
    '{"customer_id": "CUST-999"}'
);

-- Check queue status after adding messages
SELECT 'Queue Status After Test Messages:' as status;
SELECT * FROM queue_summary ORDER BY queue_name;
