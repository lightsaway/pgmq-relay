use pgmq_relay::transformer::MessageTransformer;
use pgmq_relay::config::MessageTransformation;
use serde_json::{json, Value};
use std::sync::Once;

static INIT: Once = Once::new();

fn setup_metrics() {
    INIT.call_once(|| {
        let _ = pgmq_relay::metrics_service::init_metrics();
    });
}

#[tokio::test]
async fn test_end_to_end_message_transformation() {
    setup_metrics();
    // Simulate a complete message transformation flow
    let message = json!({
        "user_id": "user123",
        "event": "purchase",
        "amount": 99.99,
        "product": {
            "id": "prod456",
            "name": "Widget"
        }
    });

    let headers = Some(json!({
        "x-pgmq-group": "purchase_queue_key",
        "correlation_id": "corr789",
        "source": "web_api"
    }));

    // Test 1: Passthrough with header key extraction
    let result = MessageTransformer::transform_message(
        &message,
        &headers,
        12345,
        &None,
        "purchase_queue",
        "x-pgmq-group",
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    assert_eq!(result.key, Some("purchase_queue_key".to_string()));
    assert_eq!(result.id, 12345);

    // Verify the message payload is correct
    let payload_value: Value = serde_json::from_slice(&result.payload).unwrap();
    assert_eq!(payload_value, message);

    // Test 2: JSON extraction with fallback key
    let transformation = Some(MessageTransformation::JsonExtract {
        field: "product".to_string(),
    });

    let result = MessageTransformer::transform_message(
        &message,
        &headers,
        12346,
        &transformation,
        "purchase_queue",
        "user_id", // Not in headers, should fallback to message
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    assert_eq!(result.key, Some("user123".to_string()));

    // Verify only the product object is in the payload
    let payload_value: Value = serde_json::from_slice(&result.payload).unwrap();
    assert_eq!(payload_value, message["product"]);

    // Test 3: Template transformation
    let template_transformation = Some(MessageTransformation::CustomTemplate {
        template: "User {user_id} purchased {product.name} for ${amount}".to_string(),
    });

    let result = MessageTransformer::transform_message(
        &message,
        &headers,
        12347,
        &template_transformation,
        "purchase_queue",
        "correlation_id",
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    assert_eq!(result.key, Some("corr789".to_string()));

    let payload_str = String::from_utf8(result.payload).unwrap();
    assert_eq!(payload_str, "User user123 purchased Widget for $99.99");
}

#[tokio::test]
async fn test_kafka_key_scenarios() {
    setup_metrics();
    let message = json!({
        "order_id": "order_123",
        "customer": {
            "id": "cust_456",
            "tier": "premium"
        }
    });

    // Scenario 1: FIFO header present
    let fifo_headers = Some(json!({
        "x-pgmq-group": "fifo_key_abc"
    }));

    let result = MessageTransformer::transform_message(
        &message,
        &fifo_headers,
        1,
        &None,
        "orders",
        "x-pgmq-group",
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    assert_eq!(result.key, Some("fifo_key_abc".to_string()));

    // Scenario 2: No FIFO header, fallback to message field
    let no_fifo_headers = Some(json!({
        "other_header": "value"
    }));

    let result = MessageTransformer::transform_message(
        &message,
        &no_fifo_headers,
        2,
        &None,
        "orders",
        "order_id",
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    assert_eq!(result.key, Some("order_123".to_string()));

    // Scenario 3: Nested customer ID from message
    let result = MessageTransformer::transform_message(
        &message,
        &no_fifo_headers,
        3,
        &None,
        "orders",
        "customer.id",
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    assert_eq!(result.key, Some("cust_456".to_string()));

    // Scenario 4: No matching key field anywhere
    let result = MessageTransformer::transform_message(
        &message,
        &no_fifo_headers,
        4,
        &None,
        "orders",
        "nonexistent_field",
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    assert_eq!(result.key, None);
}

#[tokio::test]
async fn test_transformation_error_handling() {
    setup_metrics();
    let message = json!({
        "valid_field": "value"
    });

    let headers = Some(json!({
        "header_field": "header_value"
    }));

    // Test JSON extract with missing field
    let bad_transformation = Some(MessageTransformation::JsonExtract {
        field: "missing_field".to_string(),
    });

    let result = MessageTransformer::transform_message(
        &message,
        &headers,
        1,
        &bad_transformation,
        "test_queue",
        "x-pgmq-group",
        |_, _, _| {}, // No-op metrics function for tests
    );

    assert!(result.is_err());
    match result.unwrap_err() {
        pgmq_relay::error::RelayError::MessageTransformation(msg) => {
            assert!(msg.contains("Field 'missing_field' not found"));
        }
        _ => panic!("Expected MessageTransformation error"),
    }
}

#[tokio::test]
async fn test_header_vs_message_priority() {
    setup_metrics();
    // Both headers and message have the same field name
    let message = json!({
        "priority": "message_priority",
        "user_id": "msg_user_123"
    });

    let headers = Some(json!({
        "priority": "header_priority",
        "user_id": "hdr_user_456"
    }));

    // Headers should win
    let result = MessageTransformer::transform_message(
        &message,
        &headers,
        1,
        &None,
        "test_queue",
        "priority",
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    assert_eq!(result.key, Some("header_priority".to_string()));

    let result = MessageTransformer::transform_message(
        &message,
        &headers,
        2,
        &None,
        "test_queue",
        "user_id",
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    assert_eq!(result.key, Some("hdr_user_456".to_string()));
}

#[tokio::test]
async fn test_realistic_fifo_scenario() {
    setup_metrics();
    // Simulate a realistic FIFO scenario with order processing
    let message = json!({
        "order_id": "ORD-2023-10-001",
        "customer_id": "CUST-12345",
        "items": [
            {"sku": "WIDGET-A", "quantity": 2},
            {"sku": "GADGET-B", "quantity": 1}
        ],
        "total_amount": 149.97,
        "timestamp": "2023-10-01T15:30:00Z"
    });

    // PGMQ FIFO headers
    let headers = Some(json!({
        "x-pgmq-group": "CUST-12345", // Partition by customer for FIFO ordering
        "message-type": "order-created",
        "version": "1.0"
    }));

    let result = MessageTransformer::transform_message(
        &message,
        &headers,
        98765,
        &None,
        "order_processing",
        "x-pgmq-group",
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    // Verify FIFO key is extracted correctly
    assert_eq!(result.key, Some("CUST-12345".to_string()));
    assert_eq!(result.id, 98765);

    // Verify headers are properly set
    assert_eq!(result.headers.get("pgmq_msg_id").unwrap(), "98765");
    assert_eq!(result.headers.get("pgmq_message_key").unwrap(), "CUST-12345");
    assert!(result.headers.contains_key("pgmq_relay_timestamp"));

    // Verify payload is the complete message
    let payload_value: Value = serde_json::from_slice(&result.payload).unwrap();
    assert_eq!(payload_value, message);
}

#[tokio::test]
async fn test_complex_template_transformation() {
    setup_metrics();
    let message = json!({
        "user": {
            "id": "user123",
            "name": "John Doe",
            "email": "john@example.com"
        },
        "action": "file_upload",
        "file": {
            "name": "document.pdf",
            "size": 2048576,
            "type": "application/pdf"
        },
        "timestamp": "2023-10-01T10:30:00Z"
    });

    let headers = Some(json!({
        "x-request-id": "req_789",
        "x-tenant-id": "tenant_abc"
    }));

    let transformation = Some(MessageTransformation::CustomTemplate {
        template: "Alert: User {user.name} ({user.email}) uploaded {file.name} ({file.type}) at {timestamp}".to_string(),
    });

    let result = MessageTransformer::transform_message(
        &message,
        &headers,
        555,
        &transformation,
        "audit_log",
        "x-tenant-id",
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    assert_eq!(result.key, Some("tenant_abc".to_string()));

    let payload_str = String::from_utf8(result.payload).unwrap();
    assert_eq!(
        payload_str,
        "Alert: User John Doe (john@example.com) uploaded document.pdf (application/pdf) at 2023-10-01T10:30:00Z"
    );
}
