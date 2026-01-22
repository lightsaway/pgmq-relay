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

#[test]
fn test_passthrough_transformation() {
    setup_metrics();
    let message = json!({"user_id": 123, "action": "login"});
    let headers = None;

    let result = MessageTransformer::transform_message(
        &message,
        &headers,
        1,
        &None,
        "test_queue",
        "user_id",
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    assert_eq!(result.id, 1);
    assert_eq!(result.key, Some("123".to_string()));
    assert!(result.headers.contains_key("pgmq_msg_id"));
    assert_eq!(result.headers["pgmq_msg_id"], "1");
    assert!(result.headers.contains_key("pgmq_queue_name"));
    assert_eq!(result.headers["pgmq_queue_name"], "test_queue");

    // Verify payload contains original message
    let payload_json: Value = serde_json::from_slice(&result.payload).unwrap();
    assert_eq!(payload_json, message);
}

#[test]
fn test_json_extract_transformation() {
    setup_metrics();
    let message = json!({"user_id": 123, "data": {"action": "login", "timestamp": "2023-01-01"}});
    let transformation = Some(MessageTransformation::JsonExtract {
        field: "data".to_string(),
    });

    let result = MessageTransformer::transform_message(
        &message,
        &None,
        2,
        &transformation,
        "test_queue",
        "user_id",
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    assert_eq!(result.id, 2);
    assert_eq!(result.key, Some("123".to_string()));

    // Verify payload contains only extracted field
    let payload_json: Value = serde_json::from_slice(&result.payload).unwrap();
    assert_eq!(payload_json, json!({"action": "login", "timestamp": "2023-01-01"}));
}

#[test]
fn test_custom_template_transformation() {
    setup_metrics();
    let message = json!({"user_id": 123, "action": "login"});
    let transformation = Some(MessageTransformation::CustomTemplate {
        template: "User {user_id} performed {action}".to_string(),
    });

    let result = MessageTransformer::transform_message(
        &message,
        &None,
        3,
        &transformation,
        "test_queue",
        "user_id",
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    assert_eq!(result.id, 3);
    assert_eq!(result.key, Some("123".to_string()));

    // Verify payload contains rendered template
    let payload_str = String::from_utf8(result.payload).unwrap();
    assert_eq!(payload_str, "User 123 performed login");
}

#[test]
fn test_json_extract_missing_field() {
    setup_metrics();
    let message = json!({"user_id": 123});
    let transformation = Some(MessageTransformation::JsonExtract {
        field: "missing_field".to_string(),
    });

    let result = MessageTransformer::transform_message(
        &message,
        &None,
        4,
        &transformation,
        "test_queue",
        "user_id",
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

#[test]
fn test_header_preservation() {
    setup_metrics();
    let message = json!({"user_id": 123});
    let headers = Some(json!({
        "correlation_id": "abc-123",
        "x-pgmq-group": "user-456",
        "priority": 1
    }));

    let result = MessageTransformer::transform_message(
        &message,
        &headers,
        5,
        &None,
        "test_queue",
        "x-pgmq-group",
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    assert_eq!(result.key, Some("user-456".to_string()));

    // Check that original headers are preserved with prefix
    assert_eq!(result.headers["pgmq_header_correlation_id"], "abc-123");
    assert_eq!(result.headers["pgmq_header_x-pgmq-group"], "user-456");
    assert_eq!(result.headers["pgmq_header_priority"], "1");

    // Check relay headers
    assert_eq!(result.headers["pgmq_msg_id"], "5");
    assert_eq!(result.headers["pgmq_queue_name"], "test_queue");
    assert_eq!(result.headers["pgmq_message_key"], "user-456");
    assert!(result.headers.contains_key("pgmq_relay_timestamp"));
}

#[test]
fn test_nested_key_extraction() {
    setup_metrics();
    let message = json!({
        "user_id": 123,
        "metadata": {
            "tenant_id": "tenant-789"
        }
    });

    let result = MessageTransformer::transform_message(
        &message,
        &None,
        6,
        &None,
        "test_queue",
        "metadata.tenant_id",
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    assert_eq!(result.key, Some("tenant-789".to_string()));
    assert_eq!(result.headers["pgmq_message_key"], "tenant-789");
}

#[test]
fn test_key_extraction_fallback_to_message_id() {
    setup_metrics();
    let message = json!({"other_field": "value"});

    let result = MessageTransformer::transform_message(
        &message,
        &None,
        7,
        &None,
        "test_queue",
        "nonexistent_field",
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    // Should fall back to None since field doesn't exist
    assert_eq!(result.key, None);
    assert!(!result.headers.contains_key("pgmq_message_key"));
}

#[test]
fn test_header_priority_headers_over_message() {
    setup_metrics();
    let message = json!({"user_id": "message-123"});
    let headers = Some(json!({"user_id": "header-456"}));

    let result = MessageTransformer::transform_message(
        &message,
        &headers,
        8,
        &None,
        "test_queue",
        "user_id",
        |_, _, _| {}, // No-op metrics function for tests
    ).unwrap();

    // Should prefer header value over message value
    assert_eq!(result.key, Some("header-456".to_string()));
    assert_eq!(result.headers["pgmq_header_user_id"], "header-456");
}

#[test]
fn test_value_to_string_conversion() {
    assert_eq!(MessageTransformer::value_to_string(&json!("string")), "string");
    assert_eq!(MessageTransformer::value_to_string(&json!(123)), "123");
    assert_eq!(MessageTransformer::value_to_string(&json!(true)), "true");
    assert_eq!(MessageTransformer::value_to_string(&json!(false)), "false");
    assert_eq!(MessageTransformer::value_to_string(&json!(null)), "null");

    let array_result = MessageTransformer::value_to_string(&json!([1, 2, 3]));
    assert!(array_result.contains("1") && array_result.contains("2") && array_result.contains("3"));
}

#[test]
fn test_template_rendering() {
    let message = json!({
        "user_id": 123,
        "action": "login",
        "timestamp": "2023-01-01T12:00:00Z"
    });

    let template = "User {user_id} performed {action} at {timestamp}";
    let result = MessageTransformer::render_template(template, &message).unwrap();

    assert_eq!(result, "User 123 performed login at 2023-01-01T12:00:00Z");
}

#[test]
fn test_template_with_missing_fields() {
    let message = json!({"user_id": 123});
    let template = "User {user_id} performed {missing_action}";
    let result = MessageTransformer::render_template(template, &message).unwrap();

    // Missing fields should remain as placeholders
    assert_eq!(result, "User 123 performed {missing_action}");
}
