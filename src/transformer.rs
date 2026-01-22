use std::collections::HashMap;
use serde_json::Value;
use crate::broker::RelayMessage;
use crate::config::MessageTransformation;
use crate::error::RelayError;

/// Message transformer responsible for converting PGMQ messages to RelayMessages
pub struct MessageTransformer;

impl MessageTransformer {
    /// Transform a PGMQ message into a RelayMessage for broker delivery
    pub fn transform_message<F>(
        message: &Value,
        headers: &Option<Value>,
        msg_id: i64,
        transformation: &Option<MessageTransformation>,
        queue_name: &str,
        key_field: &str,
        record_metrics: F,
    ) -> Result<RelayMessage, RelayError>
    where
        F: FnOnce(&str, &str, bool),
    {
        let transformation_type = Self::get_transformation_type(transformation);

        // Apply the message transformation
        let payload = Self::apply_transformation(message, transformation)?;

        // Record transformation metrics using the provided function
        record_metrics(queue_name, transformation_type, true);

        // Extract message key from headers first, then fall back to message body
        let message_key = Self::extract_message_key_from_headers_or_message(headers, message, key_field);

        // Build headers with original PGMQ headers and relay metadata
        let relay_headers = Self::build_relay_headers(headers, msg_id, queue_name, &message_key, key_field);

        crate::trace_per_message!(
            message_id = msg_id,
            payload_bytes = payload.len(),
            header_count = relay_headers.len(),
            "Message transformed"
        );

        Ok(RelayMessage {
            id: msg_id,
            payload,
            headers: relay_headers,
            key: message_key,
        })
    }

    /// Transform a PGMQ message using the global metrics system (for backward compatibility)
    pub fn transform_message_with_global_metrics(
        message: &Value,
        headers: &Option<Value>,
        msg_id: i64,
        transformation: &Option<MessageTransformation>,
        queue_name: &str,
        key_field: &str,
    ) -> Result<RelayMessage, RelayError> {
        Self::transform_message(
            message,
            headers,
            msg_id,
            transformation,
            queue_name,
            key_field,
            |queue, trans_type, success| {
                crate::metrics_service::record_message_transformation(queue, trans_type, success);
            },
        )
    }

    /// Get the transformation type string for metrics
    fn get_transformation_type(transformation: &Option<MessageTransformation>) -> &'static str {
        match transformation.as_ref().unwrap_or(&MessageTransformation::Passthrough) {
            MessageTransformation::Passthrough => "passthrough",
            MessageTransformation::JsonExtract { .. } => "json_extract",
            MessageTransformation::CustomTemplate { .. } => "custom_template",
        }
    }

    /// Apply the configured message transformation
    fn apply_transformation(
        message: &Value,
        transformation: &Option<MessageTransformation>,
    ) -> Result<Vec<u8>, RelayError> {
        match transformation.as_ref().unwrap_or(&MessageTransformation::Passthrough) {
            MessageTransformation::Passthrough => {
                serde_json::to_vec(message)
                    .map_err(|e| RelayError::MessageTransformation(e.to_string()))
            }
            MessageTransformation::JsonExtract { field } => {
                let extracted = message.get(field)
                    .ok_or_else(|| RelayError::MessageTransformation(
                        format!("Field '{}' not found in message", field)
                    ))?;
                serde_json::to_vec(extracted)
                    .map_err(|e| RelayError::MessageTransformation(e.to_string()))
            }
            MessageTransformation::CustomTemplate { template } => {
                let rendered = Self::render_template(template, message)
                    .map_err(RelayError::MessageTransformation)?;
                Ok(rendered.into_bytes())
            }
        }
    }

    /// Build relay headers including original PGMQ headers and metadata
    fn build_relay_headers(
        headers: &Option<Value>,
        msg_id: i64,
        queue_name: &str,
        message_key: &Option<String>,
        key_field: &str,
    ) -> HashMap<String, String> {
        let mut relay_headers = HashMap::new();

        // First, copy any original PGMQ headers to preserve them
        if let Some(headers_obj) = headers {
            if let Value::Object(header_map) = headers_obj {
                for (key, value) in header_map {
                    // Convert header values to strings, prefixed to indicate they're from PGMQ
                    let header_value = Self::value_to_string(value);
                    relay_headers.insert(format!("pgmq_header_{}", key), header_value);
                }
            }
        }

        // Add relay-specific headers
        relay_headers.insert("pgmq_msg_id".to_string(), msg_id.to_string());
        relay_headers.insert("pgmq_relay_timestamp".to_string(),
                             chrono::Utc::now().to_rfc3339());
        relay_headers.insert("pgmq_queue_name".to_string(), queue_name.to_string());

        // If we have a key, also add it as a header for debugging
        if let Some(ref key) = message_key {
            relay_headers.insert("pgmq_message_key".to_string(), key.clone());
            tracing::info!("Extracted message key '{}' from field '{}' for message {}", key, key_field, msg_id);
        } else {
            tracing::info!("No message key found in field '{}' for message {}, using message ID as fallback", key_field, msg_id);
        }

        relay_headers
    }

    /// Convert a JSON value to a string representation
    pub fn value_to_string(value: &Value) -> String {
        match value {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => serde_json::to_string(value).unwrap_or_else(|_| "unknown".to_string()),
        }
    }

    /// Render a template string by substituting placeholders with message values
    pub fn render_template(template: &str, message: &Value) -> Result<String, String> {
        let mut result = template.to_string();

        // Find all placeholders in the template and collect them
        let mut placeholders = Vec::new();
        let mut start = 0;

        while let Some(open_pos) = result[start..].find('{') {
            let open_pos = start + open_pos;
            if let Some(close_pos) = result[open_pos..].find('}') {
                let close_pos = open_pos + close_pos;
                let field_path = &result[open_pos + 1..close_pos];
                placeholders.push(field_path.to_string());
                start = close_pos + 1;
            } else {
                break;
            }
        }

        // Replace placeholders with actual values
        for field_path in placeholders {
            if let Some(extracted_value) = Self::extract_nested_value(message, &field_path) {
                let placeholder = format!("{{{}}}", field_path);
                let replacement = Self::value_to_string(&extracted_value);
                result = result.replace(&placeholder, &replacement);
            }
        }

        Ok(result)
    }

    /// Extract a nested value from a JSON object using a dot-separated path
    fn extract_nested_value(json_obj: &Value, field_path: &str) -> Option<Value> {
        if field_path.contains('.') {
            // Handle nested field paths like "user.name" or "product.id"
            let parts: Vec<&str> = field_path.split('.').collect();
            let mut current = json_obj;

            for part in parts {
                match current.get(part) {
                    Some(val) => current = val,
                    None => return None,
                }
            }
            Some(current.clone())
        } else {
            // Simple field lookup
            json_obj.get(field_path).cloned()
        }
    }

    /// Extract message key from headers first, then fall back to message body
    fn extract_message_key_from_headers_or_message(
        headers: &Option<Value>,
        message: &Value,
        key_field: &str,
    ) -> Option<String> {
        // First, try to extract from headers
        if let Some(headers_obj) = headers {
            if let Some(key) = Self::extract_message_key(headers_obj, key_field) {
                return Some(key);
            }
        }

        // Fall back to extracting from message body
        Self::extract_message_key(message, key_field)
    }

    /// Extract a message key from a JSON object using a field path
    fn extract_message_key(json_obj: &Value, key_field: &str) -> Option<String> {
        // Try to extract the key field from the message
        let key_value = if key_field.contains('.') {
            // Handle nested field paths like "headers.x-pgmq-group"
            let parts: Vec<&str> = key_field.split('.').collect();
            let mut current = json_obj;

            for part in parts {
                match current.get(part) {
                    Some(val) => current = val,
                    None => return None,
                }
            }
            current
        } else {
            // Simple field lookup
            json_obj.get(key_field)?
        };

        // Convert the value to a string, but return None for null values
        match key_value {
            Value::Null => None,
            _ => Some(Self::value_to_string(key_value)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MessageTransformation;
    use crate::error::RelayError;
    use serde_json::{json, Value};

    fn create_test_message() -> Value {
        json!({
            "user_id": "user123",
            "event_type": "order_created",
            "order": {
                "id": "ord_456",
                "amount": 99.99,
                "currency": "USD"
            },
            "metadata": {
                "tenant_id": "tenant_789",
                "source": "web_app"
            },
            "timestamp": "2023-10-01T12:00:00Z"
        })
    }

    fn create_test_headers() -> Value {
        json!({
            "x-pgmq-group": "fifo_key_123",
            "correlation_id": "corr_456",
            "trace_id": "trace_789",
            "content-type": "application/json"
        })
    }

    #[test]
    fn test_passthrough_transformation() {
        let message = create_test_message();
        let headers = Some(create_test_headers());

        let result = MessageTransformer::transform_message(
            &message,
            &headers,
            123,
            &None, // No transformation = passthrough
            "test_queue",
            "x-pgmq-group",
            |_, _, _| {}, // No-op metrics function for tests
        ).unwrap();

        // Verify the payload is the original message serialized
        let expected_payload = serde_json::to_vec(&message).unwrap();
        assert_eq!(result.payload, expected_payload);

        // Verify headers are set
        assert_eq!(result.headers.get("pgmq_msg_id").unwrap(), "123");
        assert!(result.headers.contains_key("pgmq_relay_timestamp"));
        assert_eq!(result.headers.get("pgmq_message_key").unwrap(), "fifo_key_123");

        // Verify key extraction
        assert_eq!(result.key, Some("fifo_key_123".to_string()));
        assert_eq!(result.id, 123);
    }

    #[test]
    fn test_json_extract_transformation() {
        let message = create_test_message();
        let headers = Some(create_test_headers());

        let transformation = Some(MessageTransformation::JsonExtract {
            field: "order".to_string(),
        });

        let result = MessageTransformer::transform_message(
            &message,
            &headers,
            456,
            &transformation,
            "test_queue",
            "correlation_id",
            |_, _, _| {}, // No-op metrics function for tests
        ).unwrap();

        // Verify the payload is just the extracted order object
        let expected_order = message.get("order").unwrap();
        let expected_payload = serde_json::to_vec(expected_order).unwrap();
        assert_eq!(result.payload, expected_payload);

        // Verify key extraction from headers
        assert_eq!(result.key, Some("corr_456".to_string()));
    }

    #[test]
    fn test_json_extract_nonexistent_field() {
        let message = create_test_message();
        let headers = Some(create_test_headers());

        let transformation = Some(MessageTransformation::JsonExtract {
            field: "nonexistent_field".to_string(),
        });

        let result = MessageTransformer::transform_message(
            &message,
            &headers,
            789,
            &transformation,
            "test_queue",
            "x-pgmq-group",
            |_, _, _| {}, // No-op metrics function for tests
        );

        // Should return an error
        assert!(result.is_err());
        match result.unwrap_err() {
            RelayError::MessageTransformation(msg) => {
                assert!(msg.contains("Field 'nonexistent_field' not found"));
            }
            _ => panic!("Expected MessageTransformation error"),
        }
    }

    #[test]
    fn test_custom_template_transformation() {
        let message = create_test_message();
        let headers = Some(create_test_headers());

        let transformation = Some(MessageTransformation::CustomTemplate {
            template: "User {user_id} created order {order.id} for {order.amount} {order.currency}".to_string(),
        });

        let result = MessageTransformer::transform_message(
            &message,
            &headers,
            101,
            &transformation,
            "test_queue",
            "trace_id",
            |_, _, _| {}, // No-op metrics function for tests
        ).unwrap();

        let payload_str = String::from_utf8(result.payload).unwrap();
        assert_eq!(payload_str, "User user123 created order ord_456 for 99.99 USD");

        // Verify key extraction from headers
        assert_eq!(result.key, Some("trace_789".to_string()));
    }

    #[test]
    fn test_key_extraction_from_headers() {
        let message = create_test_message();
        let headers = Some(create_test_headers());

        let result = MessageTransformer::transform_message(
            &message,
            &headers,
            111,
            &None,
            "test_queue",
            "x-pgmq-group",
            |_, _, _| {}, // No-op metrics function for tests
        ).unwrap();

        assert_eq!(result.key, Some("fifo_key_123".to_string()));
    }

    #[test]
    fn test_key_extraction_from_message_body() {
        let message = create_test_message();
        let headers = None; // No headers

        let result = MessageTransformer::transform_message(
            &message,
            &headers,
            222,
            &None,
            "test_queue",
            "user_id",
            |_, _, _| {}, // No-op metrics function for tests
        ).unwrap();

        assert_eq!(result.key, Some("user123".to_string()));
    }

    #[test]
    fn test_key_extraction_headers_fallback_to_message() {
        let message = create_test_message();
        let headers = Some(create_test_headers());

        // Try to extract a key that's not in headers but is in message
        let result = MessageTransformer::transform_message(
            &message,
            &headers,
            333,
            &None,
            "test_queue",
            "user_id", // Not in headers, but in message
            |_, _, _| {}, // No-op metrics function for tests
        ).unwrap();

        assert_eq!(result.key, Some("user123".to_string()));
    }

    #[test]
    fn test_nested_key_extraction() {
        let message = create_test_message();
        let headers = Some(create_test_headers());

        let result = MessageTransformer::transform_message(
            &message,
            &headers,
            444,
            &None,
            "test_queue",
            "metadata.tenant_id",
            |_, _, _| {}, // No-op metrics function for tests
        ).unwrap();

        assert_eq!(result.key, Some("tenant_789".to_string()));
    }

    #[test]
    fn test_nested_key_extraction_from_order() {
        let message = create_test_message();
        let headers = Some(create_test_headers());

        let result = MessageTransformer::transform_message(
            &message,
            &headers,
            555,
            &None,
            "test_queue",
            "order.id",
            |_, _, _| {}, // No-op metrics function for tests
        ).unwrap();

        assert_eq!(result.key, Some("ord_456".to_string()));
    }

    #[test]
    fn test_key_extraction_fallback_to_none() {
        let message = create_test_message();
        let headers = Some(create_test_headers());

        let result = MessageTransformer::transform_message(
            &message,
            &headers,
            666,
            &None,
            "test_queue",
            "nonexistent.field",
            |_, _, _| {}, // No-op metrics function for tests
        ).unwrap();

        // Should fall back to None (message ID will be used by Kafka client)
        assert_eq!(result.key, None);
        assert!(!result.headers.contains_key("pgmq_message_key"));
    }

    #[test]
    fn test_key_extraction_different_value_types() {
        let message = json!({
            "string_field": "string_value",
            "number_field": 42,
            "bool_field": true,
            "null_field": null,
            "array_field": [1, 2, 3],
            "object_field": {"nested": "value"}
        });

        let headers = None;

        // Test string value
        let result = MessageTransformer::transform_message(&message, &headers, 1, &None, "test", "string_field", |_, _, _| {}).unwrap();
        assert_eq!(result.key, Some("string_value".to_string()));

        // Test number value
        let result = MessageTransformer::transform_message(&message, &headers, 2, &None, "test", "number_field", |_, _, _| {}).unwrap();
        assert_eq!(result.key, Some("42".to_string()));

        // Test boolean value
        let result = MessageTransformer::transform_message(&message, &headers, 3, &None, "test", "bool_field", |_, _, _| {}).unwrap();
        assert_eq!(result.key, Some("true".to_string()));

        // Test null value (should return None)
        let result = MessageTransformer::transform_message(&message, &headers, 4, &None, "test", "null_field", |_, _, _| {}).unwrap();
        assert_eq!(result.key, None);

        // Test array value (should serialize as JSON)
        let result = MessageTransformer::transform_message(&message, &headers, 5, &None, "test", "array_field", |_, _, _| {}).unwrap();
        assert_eq!(result.key, Some("[1,2,3]".to_string()));
    }

    #[test]
    fn test_empty_headers() {
        let message = create_test_message();
        let headers = Some(json!({})); // Empty headers object

        let result = MessageTransformer::transform_message(
            &message,
            &headers,
            777,
            &None,
            "test_queue",
            "user_id", // Should fallback to message body
            |_, _, _| {}, // No-op metrics function for tests
        ).unwrap();

        assert_eq!(result.key, Some("user123".to_string()));
    }

    #[test]
    fn test_no_headers() {
        let message = create_test_message();
        let headers = None;

        let result = MessageTransformer::transform_message(
            &message,
            &headers,
            888,
            &None,
            "test_queue",
            "user_id",
            |_, _, _| {}, // No-op metrics function for tests
        ).unwrap();

        assert_eq!(result.key, Some("user123".to_string()));
    }

    #[test]
    fn test_complex_template_with_missing_fields() {
        let message = json!({
            "user": "john",
            "action": "login"
        });

        let transformation = Some(MessageTransformation::CustomTemplate {
            template: "User {user} performed {action} on {missing_field}".to_string(),
        });

        let result = MessageTransformer::transform_message(
            &message,
            &None,
            999,
            &transformation,
            "test_queue",
            "user",
            |_, _, _| {}, // No-op metrics function for tests
        ).unwrap();

        let payload_str = String::from_utf8(result.payload).unwrap();
        // Missing fields should remain as placeholders
        assert_eq!(payload_str, "User john performed login on {missing_field}");
    }

    #[test]
    fn test_metrics_recording() {
        // This test verifies that metrics are recorded during transformation
        // Note: In a real test, you'd want to mock the metrics or check counters
        let message = create_test_message();
        let headers = Some(create_test_headers());

        // Test successful transformation
        let result = MessageTransformer::transform_message(
            &message,
            &headers,
            1001,
            &None,
            "metrics_test_queue",
            "x-pgmq-group",
            |_, _, _| {}, // No-op metrics function for tests
        );

        assert!(result.is_ok());
        // In a real implementation, you'd verify that
        // metrics::record_message_transformation was called with success=true
    }

    #[test]
    fn test_header_priority_over_message() {
        let message = json!({
            "x-pgmq-group": "message_key",
            "user_id": "user_from_message"
        });

        let headers = Some(json!({
            "x-pgmq-group": "header_key",
            "user_id": "user_from_header"
        }));

        // Headers should have priority
        let result = MessageTransformer::transform_message(
            &message,
            &headers,
            1002,
            &None,
            "test_queue",
            "x-pgmq-group",
            |_, _, _| {}, // No-op metrics function for tests
        ).unwrap();

        assert_eq!(result.key, Some("header_key".to_string()));

        // Test with user_id too
        let result = MessageTransformer::transform_message(
            &message,
            &headers,
            1003,
            &None,
            "test_queue",
            "user_id",
            |_, _, _| {}, // No-op metrics function for tests
        ).unwrap();

        assert_eq!(result.key, Some("user_from_header".to_string()));
    }
}
