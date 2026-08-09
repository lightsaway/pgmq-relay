//! Performance-optimized logging macros and helpers for pgmq-relay.
//!
//! These macros provide conditional compilation for expensive debug operations
//! and structured logging with minimal runtime overhead.

/// Redact the userinfo (username:password) portion of connection URLs so they can be
/// logged safely. Handles comma-separated server lists (e.g. NATS cluster URLs) and
/// URLs without a scheme.
pub fn redact_url(url: &str) -> String {
    url.split(',')
        .map(redact_single_url)
        .collect::<Vec<_>>()
        .join(",")
}

fn redact_single_url(url: &str) -> String {
    let (prefix, rest) = match url.find("://") {
        Some(scheme_end) => url.split_at(scheme_end + 3),
        None => ("", url),
    };
    // Use the last '@' so passwords containing '@' are still fully redacted.
    match rest.rfind('@') {
        Some(at) => format!("{}***@{}", prefix, &rest[at + 1..]),
        None => url.to_string(),
    }
}

/// Macro for expensive debug operations that should only run in debug builds
/// or when explicitly enabled via feature flag.
#[macro_export]
macro_rules! debug_expensive {
    ($($arg:tt)*) => {
        #[cfg(any(debug_assertions, feature = "verbose-logging"))]
        tracing::debug!($($arg)*);
    };
}

/// Macro for per-message trace logging that should normally be disabled in production
#[macro_export]
macro_rules! trace_per_message {
    ($($arg:tt)*) => {
        #[cfg(any(debug_assertions, feature = "trace-messages"))]
        tracing::trace!($($arg)*);
    };
}

/// Macro for hot path trace logging that can be completely compiled out
#[macro_export]
macro_rules! trace_hot_path {
    ($($arg:tt)*) => {
        #[cfg(any(debug_assertions, feature = "trace-hot-paths"))]
        tracing::trace!($($arg)*);
    };
}

/// Structured error logging with context
#[macro_export]
macro_rules! error_with_context {
    ($error:expr, $context:expr, $($field:ident = $value:expr),* $(,)?) => {
        tracing::error!(
            error = %$error,
            context = $context,
            $($field = $value,)*
            "Operation failed"
        );
    };
}

/// Structured info logging for operations
#[macro_export]
macro_rules! info_operation {
    ($operation:expr, $($field:ident = $value:expr),* $(,)?) => {
        tracing::info!(
            operation = $operation,
            $($field = $value,)*
        );
    };
}

/// Performance span for tracking operation duration
#[macro_export]
macro_rules! perf_span {
    ($name:expr, $($field:ident = $value:expr),*) => {
        tracing::info_span!($name, $($field = $value,)*)
    };
}

#[cfg(test)]
mod tests {
    use super::redact_url;

    #[test]
    fn redacts_credentials_in_urls() {
        assert_eq!(
            redact_url("postgres://relay:s3cret@db.example.com:5432/app"),
            "postgres://***@db.example.com:5432/app"
        );
        assert_eq!(
            redact_url("amqp://guest:guest@localhost:5672/%2f"),
            "amqp://***@localhost:5672/%2f"
        );
    }

    #[test]
    fn redacts_passwords_containing_at_signs() {
        assert_eq!(
            redact_url("postgres://user:p@ss@host/db"),
            "postgres://***@host/db"
        );
    }

    #[test]
    fn leaves_urls_without_credentials_unchanged() {
        assert_eq!(redact_url("nats://localhost:4222"), "nats://localhost:4222");
        assert_eq!(redact_url("localhost:9092"), "localhost:9092");
    }

    #[test]
    fn redacts_each_url_in_a_server_list() {
        assert_eq!(
            redact_url("nats://a:b@host1:4222,nats://a:b@host2:4222"),
            "nats://***@host1:4222,nats://***@host2:4222"
        );
    }

    #[test]
    fn test_macros_compile() {
        // Test that all macros compile correctly
        debug_expensive!("test debug");
        trace_per_message!("test trace");
        trace_hot_path!("test hot path");
        error_with_context!("test error", "test context", field = "value");
        info_operation!("test op", field = "value");
        let _span = perf_span!("test_span", field = "value");
    }
}
