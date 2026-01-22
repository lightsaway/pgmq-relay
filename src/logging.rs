/// Performance-optimized logging macros for pgmq-relay
/// 
/// These macros provide conditional compilation for expensive debug operations
/// and structured logging with minimal runtime overhead.

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