use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use prometheus::{Encoder, TextEncoder};
use std::future::Future;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

use crate::error::RelayError;
use crate::health::HealthState;
use crate::metrics_service;

#[derive(Clone)]
pub struct MetricsState {
    health: HealthState,
}

pub async fn start_metrics_server(
    bind_addr: SocketAddr,
    health: HealthState,
) -> Result<(), RelayError> {
    start_metrics_server_with_shutdown(bind_addr, health, std::future::pending::<()>()).await
}

pub async fn start_metrics_server_with_shutdown<F>(
    bind_addr: SocketAddr,
    health: HealthState,
    shutdown: F,
) -> Result<(), RelayError>
where
    F: Future<Output = ()> + Send + 'static,
{
    let app = create_metrics_app(health);

    let listener = TcpListener::bind(&bind_addr).await?;
    info!("Metrics server listening on http://{}", bind_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(|e| RelayError::Configuration(format!("Metrics server failed: {}", e)))?;
    Ok(())
}

fn create_metrics_app(health: HealthState) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .layer(ServiceBuilder::new().layer(CorsLayer::permissive()))
        .with_state(MetricsState { health })
}

async fn metrics_handler(State(_state): State<MetricsState>) -> Response {
    let registry = metrics_service::Metrics::registry();
    let metric_families = registry.gather();

    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();

    match encoder.encode(&metric_families, &mut buffer) {
        Ok(()) => match String::from_utf8(buffer) {
            Ok(metrics_output) => (
                StatusCode::OK,
                [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
                metrics_output,
            )
                .into_response(),
            Err(e) => {
                error!("Failed to convert metrics to UTF-8: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to encode metrics as UTF-8",
                )
                    .into_response()
            }
        },
        Err(e) => {
            error!("Failed to encode metrics: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to encode metrics",
            )
                .into_response()
        }
    }
}

async fn health_handler(State(state): State<MetricsState>) -> Response {
    if state.health.is_live() {
        (StatusCode::OK, "OK").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "Unhealthy").into_response()
    }
}

async fn ready_handler(State(state): State<MetricsState>) -> Response {
    if state.health.is_ready() {
        (StatusCode::OK, "Ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "Not Ready").into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_health_endpoint() {
        let app = create_metrics_app(crate::health::HealthState::new());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_ready_endpoint_not_ready_by_default() {
        // A freshly created HealthState is not ready until workers start and PGMQ is ready.
        let app = create_metrics_app(crate::health::HealthState::new());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_ready_endpoint_when_ready() {
        let health = crate::health::HealthState::new();
        health.set_started(true);
        health.set_pgmq_ready(true);
        let app = create_metrics_app(health);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_metrics_endpoint() {
        crate::metrics_service::init_metrics().unwrap();
        let app = create_metrics_app(crate::health::HealthState::new());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let content_type = response.headers().get("content-type").unwrap();
        assert!(content_type.to_str().unwrap().contains("text/plain"));
    }
}
