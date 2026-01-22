use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use prometheus::{Encoder, TextEncoder};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

use crate::metrics_service;

#[derive(Clone)]
pub struct MetricsState;

pub async fn start_metrics_server(bind_addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let app = create_metrics_app();

    let listener = TcpListener::bind(&bind_addr).await?;
    info!("Metrics server listening on http://{}", bind_addr);

    axum::serve(listener, app).await?;
    Ok(())
}

fn create_metrics_app() -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .layer(
            ServiceBuilder::new()
                .layer(CorsLayer::permissive())
        )
        .with_state(MetricsState)
}

async fn metrics_handler(State(_state): State<MetricsState>) -> Response {
    let registry = metrics_service::Metrics::registry();
    let metric_families = registry.gather();
    
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    
    match encoder.encode(&metric_families, &mut buffer) {
        Ok(()) => {
            match String::from_utf8(buffer) {
                Ok(metrics_output) => {
                    (
                        StatusCode::OK,
                        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
                        metrics_output,
                    ).into_response()
                }
                Err(e) => {
                    error!("Failed to convert metrics to UTF-8: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Failed to encode metrics as UTF-8",
                    ).into_response()
                }
            }
        }
        Err(e) => {
            error!("Failed to encode metrics: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to encode metrics",
            ).into_response()
        }
    }
}

async fn health_handler(State(_state): State<MetricsState>) -> Response {
    (StatusCode::OK, "OK").into_response()
}

async fn ready_handler(State(_state): State<MetricsState>) -> Response {
    (StatusCode::OK, "Ready").into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_health_endpoint() {
        let app = create_metrics_app();

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
    async fn test_ready_endpoint() {
        let app = create_metrics_app();

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
        let app = create_metrics_app();

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