use clap::Parser;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tracing::{error, info};

mod broker;
mod brokers;
mod circuit_breaker;
mod config;
mod error;
mod logging;
mod metrics_server;
mod metrics_service;
mod pgmq_client;
mod relay;
mod transformer;
mod validator;

use config::Config;
use metrics_service::{MetricsService, PrometheusMetricsService, SystemMetric};
use relay::Relay;

#[derive(Parser)]
#[command(name = "pgmq-relay")]
#[command(about = "A relay service that forwards PGMQ messages to message brokers")]
struct Cli {
    #[arg(short, long, default_value = "config.toml")]
    config: String,

    #[arg(long, default_value = "30")]
    shutdown_timeout: u64,

    #[arg(long, help = "Validate configuration file and exit")]
    validate_config: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize structured logging based on environment
    let log_format = std::env::var("LOG_FORMAT").unwrap_or_else(|_| "text".to_string());
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "pgmq_relay=info".into());

    match log_format.as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(env_filter)
                .with_current_span(false) // Reduce performance overhead
                .with_span_list(false) // Reduce performance overhead
                .init();
        }
        _ => {
            tracing_subscriber::fmt().with_env_filter(env_filter).init();
        }
    }

    let cli = Cli::parse();

    info!("Starting PGMQ Relay");
    info!("Loading configuration from: {}", cli.config);

    let config = Config::from_file(&cli.config)?;

    // If validate_config flag is set, validate and exit
    if cli.validate_config {
        info!("✅ Configuration validation successful!");
        info!("📄 Config file: {}", cli.config);
        info!("🔧 Queues configured: {}", config.queues.len());
        info!("🔗 Brokers configured: {}", config.brokers.len());
        info!("📊 Metrics enabled: {}", config.metrics.enabled);

        // Additional validation with suggestions
        match config.validate_with_suggestions() {
            Ok(()) => {
                info!("🎉 All configuration checks passed!");
                return Ok(());
            }
            Err(e) => {
                error!("❌ Configuration validation failed: {}", e);
                std::process::exit(1);
            }
        }
    }

    info!("Configuration loaded successfully");

    let metrics_service: Arc<dyn MetricsService> = Arc::new(PrometheusMetricsService::new()?);
    info!("Metrics initialized");

    // Record startup
    metrics_service.record_system_metric(SystemMetric::Restart);

    let mut relay = Relay::new(config.clone(), Arc::clone(&metrics_service)).await?;

    if config.metrics.enabled {
        let metrics_addr = format!("{}:{}", config.metrics.bind_address, config.metrics.port)
            .parse()
            .map_err(|e| format!("Invalid metrics bind address: {}", e))?;

        tokio::spawn(async move {
            info!("Starting metrics server on {}", metrics_addr);
            if let Err(e) = metrics_server::start_metrics_server(metrics_addr).await {
                error!("Metrics server failed: {}", e);
            }
        });
    }

    relay.start().await?;

    // Start background task for system metrics
    let metrics_service_bg = Arc::clone(&metrics_service);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;

            // Record uptime tick (every 10 seconds)
            metrics_service_bg.record_system_metric(SystemMetric::UptimeTick);

            // Update memory usage if possible
            #[cfg(target_os = "linux")]
            if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
                for line in status.lines() {
                    if line.starts_with("VmRSS:") {
                        if let Some(kb_str) = line.split_whitespace().nth(1) {
                            if let Ok(kb) = kb_str.parse::<f64>() {
                                metrics_service_bg
                                    .record_system_metric(SystemMetric::MemoryUsage(kb * 1024.0));
                                // Convert KB to bytes
                            }
                        }
                        break;
                    }
                }
            }

            // For non-Linux systems, we can't easily get memory usage
            // so we'll set a placeholder value or use system-specific methods
            #[cfg(not(target_os = "linux"))]
            {
                // Use 0 as placeholder - in production you might want to use
                // platform-specific memory APIs
                metrics_service_bg.record_system_metric(SystemMetric::MemoryUsage(0.0));
            }
        }
    });

    info!("PGMQ Relay is running. Press Ctrl+C to stop.");

    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Received shutdown signal");
        }
        result = relay.wait_for_shutdown() => {
            if let Err(e) = result {
                error!("Relay error: {}", e);
                return Err(e.into());
            }
        }
    }

    let shutdown_timeout = Duration::from_secs(cli.shutdown_timeout);
    relay.shutdown(shutdown_timeout).await?;

    info!("PGMQ Relay stopped");
    Ok(())
}
