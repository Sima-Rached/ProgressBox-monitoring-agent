use utoipa::OpenApi;
use common::{AckResponse, AlertsEnvelope, BrokersEnvelope, ConnectionsResponse, ErrorResponse, HistoryEnvelope, MetricsEnvelope, ThroughputResponse};

/// Get live metrics for all brokers
#[utoipa::path(
    get,
    path = "/api/v1/metrics",
    responses((status = 200, description = "Current broker metrics", body = MetricsEnvelope)),
    security(("api_key" = []))
)]
#[allow(dead_code)]
async fn docs_get_metrics() {}

//GET metrics/connections
#[utoipa::path(
    get,
    path = "/api/v1/metrics/metrics/connections",
    params(
        ("broker_id" = String, Query, description = "Broker id"),
    ),
    responses(
        (status = 200, description = "Connection metrics", body = ConnectionsResponse),
        (status = 400, description = "Invalid broker_id ", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
#[allow(dead_code)]
async fn docs_get_metrics_connections() {}

// GET /metrics/throughput 
#[utoipa::path(
    get,
    path = "/api/v1/metrics/throughput",
    params(
        ("broker_id" = String, Query, description = "Broker id"),
    ),
    responses(
        (status = 200, description = "Throughput metric points", body = ThroughputResponse),
        (status = 400, description = "Invalid broker_id", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
#[allow(dead_code)]
async fn docs_get_metrics_throughput() {}


/// Get historical metrics for one broker
#[utoipa::path(
    get,
    path = "/api/v1/metrics/history",
    params(
        ("broker_id" = String, Query, description = "Broker id"),
        ("from" = String, Query, description = "RFC3339 start time"),
        ("to" = Option<String>, Query, description = "RFC3339 end time"),
        ("limit" = Option<usize>, Query, description = "Max rows, default 100, capped at 1000"),
        ("offset" = Option<usize>, Query),
    ),
    responses(
        (status = 200, description = "Historical metric points", body = HistoryEnvelope),
        (status = 400, description = "Invalid broker_id or timestamp", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
#[allow(dead_code)]
async fn docs_get_metrics_history() {}

/// List registered brokers
#[utoipa::path(
    get, path = "/api/v1/brokers",
    responses((status = 200, body = BrokersEnvelope)),
    security(("api_key" = []))
)]
#[allow(dead_code)]
async fn docs_get_brokers() {}

/// List alerts, optionally filtered
#[utoipa::path(
    get, path = "/api/v1/alerts",
    params(
        ("acknowledged" = Option<bool>, Query),
        ("broker_id" = Option<String>, Query),
        ("metric" = Option<String>, Query),
    ),
    responses((status = 200, body = AlertsEnvelope)),
    security(("api_key" = []))
)]
#[allow(dead_code)]
async fn docs_get_alerts() {}

/// Acknowledge an alert
#[utoipa::path(
    patch, path = "/api/v1/alerts/{id}/acknowledge",
    params(("id" = String, Path, description = "Alert id")),
    responses(
        (status = 200, body = AckResponse),
        (status = 404, description = "No alert with that id", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
#[allow(dead_code)]
async fn docs_patch_alert_acknowledge() {}

/// Reload alert rules from rules.toml
#[utoipa::path(
    post, path = "/api/v1/reload",
    responses(
        (status = 200, description = "Rules reloaded"),
        (status = 400, description = "Invalid rules file, old rules kept", body = ErrorResponse),
    ),
    security(("api_key" = []))
)]
#[allow(dead_code)]
async fn docs_post_reload() {}

#[derive(OpenApi)]
#[openapi(
    paths(
        docs_get_metrics,
        docs_get_metrics_history,
        docs_get_brokers,
        docs_get_alerts,
        docs_patch_alert_acknowledge,
        docs_post_reload,
    ),
    components(schemas(
        AlertsEnvelope, AckResponse, BrokersEnvelope,
        MetricsEnvelope, HistoryEnvelope, ErrorResponse,
    )),
    info(title = "ProgressBox API", version = "1.0.0", description = "Broker monitoring gateway API"),
    modifiers(&SecurityAddon),
)]
pub struct ApiDoc;

struct SecurityAddon;
impl utoipa::Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::security::{ApiKey, ApiKeyValue, SecurityScheme};
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "api_key",
                SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::new("x-api-key"))),
            );
        }
    }
}