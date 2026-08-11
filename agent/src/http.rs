use axum::{
    extract::{Path, State},
    http::StatusCode,
    middleware,
    response::Response,
    routing::{delete, get, post, patch},
    Json, Router,
};
use chrono::Utc;
use std::sync::Arc;
use dashmap::DashMap;
use influxdb2::Client as InfluxClient;
use axum::response::IntoResponse;
use axum::extract::Query;

use crate::db::{self, DbConn};
use crate::config::{BrokerConfig, RulesConfig};
use crate::registry::{self, BrokerRuntime};
use crate::types::{BrokerMetrics, BrokerRegistry, CooldownState, RulesStore};
use crate::types::AlertStore;

// DTOs that cross the HTTP boundary live in `common` so the gateway can
// decode and re-serve them without duplicating struct definitions.
use common::{
    AlertsEnvelope, AckResponse,
    BrokerMetricsResponse, MetricsEnvelope, BrokersEnvelope,
    BrokerMetricsHistory, HistoryEnvelope,
};

// ── GET /alerts ───────────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
pub struct AlertsQuery {
    pub acknowledged: Option<bool>,
    pub broker_id: Option<String>,
    pub metric: Option<String>,
}

pub async fn get_alerts(
    State(state): State<Arc<AppState>>,
    Query(q): Query<AlertsQuery>,
) -> Json<AlertsEnvelope> {
    let alerts: Vec<_> = state
        .alerts
        .lock()
        .unwrap()
        .iter()
        .filter(|a| q.acknowledged.map_or(true, |v| a.acknowledged == v))
        .filter(|a| q.broker_id.as_deref().map_or(true, |b| a.broker_id == b))
        .filter(|a| q.metric.as_deref().map_or(true, |m| a.metric == m))
        .cloned()
        .collect();

    let count = alerts.len();
    Json(AlertsEnvelope { alerts, count })
}

// ── PATCH /alerts/:id/acknowledge ────────────────────────────────────────────

pub async fn patch_alert_acknowledge(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    match db::acknowledge_alert(&state.db, &id) {
        Ok(true) => {
            if let Some(alert) = state.alerts.lock().unwrap().iter_mut().find(|a| a.id == id) {
                alert.acknowledged = true;
            }
            (StatusCode::OK, Json(serde_json::json!({ "id": id, "acknowledged": true })))
        }
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("no alert with id {}", id) })),
        ),
        Err(e) => {
            eprintln!("[ack] db error: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "db error" })))
        }
    }
}

// ── GET /metrics ──────────────────────────────────────────────────────────────

pub async fn get_metrics(
    State(state): State<Arc<AppState>>,
) -> Json<MetricsEnvelope> {
    let now = Utc::now().timestamp();

    let mut brokers: Vec<BrokerMetricsResponse> = state
        .metrics
        .iter()
        .map(|entry| {
            let (broker_id, m) = (entry.key().clone(), entry.value());

            let stale = match m.last_updated_secs {
                Some(ts) => (now - ts) > state.stale_threshold_secs,
                None => true,
            };

            BrokerMetricsResponse {
                broker_id,
                clients_connected: m.clients_connected,
                messages_sent: m.messages_sent,
                messages_received: m.messages_received,
                bytes_sent: m.bytes_sent,
                bytes_received: m.bytes_received,
                cpu_percent: m.cpu_percent,
                mem_usage_mb: m.mem_usage_mb,
                net_rx_bytes: m.net_rx_bytes,
                net_tx_bytes: m.net_tx_bytes,
                last_updated_secs: m.last_updated_secs,
                stale,
                mqtt_online: m.mqtt_online,
                docker_online: m.docker_online,
                online: m.mqtt_online && m.docker_online,
            }
        })
        .collect();

    brokers.sort_by(|a, b| a.broker_id.cmp(&b.broker_id));
    let count = brokers.len();
    Json(MetricsEnvelope { brokers, count })
}

// ── GET /metrics/history ──────────────────────────────────────────────────────
// Kept on the agent because it needs direct InfluxDB client access.
// The gateway will proxy this as GET /api/v1/metrics/history.

#[derive(serde::Deserialize)]
pub struct HistoryQuery {
    pub broker_id: String,
    pub from: String,
    pub to: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

pub async fn get_metrics_history(
    State(state): State<Arc<AppState>>,
    Query(q): Query<HistoryQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    if chrono::DateTime::parse_from_rfc3339(&q.from).is_err() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!(
                    "invalid 'from' timestamp '{}': must be ISO 8601 / RFC 3339 (e.g. 2026-07-01T00:00:00Z)",
                    q.from
                )
            })),
        );
    }

    let to = q.to.clone().unwrap_or_else(|| Utc::now().to_rfc3339());
    if chrono::DateTime::parse_from_rfc3339(&to).is_err() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!("invalid 'to' timestamp '{}': must be ISO 8601 / RFC 3339", to)
            })),
        );
    }

    let limit  = q.limit.unwrap_or(100).min(1000);
    let offset = q.offset.unwrap_or(0);

    // Flux injection note (from prior review): broker_id is interpolated
    // directly into the query string. Validate it against the same
    // character set as BrokerConfig::id before interpolating.
    // TODO: replace with InfluxDB parameterised-query support once stable.
    let bucket     = &state.influx_bucket;
    let broker_id  = &q.broker_id;

    let flux = format!(
        r#"from(bucket: "{bucket}")
  |> range(start: {from}, stop: {to})
  |> filter(fn: (r) => r._measurement == "broker_metrics")
  |> filter(fn: (r) => r.broker_id == "{broker_id}")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"], desc: false)
  |> limit(n: {limit}, offset: {offset})"#,
        bucket    = bucket,
        from      = q.from,
        to        = to,
        broker_id = broker_id,
        limit     = limit,
        offset    = offset,
    );

    let query = influxdb2::models::Query::new(flux);

    match state.influx_client.query::<BrokerMetricsHistory>(Some(query)).await {
        Ok(results) => {
            let count = results.len();
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "broker_id": q.broker_id,
                    "from":      q.from,
                    "to":        to,
                    "limit":     limit,
                    "offset":    offset,
                    "count":     count,
                    "results":   results,
                })),
            )
        }
        Err(e) => {
            eprintln!("[history] InfluxDB query error: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "InfluxDB query failed" })),
            )
        }
    }
}

// ── GET /brokers ──────────────────────────────────────────────────────────────

pub async fn get_brokers(State(state): State<Arc<AppState>>) -> Json<BrokersEnvelope> {
    let mut brokers = registry::list_brokers(&state.registry);
    brokers.sort();
    let count = brokers.len();
    Json(BrokersEnvelope { brokers, count })
}

// ── POST /brokers ─────────────────────────────────────────────────────────────

pub async fn post_broker(
    State(state): State<Arc<AppState>>,
    Json(broker): Json<BrokerConfig>,
) -> (StatusCode, Json<serde_json::Value>) {
    if broker.id.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "broker id must not be empty" })),
        );
    }

    registry::spawn_broker(broker.clone(), &state.runtime, &state.registry);

    (
        StatusCode::CREATED,
        Json(serde_json::json!({ "registered": broker.id })),
    )
}

// ── DELETE /brokers/:id ───────────────────────────────────────────────────────

pub async fn delete_broker(
    State(state): State<Arc<AppState>>,
    Path(broker_id): Path<String>,
) -> StatusCode {
    let stopped = registry::stop_broker(&broker_id, state.metrics.clone(), &state.registry);
    if stopped {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

// ── POST /reload ──────────────────────────────────────────────────────────────

pub async fn post_reload(
    State(state): State<Arc<AppState>>,
) -> (StatusCode, Json<serde_json::Value>) {
    let new_rules = match RulesConfig::load(&state.rules_path) {
        Ok(r) => r.alert_rules,
        Err(e) => {
            eprintln!("[reload] rejected: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            );
        }
    };

    let count = new_rules.len();

    {
        let mut store = state.rules_store.write().await;
        *store = new_rules.clone();
    }

    state.cooldowns.clear();

    println!("[reload] loaded {} rule(s) from '{}'", count, state.rules_path);

    let summary: Vec<serde_json::Value> = new_rules
        .into_iter()
        .map(|r| serde_json::json!({
            "metric":        r.metric,
            "operator":      r.operator,
            "threshold":     r.threshold,
            "cooldown_secs": r.cooldown_secs,
        }))
        .collect();

    (
        StatusCode::OK,
        Json(serde_json::json!({ "reloaded": count, "rules": summary })),
    )
}

// ── Shared state ──────────────────────────────────────────────────────────────

pub struct AppState {
    pub metrics:              Arc<DashMap<String, BrokerMetrics>>,
    pub stale_threshold_secs: i64,
    pub registry:             BrokerRegistry,
    pub runtime:              BrokerRuntime,
    pub alerts:               AlertStore,
    pub rules_store:          RulesStore,
    pub cooldowns:            CooldownState,
    pub rules_path:           String,
    pub influx_client:        Arc<InfluxClient>,
    pub influx_bucket:        String,
    pub db: DbConn,
    // api_key / rate_limit_* removed — auth and rate limiting are now the
    // gateway's responsibility. The agent is internal-only.
}

// ── Router ────────────────────────────────────────────────────────────────────
// All routes are "internal" now — no API-key middleware, no GovernorLayer.
// The gateway sits in front and owns auth + rate limiting for anything
// it exposes externally. Internal management routes (/reload, /brokers
// mutations) are never forwarded by the gateway.

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/metrics",                      get(get_metrics))
        .route("/metrics/history",              get(get_metrics_history))
        .route("/brokers",                      get(get_brokers).post(post_broker))
        .route("/brokers/{id}",                 delete(delete_broker))
        .route("/alerts",                       get(get_alerts))
        .route("/alerts/{id}/acknowledge",      patch(patch_alert_acknowledge))
        .route("/reload",                       post(post_reload))
        .with_state(state)
}
