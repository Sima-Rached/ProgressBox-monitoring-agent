//! Shared types between `agent` and `gateway`.
//!
//! These are the response/DTO shapes that cross the HTTP boundary between
//! the two services (or would be re-exposed by the gateway to its own
//! external clients). Keeping them here means the gateway can decode and
//! re-serve agent responses (or later, validate/document them via OpenAPI)
//! without duplicating struct definitions that could silently drift.
//!
//! Internal-only agent types (BrokerMetrics, AlertStore, CooldownState,
//! RulesStore, BrokerRegistry, etc.) stay in `agent`, since the gateway
//! never touches them directly — it only ever sees agent HTTP responses.

use serde::{Deserialize, Serialize};

// ── Fired alert record ──────────────────────────────────────────────────────
// Moved from agent/src/types.rs — used in AlertsEnvelope, which the gateway
// may proxy or re-serve.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FiredAlert {
    pub id: u64,
    pub broker_id: String,
    pub metric: String,
    pub operator: String,
    pub value: f64,
    pub threshold: f64,
    pub fired_at: i64,
    pub acknowledged: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertsEnvelope {
    pub alerts: Vec<FiredAlert>,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckResponse {
    pub id: u64,
    pub acknowledged: bool,
}

// ── Live metrics response — moved from agent/src/http.rs ───────────────────
// Shape shared by GET /metrics (internal) and GET /api/v1/metrics (external,
// gateway-fronted). Keeping one definition means the two endpoints can never
// silently diverge in what fields they expose.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerMetricsResponse {
    pub broker_id: String,
    pub clients_connected: Option<u64>,
    pub messages_sent: Option<u64>,
    pub messages_received: Option<u64>,
    pub bytes_sent: Option<u64>,
    pub bytes_received: Option<u64>,
    pub cpu_percent: Option<f64>,
    pub mem_usage_mb: Option<f64>,
    pub net_rx_bytes: Option<u64>,
    pub net_tx_bytes: Option<u64>,
    pub last_updated_secs: Option<i64>,
    pub stale: bool,
    pub mqtt_online: bool,
    pub docker_online: bool,
    pub online: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsEnvelope {
    pub brokers: Vec<BrokerMetricsResponse>,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokersEnvelope {
    pub brokers: Vec<String>,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

// ── Historical metrics — moved from agent/src/http.rs ───────────────────────
// NOTE (carried over from prior review): net_rx_bytes / net_tx_bytes /
// mqtt_online / docker_online are NOT present here because influx.rs never
// writes them to InfluxDB. BrokerMetricsResponse (live) has them,
// BrokerMetricsHistory (persisted) does not — that gap still exists and is
// unrelated to this scaffolding step. Flagging again so it isn't lost.

#[derive(Debug, Clone, Serialize, Deserialize, influxdb2::FromDataPoint)]
pub struct BrokerMetricsHistory {
    #[serde(rename = "broker_id")]
    pub broker_id: String,
    pub clients_connected: i64,
    pub messages_sent: i64,
    pub messages_received: i64,
    pub bytes_sent: i64,
    pub bytes_received: i64,
    pub cpu_percent: f64,
    pub mem_usage_mb: f64,
    pub time: chrono::DateTime<chrono::FixedOffset>,
}

impl Default for BrokerMetricsHistory {
    fn default() -> Self {
        Self {
            broker_id: String::new(),
            clients_connected: 0,
            messages_sent: 0,
            messages_received: 0,
            bytes_sent: 0,
            bytes_received: 0,
            cpu_percent: 0.0,
            mem_usage_mb: 0.0,
            time: chrono::DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEnvelope {
    pub broker_id: String,
    pub from: String,
    pub to: String,
    pub limit: usize,
    pub offset: usize,
    pub count: usize,
    pub results: Vec<BrokerMetricsHistory>,
}
