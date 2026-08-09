// gateway/src/main.rs
//
// The gateway is the only externally-facing HTTP service. It owns:
//   - API-key auth        (moved out of the agent in Step 2)
//   - rate limiting        (tower_governor, also moved out of the agent)
//   - reverse-proxying     /api/v1/* -> the internal agent
//
// It intentionally does NOT expose the agent's internal management routes
// (POST /brokers, DELETE /brokers/:id, POST /reload) — only the read-mostly
// surface a dashboard/client needs. Add more routes to `protected_routes()`
// below if the agent grows more public-facing endpoints.

use axum::{
    body::{Body, Bytes},
    extract::{OriginalUri, State},
    http::{HeaderMap, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, patch},
    Router,
};
use std::{net::SocketAddr, sync::Arc};
use tower_governor::{governor::GovernorConfigBuilder, GovernorLayer};

#[derive(Clone)]
struct GatewayState {
    http:           reqwest::Client,
    agent_base_url: String,
    api_key:        String,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    // Agent is internal-only now (see agent/src/main.rs) — reachable by its
    // docker-compose service name, not a published host port.
    let agent_base_url = std::env::var("AGENT_URL")
        .unwrap_or_else(|_| "http://agent:3000".to_string());

    let api_key = std::env::var("API_KEY")
        .expect("API_KEY must be set (gateway owns auth now, not the agent)");

    let rate_limit_per_second: u64 = std::env::var("RATE_LIMIT_PER_SECOND")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let rate_limit_burst: u32 = std::env::var("RATE_LIMIT_BURST")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);

    let state = Arc::new(GatewayState {
        http: reqwest::Client::new(),
        agent_base_url,
        api_key,
    });

    // Same knobs (RATE_LIMIT_PER_SECOND / RATE_LIMIT_BURST) the agent used
    // to read before Step 2 — just relocated here.
    let governor_conf = Arc::new(
        GovernorConfigBuilder::default()
            .per_second(rate_limit_per_second)
            .burst_size(rate_limit_burst)
            .finish()
            .expect("invalid rate limit configuration"),
    );

    let app = Router::new()
        .nest("/api/v1", protected_routes(state.clone()))
        .layer(middleware::from_fn_with_state(state.clone(), require_api_key))
        .layer(GovernorLayer {
            config: governor_conf,
        });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .expect("failed to bind gateway HTTP server to port 3000");
    println!("Gateway HTTP listening on http://0.0.0.0:3000 (external, auth + rate-limited)");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .expect("gateway server error");
}

// Only the agent's read-mostly / dashboard-facing routes are exposed here.
// /reload and the broker-mutation routes stay agent-internal.
fn protected_routes(state: Arc<GatewayState>) -> Router {
    Router::new()
        .route("/metrics", get(proxy))
        .route("/metrics/history", get(proxy))
        .route("/brokers", get(proxy))
        .route("/alerts", get(proxy))
        .route("/alerts/{id}/acknowledge", patch(proxy))
        .with_state(state)
}

// ── Auth ─────────────────────────────────────────────────────────────────────

async fn require_api_key(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    match headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        Some(key) if key == state.api_key => next.run(request).await,
        _ => (StatusCode::UNAUTHORIZED, "invalid or missing API key").into_response(),
    }
}

// ── Reverse proxy ────────────────────────────────────────────────────────────
//
// Forwards the request to the agent with the `/api/v1` prefix stripped
// (e.g. GET /api/v1/metrics -> GET http://agent:3000/metrics), preserving
// method, query string, body, and the agent's response status/content-type.

async fn proxy(
    State(state): State<Arc<GatewayState>>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> Response {
    let path_and_query = uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/api/v1");
    let forwarded_path = path_and_query.strip_prefix("/api/v1").unwrap_or(path_and_query);
    let target = format!("{}{}", state.agent_base_url, forwarded_path);

    let upstream = state
        .http
        .request(method, &target)
        .body(body.to_vec())
        .send()
        .await;

    match upstream {
        Ok(resp) => {
            let status = resp.status();
            let content_type = resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .cloned();
            let bytes = resp.bytes().await.unwrap_or_default();

            let mut builder = Response::builder().status(status);
            if let Some(ct) = content_type {
                builder = builder.header(axum::http::header::CONTENT_TYPE, ct.as_bytes());
            }
            builder.body(Body::from(bytes)).unwrap_or_else(|_| {
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            })
        }
        Err(e) => {
            eprintln!("[gateway] proxy error forwarding to {}: {:?}", target, e);
            (StatusCode::BAD_GATEWAY, "agent unreachable").into_response()
        }
    }
}
