use chrono::Utc;
use lettre::{
    message::header::ContentType, transport::smtp::authentication::Credentials, Message,
    SmtpTransport, Transport,
};
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::db::{insert_alert, DbConn};
use crate::config::EmailConfig;
use crate::types::{AlertStore, CooldownState, FiredAlert, RulesStore, SharedState};

pub async fn run_alert_task(
    state: SharedState,
    cooldowns: CooldownState,
    alert_store: AlertStore,
    rules_store: RulesStore,      // ← replaces the cloned Vec<AlertRule>
    email_cfg: EmailConfig,
    eval_interval_secs: u64,
    db_conn: DbConn,
) {
    let creds = Credentials::new(email_cfg.username.clone(), email_cfg.password.clone());
    let mailer = SmtpTransport::starttls_relay(&email_cfg.smtp_host)
        .expect("failed to build SMTP transport")
        .port(email_cfg.smtp_port)
        .credentials(creds)
        .build();


    loop {
        tokio::time::sleep(Duration::from_secs(eval_interval_secs)).await;

        // Take a point-in-time snapshot of the rules under a read lock.
        // The lock is released immediately after the clone so the eval
        // loop below (which may be slow on many brokers) never holds it.
        let rules = rules_store.read().await.clone();

        if rules.is_empty() {
            continue;
        }

        for entry in state.iter() {
            let (broker_id, metrics) = (entry.key().clone(), entry.value());

            for rule in &rules {
                let value: Option<f64> = match rule.metric.as_str() {
                    "clients_connected"  => metrics.clients_connected.map(|v| v as f64),
                    "messages_sent"      => metrics.messages_sent.map(|v| v as f64),
                    "messages_received"  => metrics.messages_received.map(|v| v as f64),
                    "bytes_sent"         => metrics.bytes_sent.map(|v| v as f64),
                    "bytes_received"     => metrics.bytes_received.map(|v| v as f64),
                    "cpu_percent"        => metrics.cpu_percent,
                    "mem_usage_mb"       => metrics.mem_usage_mb,
                    "net_rx_bytes"       => metrics.net_rx_bytes.map(|v| v as f64),
                    "net_tx_bytes"       => metrics.net_tx_bytes.map(|v| v as f64),
                    _ => None, // unreachable given validation in RulesConfig::load
                };

                let Some(value) = value else { continue };

                let breached = match rule.operator.as_str() {
                    ">" =>  value > rule.threshold,
                    "<" =>  value < rule.threshold,
                    "==" => (value - rule.threshold).abs() < f64::EPSILON,
                    _ => false,
                };
                if !breached {
                    continue;
                }

                let cooldown_key = format!("{}:{}", broker_id, rule.metric);
                let now = Instant::now();

                if let Some(last_fired) = cooldowns.get(&cooldown_key) {
                    if now.duration_since(*last_fired).as_secs() < rule.cooldown_secs {
                        continue;
                    }
                }
                cooldowns.insert(cooldown_key, now);

                let alert = FiredAlert {
                    id: Uuid::new_v4().to_string(),
                    broker_id: broker_id.clone(),
                    metric: rule.metric.clone(),
                    operator: rule.operator.clone(),
                    value,
                    threshold: rule.threshold,
                    fired_at: Utc::now().timestamp(),
                    acknowledged: false,
                };

                eprintln!(
                    "[ALERT] broker={} metric={} value={:.2} {} {} (id={})",
                    alert.broker_id, alert.metric, alert.value,
                    alert.operator, alert.threshold, alert.id
                );

                if let Err(e) = insert_alert(&db_conn, &alert) {
                eprintln!("[ALERT] failed to persist alert {}: {:?}", alert.id, e);
                // decide: continue anyway (in-memory only, degraded) or `continue;` to drop it.
                // Given alerts feed email + audit trail, degraded-but-visible is usually
                // better than silently dropping — so we fall through.
                }

                alert_store.lock().unwrap().push(alert.clone());

                for recipient in &email_cfg.to {
                    let body = format!(
                        "ProgressBox Alert\n\
                         ─────────────────\n\
                         Broker:    {}\n\
                         Metric:    {}\n\
                         Condition: {} {} {}\n\
                         Value:     {:.4}\n\
                         Time:      {} (Unix)\n\
                         Alert ID:  {}",
                        alert.broker_id,
                        alert.metric,
                        alert.metric, alert.operator, alert.threshold,
                        alert.value,
                        alert.fired_at,
                        alert.id,
                    );

                    let email = match Message::builder()
                        .from(email_cfg.from.parse().unwrap())
                        .to(recipient.parse().unwrap())
                        .subject(format!(
                            "[ProgressBox] ALERT — {} {} {} {} on {}",
                            alert.metric, alert.operator, alert.threshold,
                            alert.value, alert.broker_id
                        ))
                        .header(ContentType::TEXT_PLAIN)
                        .body(body)
                    {
                        Ok(m) => m,
                        Err(e) => {
                            eprintln!("[ALERT] failed to build email: {:?}", e);
                            continue;
                        }
                    };

                    let mailer_clone = mailer.clone();
                    let recipient_clone = recipient.clone();
                    let send_result = tokio::task::spawn_blocking(move || {
                        mailer_clone.send(&email)
                    })
                    .await;

                    match send_result {
                        Ok(Ok(_))  => println!("[ALERT] email sent to {}", recipient_clone),
                        Ok(Err(e)) => eprintln!("[ALERT] email send failed to {}: {:?}", recipient_clone, e),
                        Err(join_err) => eprintln!(
                            "[ALERT] email send task panicked for {}: {:?}", recipient_clone, join_err
                        ),
                    }
                }
            }
        }
    }
}