use chrono::Utc;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::Duration;

use crate::config::BrokerConfig;
use crate::types::SharedState;

pub async fn run_mqtt_task(broker: BrokerConfig, state: SharedState, scrape_interval_secs: u64) {
    loop {
        let mut mqttoptions = MqttOptions::new(
            format!("cloud-monitoring-agent-{}", broker.id),
            broker.mqtt_host.clone(),
            broker.mqtt_port,
        );
        mqttoptions.set_keep_alive(Duration::from_secs(30));

        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

        if let Err(e) = client.subscribe("$SYS/#", QoS::AtMostOnce).await {
            eprintln!("[{}] failed to subscribe to $SYS/#: {:?}", broker.id, e);
            let mut entry = state.entry(broker.id.clone()).or_default();  // ← expand
            entry.broker_host = broker.mqtt_host.clone();                 // ← add
            entry.mqtt_online = false;
            tokio::time::sleep(Duration::from_secs(scrape_interval_secs)).await;
            continue;
        }

        println!("[{}] agent subscribed to $SYS/# on {}:{}", broker.id, broker.mqtt_host, broker.mqtt_port);
        {
            let mut entry = state.entry(broker.id.clone()).or_default();  // ← expand
            entry.broker_host = broker.mqtt_host.clone();                 // ← add
            entry.mqtt_online = true;
        }

        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    let topic = publish.topic.as_str();
                    let payload = String::from_utf8_lossy(&publish.payload);

                    let mut entry = state.entry(broker.id.clone()).or_default();
                    entry.broker_host = broker.mqtt_host.clone();         // ← add

                    match topic {
                        "$SYS/broker/clients/connected" => {
                            entry.clients_connected = payload.trim().parse().ok();
                        }
                        "$SYS/broker/messages/sent" => {
                            entry.messages_sent = payload.trim().parse().ok();
                        }
                        "$SYS/broker/messages/received" => {
                            entry.messages_received = payload.trim().parse().ok();
                        }
                        "$SYS/broker/bytes/sent" => {
                            entry.bytes_sent = payload.trim().parse().ok();
                        }
                        "$SYS/broker/bytes/received" => {
                            entry.bytes_received = payload.trim().parse().ok();
                        }
                        _ => {}
                    }

                    entry.last_updated_secs = Some(Utc::now().timestamp());
                    println!("[{}] {:?}", broker.id, *entry);
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("[{}] MQTT connection lost: {:?}", broker.id, e);
                    let mut entry = state.entry(broker.id.clone()).or_default();  // ← expand
                    entry.broker_host = broker.mqtt_host.clone();                 // ← add
                    entry.mqtt_online = false;
                    tokio::time::sleep(Duration::from_secs(scrape_interval_secs)).await;
                    break;
                }
            }
        }
    }
}