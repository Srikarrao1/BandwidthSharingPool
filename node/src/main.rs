use log::{error, info, warn, LevelFilter};
use mqtt_common::{
    DataPacket, DataPayload, DataRequest, NodeInfo, NodeStatus, NodeType, RoutingRequest,
    RoutingResponse, RoutingStatus, ClientConfiguration,
};
use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::signal;
use tokio::time;
use uuid::Uuid;

type DynError = Box<dyn Error + Send + Sync>;

pub struct Node {
    node_info: NodeInfo,
    client: AsyncClient,
    current_load: Arc<AtomicU32>,
}

impl Node {
    pub async fn new(capacity: u32, mqtt_host: &str, mqtt_port: u16) -> Result<Self, DynError> {
        let node_info = NodeInfo::new(NodeType::Node, capacity);
        let node_id = node_info.node_id.clone();

        let mut mqtt_options = MqttOptions::new(node_id.clone(), mqtt_host, mqtt_port);
        mqtt_options.set_keep_alive(Duration::from_secs(5));

        let (client, eventloop) = AsyncClient::new(mqtt_options, 10);

        // Subscribe to all relevant topics
        client.subscribe("data/request/#", QoS::AtLeastOnce).await?;
        client
            .subscribe("routing/request/#", QoS::AtLeastOnce)
            .await?;
        client
            .subscribe("data/incoming/#", QoS::AtLeastOnce)
            .await?;

        let node = Node {
            node_info,
            client: client.clone(),
            current_load: Arc::new(AtomicU32::new(0)),
        };

        // Start heartbeat sender
        node.start_heartbeat().await;

        // Start event loop handler
        node.start_event_loop(eventloop).await;

        Ok(node)
    }

    async fn start_heartbeat(&self) {
        let node_info_clone = self.node_info.clone();
        let client_clone = self.client.clone();
        let current_load = self.current_load.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let mut heartbeat = node_info_clone.clone();
                heartbeat.last_heartbeat = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                heartbeat.current_load = current_load.load(Ordering::Relaxed);

                if let Ok(payload) = serde_json::to_string(&heartbeat) {
                    let topic = format!("heartbeat/master/{}", heartbeat.node_id);
                    if let Err(e) = client_clone
                        .publish(&topic, QoS::AtLeastOnce, false, payload)
                        .await
                    {
                        eprintln!("Error publishing heartbeat: {:?}", e);
                    } else {
                        println!("Heartbeat sent on topic: {}", topic);
                    }
                }
            }
        });
    }

    async fn start_event_loop(&self, eventloop: EventLoop) {
        let node_info_clone = self.node_info.clone();
        let client_clone = self.client.clone();
        let current_load_clone = self.current_load.clone();

        tokio::spawn(async move {
            let mut eventloop = eventloop;

            loop {
                match eventloop.poll().await {
                    Ok(event) => {
                        if let rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish)) = event {
                            println!("Received message on topic: {}", publish.topic);

                            match publish.topic.as_str() {
                                topic if topic.starts_with("routing/request") => {
                                    if let Ok(request) =
                                        serde_json::from_slice::<RoutingRequest>(&publish.payload)
                                    {
                                        println!(
                                            "Processing routing request from slave: {}",
                                            request.client_id
                                        );
                                        Node::handle_routing_request(
                                            &request,
                                            &node_info_clone,
                                            &client_clone,
                                            &current_load_clone,
                                        )
                                        .await;
                                    }
                                }
                                topic if topic.starts_with("data/request") => {
                                    if let Ok(request) =
                                        serde_json::from_slice::<DataRequest>(&publish.payload)
                                    {
                                        println!("Processing data request: {}", request.request_id);
                                        Node::handle_data_request(
                                            &request,
                                            &node_info_clone,
                                            &client_clone,
                                        )
                                        .await;
                                    }
                                }
                                topic if topic.starts_with("data/incoming") => {
                                    if let Ok(packet) =
                                        serde_json::from_slice::<DataPacket>(&publish.payload)
                                    {
                                        println!("Processing incoming data packet: {}", packet.id);
                                        Node::handle_data_packet(
                                            &packet,
                                            &node_info_clone,
                                            &client_clone,
                                            &current_load_clone,
                                        )
                                        .await;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Event loop error: {:?}", e);
                        time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });
    }

    async fn handle_routing_request(
        request: &RoutingRequest,
        node_info: &NodeInfo,
        client: &AsyncClient,
        current_load: &Arc<AtomicU32>,
    ) {
        let current_load_val = current_load.load(Ordering::Relaxed);

        let (status, rejection_reason) = if current_load_val >= node_info.capacity {
            (
                RoutingStatus::Rejected,
                Some("Capacity limit reached".to_string()),
            )
        } else if request.preferred_node.is_some()
            && request.preferred_node.as_ref() != Some(&node_info.node_id)
        {
            (
                RoutingStatus::Rejected,
                Some("Not preferred master".to_string()),
            )
        } else {
            (RoutingStatus::Accepted, None)
        };

        let response = RoutingResponse {
            node_id: node_info.node_id.clone(),
            client_id: request.client_id.clone(),
            status,
            rejection_reason,
            configuration: if status == RoutingStatus::Accepted {
                Some(ClientConfiguration {
                    subscribe_topics: vec![
                        format!("data/response/{}/{}", node_info.node_id, request.client_id),
                        "data/broadcast/#".to_string(),
                    ],
                    publish_topic: format!(
                        "data/request/{}/{}",
                        node_info.node_id, request.client_id
                    ),
                    qos: 1,
                    max_batch_size: 100,
                    processing_timeout_ms: 5000,
                })
            } else {
                None
            },
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        if let Ok(response_payload) = serde_json::to_string(&response) {
            let topic = format!("routing/response/{}", request.client_id);
            if let Err(e) = client
                .publish(&topic, QoS::AtLeastOnce, false, response_payload)
                .await
            {
                eprintln!("Error publishing routing response: {:?}", e);
            } else {
                println!("Routing response sent on topic: {}", topic);
            }
        }
    }

    async fn handle_data_request(
        request: &DataRequest,
        node_info: &NodeInfo,
        client: &AsyncClient,
    ) {
        println!("Processing data request from slave {}", request.client_id);

        // Generate sample data packets with expanded types
        let data_packets = request
            .data_types
            .iter()
            .filter_map(|data_type| {
                let packet = match data_type.as_str() {
                    "sensor" => {
                        let mut metadata = HashMap::new();
                        metadata.insert("source".to_string(), "sensor-1".to_string());

                        Some(DataPacket {
                            data_type: data_type.clone(),
                            metadata,
                            id: Uuid::new_v4().to_string(),
                            timestamp: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs()
                                .to_string(),
                            payload: DataPayload::SensorData {
                                sensor_id: "temp-1".to_string(),
                                temperature: 23.5,
                                humidity: 45.0,
                                pressure: 1013.2,
                            },
                        })
                    }
                    "text" => {
                        let mut metadata = HashMap::new();
                        metadata.insert("type".to_string(), "text".to_string());

                        Some(DataPacket {
                            id: Uuid::new_v4().to_string(),
                            timestamp: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs()
                                .to_string(),
                            data_type: data_type.clone(),
                            payload: DataPayload::Text(format!(
                                "Sample text data for request {}",
                                request.request_id
                            )),
                            metadata,
                        })
                    }
                    "number" => {
                        let mut metadata = HashMap::new();
                        metadata.insert("type".to_string(), "number".to_string());

                        Some(DataPacket {
                            id: Uuid::new_v4().to_string(),
                            timestamp: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs()
                                .to_string(),
                            data_type: data_type.clone(),
                            payload: DataPayload::Number(42.5),
                            metadata,
                        })
                    }
                    "coordinates" => {
                        let mut metadata = HashMap::new();
                        metadata.insert("type".to_string(), "coordinates".to_string());

                        Some(DataPacket {
                            id: Uuid::new_v4().to_string(),
                            timestamp: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs()
                                .to_string(),
                            data_type: data_type.clone(),
                            payload: DataPayload::Coordinates {
                                x: 10.0,
                                y: 20.0,
                                z: 30.0,
                            },
                            metadata,
                        })
                    }
                    "image" => {
                        let mut metadata = HashMap::new();
                        metadata.insert("type".to_string(), "image".to_string());

                        Some(DataPacket {
                            id: Uuid::new_v4().to_string(),
                            timestamp: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs()
                                .to_string(),
                            data_type: data_type.clone(),
                            payload: DataPayload::ImageData {
                                width: 640,
                                height: 480,
                                format: "jpeg".to_string(),
                                data: vec![0; 100], // Sample image data
                            },
                            metadata,
                        })
                    }
                    "log" => {
                        let mut metadata = HashMap::new();
                        metadata.insert("type".to_string(), "log".to_string());

                        Some(DataPacket {
                            id: Uuid::new_v4().to_string(),
                            timestamp: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs()
                                .to_string(),
                            data_type: data_type.clone(),
                            payload: DataPayload::LogEntry {
                                level: "INFO".to_string(),
                                message: "Sample log entry".to_string(),
                                timestamp: chrono::Utc::now().to_rfc3339(),
                            },
                            metadata,
                        })
                    }
                    _ => None,
                };
                packet
            })
            .collect::<Vec<_>>();

        // Send data packets
        let response_topic = format!("data/response/{}/{}", node_info.node_id, request.client_id);

        for packet in data_packets {
            if let Ok(payload) = serde_json::to_string(&packet) {
                if let Err(e) = client
                    .publish(&response_topic, QoS::AtLeastOnce, false, payload)
                    .await
                {
                    eprintln!("Error publishing data response: {:?}", e);
                } else {
                    println!("Data packet sent on topic: {}", response_topic);
                }
            }
        }
    }

    async fn handle_data_packet(
        packet: &DataPacket,
        node_info: &NodeInfo,
        client: &AsyncClient,
        current_load: &Arc<AtomicU32>,
    ) {
        current_load.fetch_add(1, Ordering::Relaxed);

        // Process the data packet based on type
        match &packet.payload {
            DataPayload::Text(text) => {
                println!("Processing text data: {}", text);
            }
            DataPayload::Number(num) => {
                println!("Processing number data: {}", num);
            }
            DataPayload::Coordinates { x, y, z } => {
                println!("Processing coordinates: x={}, y={}, z={}", x, y, z);
            }
            DataPayload::SensorData {
                sensor_id,
                temperature,
                humidity,
                pressure,
            } => {
                println!(
                    "Processing sensor data - Sensor: {}, Temp: {}°C, Humidity: {}%, Pressure: {}hPa",
                    sensor_id, temperature, humidity, pressure
                );
            }
            DataPayload::ImageData {
                width,
                height,
                format,
                data,
            } => {
                println!(
                    "Processing image data: {}x{} {}, {} bytes",
                    width,
                    height,
                    format,
                    data.len()
                );
            }
            DataPayload::LogEntry {
                level,
                message,
                timestamp,
            } => {
                println!(
                    "Processing log entry: [{}] {} at {}",
                    level, message, timestamp
                );
            }
        }

        // Simulate processing time based on data type
        let processing_time = match &packet.payload {
            DataPayload::Text(_) => 100,
            DataPayload::Number(_) => 50,
            DataPayload::Coordinates { .. } => 150,
            DataPayload::SensorData { .. } => 200,
            DataPayload::ImageData { .. } => 500,
            DataPayload::LogEntry { .. } => 75,
        };

        time::sleep(Duration::from_millis(processing_time)).await;

        // Send processed notification
        let processed_topic = format!("data/processed/{}", packet.id);
        if let Ok(payload) = serde_json::to_string(&packet) {
            if let Err(e) = client
                .publish(&processed_topic, QoS::AtLeastOnce, false, payload)
                .await
            {
                eprintln!("Error publishing processed data: {:?}", e);
            } else {
                println!("Processed data sent on topic: {}", processed_topic);
            }
        }

        current_load.fetch_sub(1, Ordering::Relaxed);
    }
}

type BoxError = Box<dyn Error>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    /* Initialize logging with timestamp */
    env_logger::Builder::from_default_env()
        .format_timestamp_millis()
        .filter_level(LevelFilter::Info)
        .init();
    info!("Starting MQTT Node...");

    /* Load configuration */
    let config = NodeConfig {
        mqtt_host: std::env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".to_string()),
        mqtt_port: std::env::var("MQTT_PORT")
            .unwrap_or_else(|_| "1883".to_string())
            .parse()
            .unwrap_or(1883),
        node_capacity: std::env::var("NODE_CAPACITY")
            .unwrap_or_else(|_| "100".to_string())
            .parse()
            .unwrap_or(100),
    };
    info!("Using configuration: {:?}", config);

    /* Initialize the master node with error conversion */
    let node = Node::new(config.node_capacity, &config.mqtt_host, config.mqtt_port)
        .await
        .map_err(|e| -> BoxError {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;

    info!(
        "Node initialized successfully with ID: {}",
        node.node_info.node_id
    );

    /* Create a future that completes when a shutdown signal is received */
    let shutdown = async {
        match signal::ctrl_c().await {
            Ok(()) => {
                info!("Received shutdown signal");
            }
            Err(err) => {
                error!("Failed to listen for shutdown signal: {}", err);
            }
        }
    };

    /* Run the node until shutdown */
    tokio::select! {
        _ = shutdown => {
            info!("Initiating shutdown sequence...");
        }
    }

    /* Perform cleanup */
    cleanup(&node).await; // Note: Added ? operator here
    info!("Node shut down successfully");
    Ok(())
}

#[derive(Debug)]
struct NodeConfig {
    mqtt_host: String,
    mqtt_port: u16,
    node_capacity: u32,
}

async fn cleanup(node: &Node) {
    info!("Starting cleanup process...");

    // Create final heartbeat message
    let mut final_heartbeat = node.node_info.clone();
    final_heartbeat.status = NodeStatus::Inactive;

    // Publish offline status
    if let Ok(payload) = serde_json::to_string(&final_heartbeat) {
        match node
            .client
            .publish(
                format!("heartbeat/node/{}", final_heartbeat.node_id),
                QoS::AtLeastOnce,
                false,
                payload,
            )
            .await
        {
            Ok(_) => info!("Published offline status successfully"),
            Err(e) => warn!("Failed to publish offline status: {}", e),
        }
    }

    // Allow time for final messages to be sent
    tokio::time::sleep(Duration::from_secs(1)).await;
    info!("Cleanup completed");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_node_config() {
        let config = NodeConfig {
            mqtt_host: "localhost".to_string(),
            mqtt_port: 1883,
            node_capacity: 100,
        };
        assert_eq!(config.mqtt_host, "localhost");
        assert_eq!(config.mqtt_port, 1883);
        assert_eq!(config.node_capacity, 100);
    }
}
