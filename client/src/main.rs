use log::{error, info, LevelFilter};
use mqtt_common::{
    DataPacket, DataPayload, DataResponse, NodeInfo, NodeStatus, NodeType, ProcessingStatus,
    RoutingRequest, RoutingResponse, RoutingStatus, ClientConfiguration,
};
use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::signal;
use tokio::time;
use uuid::Uuid;

type BoxError = Box<dyn Error + Send + Sync>;
type DynError = Box<dyn Error + Send + Sync>;

#[derive(Debug)]
struct NodeConfig {
    mqtt_host: String,
    mqtt_port: u16,
    node_capacity: u32,
    data_request_interval: u64,
}
async fn cleanup(slave: &SlaveNode) -> Result<(), BoxError> {
    // Publish offline status before shutdown
    if let Some(master_id) = slave.master_id.read().await.as_ref() {
        let mut final_heartbeat = slave.node_info.clone();
        final_heartbeat.status = NodeStatus::Offline;
        if let Ok(payload) = serde_json::to_string(&final_heartbeat) {
            slave
                .client
                .publish(
                    format!("heartbeat/slave/{}", final_heartbeat.node_id),
                    QoS::AtLeastOnce,
                    false,
                    payload,
                )
                .await?;
        }
    }
    Ok(())
}

struct SlaveNode {
    node_info: NodeInfo,
    client: AsyncClient,
    current_load: Arc<AtomicU32>,
    master_id: Arc<tokio::sync::RwLock<Option<String>>>,
    config: Arc<tokio::sync::RwLock<Option<ClientConfiguration>>>,
    data_request_interval: Duration,
}

impl SlaveNode {
    async fn new(capacity: u32, data_request_interval: Duration) -> Result<Self, DynError> {
        let node_info = NodeInfo::new(NodeType::Client, capacity);
        let node_id = node_info.node_id.clone();

        let mut mqtt_options = MqttOptions::new(node_id.clone(), "localhost", 1883);
        mqtt_options.set_keep_alive(Duration::from_secs(5));

        let (client, eventloop) = AsyncClient::new(mqtt_options, 10);

        let node = SlaveNode {
            node_info,
            client: client.clone(),
            current_load: Arc::new(AtomicU32::new(0)),
            master_id: Arc::new(tokio::sync::RwLock::new(None)),
            config: Arc::new(tokio::sync::RwLock::new(None)),
            data_request_interval,
        };

        // Start heartbeat sender
        let mut node_info_clone = node.node_info.clone();
        let client_clone = client.clone();
        let current_load = node.current_load.clone();
        let master_id = node.master_id.clone();

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

                if let Some(master) = master_id.read().await.as_ref() {
                    if let Ok(payload) = serde_json::to_string(&heartbeat) {
                        if let Err(e) = client_clone
                            .publish(
                                format!("heartbeat/slave/{}", heartbeat.node_id),
                                QoS::AtLeastOnce,
                                false,
                                payload,
                            )
                            .await
                        {
                            eprintln!("Error publishing heartbeat: {:?}", e);
                            heartbeat.status = NodeStatus::Error;
                        }
                    }
                } else {
                    // If no master is assigned, send routing request
                    node_info_clone.status = NodeStatus::Inactive;
                    Self::request_routing(&client_clone, &heartbeat).await;
                }
            }
        });

        // Start data requester
        let client_clone = client.clone();
        let master_id = node.master_id.clone();
        let node_id = node.node_info.node_id.clone();
        let data_request_interval = node.data_request_interval;

        tokio::spawn(async move {
            let mut interval = time::interval(data_request_interval);
            loop {
                interval.tick().await;
                if let Some(master) = master_id.read().await.as_ref() {
                    Self::request_data(&client_clone, master, &node_id).await;
                }
            }
        });

        // Event loop handler
        let node_info_clone = node.node_info.clone();
        let client_clone = client.clone();
        let current_load_clone = node.current_load.clone();
        let master_id = node.master_id.clone();
        let config = node.config.clone();

        tokio::spawn(async move {
            handle_events(
                eventloop,
                node_info_clone,
                client_clone,
                current_load_clone,
                master_id,
                config,
            )
            .await;
        });

        Ok(node)
    }

    async fn request_routing(client: &AsyncClient, node_info: &NodeInfo) {
        let request = RoutingRequest {
            client_id: node_info.node_id.clone(),
            data_type: vec!["text".to_string(), "sensor".to_string()],
            node_info: node_info.clone(),
            preferred_node: None,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        if let Ok(payload) = serde_json::to_string(&request) {
            if let Err(e) = client
                .publish("routing/request", QoS::AtLeastOnce, false, payload)
                .await
            {
                eprintln!("Error publishing routing request: {:?}", e);
            }
        }
    }
    async fn request_data(client: &AsyncClient, master_id: &str, node_id: &str) {
        let data_request = DataRequest {
            request_id: Uuid::new_v4().to_string(),
            slave_id: node_id.to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            data_types: vec!["text".to_string(), "sensor".to_string()],
            max_items: 10,
        };

        // Publish to the specific master-slave data request topic
        let topic = format!("data/request/{}/{}", master_id, node_id);
        if let Ok(payload) = serde_json::to_string(&data_request) {
            if let Err(e) = client
                .publish(&topic, QoS::AtLeastOnce, false, payload)
                .await
            {
                eprintln!("Error publishing data request: {:?}", e);
            } else {
                println!(
                    "Sent data request to node {} on topic {}",
                    master_id, topic
                );
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct DataRequest {
    request_id: String,
    slave_id: String,
    timestamp: u64,
    data_types: Vec<String>,
    max_items: u32,
}

async fn handle_events(
    mut eventloop: EventLoop,
    node_info: NodeInfo,
    client: AsyncClient,
    current_load: Arc<AtomicU32>,
    master_id: Arc<tokio::sync::RwLock<Option<String>>>,
    config: Arc<tokio::sync::RwLock<Option<ClientConfiguration>>>,
) {
    loop {
        match eventloop.poll().await {
            Ok(event) => {
                if let rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish)) = event {
                    // Handle routing response
                    if publish
                        .topic
                        .starts_with(&format!("routing/response/slave-{}", node_info.node_id))
                    {
                        if let Ok(response) =
                            serde_json::from_slice::<RoutingResponse>(&publish.payload)
                        {
                            handle_routing_response(response, &client, &master_id, &config).await;
                        }
                    }
                    // Handle data response from master
                    else if let Some(master) = master_id.read().await.as_ref() {
                        let data_response_topic =
                            format!("data/response/{}/{}", master, node_info.node_id);
                        if publish.topic == data_response_topic {
                            if let Ok(data_packet) =
                                serde_json::from_slice::<DataPacket>(&publish.payload)
                            {
                                handle_data_response(&data_packet).await;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("[{}] Event loop error: {:?}", node_info.node_id, e);
                time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn handle_routing_response(
    response: RoutingResponse,
    client: &AsyncClient,
    master_id: &Arc<tokio::sync::RwLock<Option<String>>>,
    config: &Arc<tokio::sync::RwLock<Option<ClientConfiguration>>>,
) {
    match response.status {
        RoutingStatus::Accepted => {
            println!("Routing accepted by node: {}", response.node_id);
            *master_id.write().await = Some(response.node_id);
            if let Some(cfg) = response.configuration {
                *config.write().await = Some(cfg.clone());

                // Subscribe to configured topics
                for topic in cfg.subscribe_topics {
                    if let Err(e) = client.subscribe(&topic, QoS::AtLeastOnce).await {
                        eprintln!("Error subscribing to topic {}: {:?}", topic, e);
                    }
                }

                // Subscribe to data response topic
                let master = master_id.read().await;
                if let Some(master_id) = master.as_ref() {
                    if let Err(e) = client
                        .subscribe(format!("data/response/{}/+", master_id), QoS::AtLeastOnce)
                        .await
                    {
                        eprintln!("Error subscribing to data response topic: {:?}", e);
                    }
                }
            }
        }
        RoutingStatus::Rejected => {
            println!("Routing rejected: {:?}", response.rejection_reason);
            *master_id.write().await = None;
            *config.write().await = None;
        }
        RoutingStatus::Pending => {
            println!("Routing pending...");
        }
    }
}

async fn handle_data_response(data_packet: &DataPacket) {
    println!("Received data packet: {:?}", data_packet.id);
    match &data_packet.payload {
        DataPayload::Text(text) => println!("Text data: {}", text),
        DataPayload::SensorData {
            sensor_id,
            temperature,
            humidity,
            pressure,
        } => {
            println!(
                "Sensor {} reading: {}'Celcius {}'Bar {}'Pascal",
                sensor_id, temperature, humidity, pressure
            )
        }
        _ => println!("Other data type received"),
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    /* Initialize logging with timestamp */
    env_logger::Builder::from_default_env()
        .format_timestamp_millis()
        .filter_level(LevelFilter::Info)
        .init();
    info!("Starting MQTT Client Node...");

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
        data_request_interval: std::env::var("DATA_REQUEST_INTERVAL")
            .unwrap_or_else(|_| "10".to_string())
            .parse()
            .unwrap_or(10),
    };
    info!("Using configuration: {:?}", config);

    /* Initialize the slave node with error conversion */
    let slave = SlaveNode::new(
        config.node_capacity,
        Duration::from_secs(config.data_request_interval),
    )
    .await
    .map_err(|e| -> BoxError {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        ))
    })?;

    info!(
        "Client node initialized successfully with ID: {}",
        slave.node_info.node_id
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
    cleanup(&slave).await?;
    info!("Slave node shut down successfully");
    Ok(())
}
