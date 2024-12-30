use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time;
use uuid::Uuid;


// Import the common types
use mqtt_common::{
    NodeInfo, NodeStatus, NodeType, RoutingRequest, RoutingResponse, RoutingStatus,
    ClientConfiguration,
};

#[derive(Clone)]
struct OrchestrationService {
    nodes: Arc<Mutex<HashMap<String, NodeInfo>>>,
    routing_table: Arc<Mutex<HashMap<String, String>>>,
    client: Arc<AsyncClient>,
}

impl OrchestrationService {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let mut mqtt_options = MqttOptions::new(
            format!("orchestrator-{}", Uuid::new_v4()),
            "localhost",
            1883,
        );
        mqtt_options.set_keep_alive(Duration::from_secs(5));

        let (client, eventloop) = AsyncClient::new(mqtt_options, 10);
        let client = Arc::new(client);

        let nodes = Arc::new(Mutex::new(HashMap::new()));
        let routing_table = Arc::new(Mutex::new(HashMap::new()));

        let service = OrchestrationService {
            nodes: Arc::clone(&nodes),
            routing_table: Arc::clone(&routing_table),
            client: Arc::clone(&client),
        };

        // Subscribe to required topics
        client
            .subscribe("heartbeat/master/+", QoS::AtLeastOnce)
            .await?;
        client
            .subscribe("routing/request", QoS::AtLeastOnce)
            .await?;
        client
            .subscribe("master/status/+", QoS::AtLeastOnce)
            .await?;

        // Start event loop handler
        service.start_event_loop(eventloop).await;

        Ok(service)
    }

    async fn handle_routing_request(
        &self,
        request: RoutingRequest,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut nodes_guard = self.nodes.lock().await;
        let selected_node = nodes_guard
            .iter_mut() // Note: Using iter_mut() to allow updating the load
            .filter(|(_, info)| {
                info.status == NodeStatus::Active
                    && info.current_load + 1 <= info.capacity
                    && info.node_type == NodeType::Node
            })
            .min_by_key(|(_, info)| {
                ((info.current_load as f32 / info.capacity as f32) * 100.0) as u32
            });

        if let Some((node_id, master_info)) = selected_node {
            // Update the master's load before releasing the lock
            master_info.current_load += 1;
            let node_id = node_id.clone();

            // Update routing table
            self.routing_table
                .lock()
                .await
                .insert(request.client_id.clone(), node_id.clone());

            // Create slave configuration
            let slave_config = ClientConfiguration {
                subscribe_topics: vec![
                    format!("data/input/{}", request.client_id),
                    format!("control/{}", request.client_id),
                ],
                publish_topic: format!("data/processed/{}", request.client_id),
                qos: 1,
                max_batch_size: 100,
                processing_timeout_ms: 30000,
            };

            let response = RoutingResponse {
                node_id: node_id.clone(),
                client_id: request.client_id.clone(),
                status: RoutingStatus::Accepted,
                rejection_reason: None,
                configuration: Some(slave_config),
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            };

            if let Ok(response_payload) = serde_json::to_string(&response) {
                self.client
                    .publish(
                        format!("routing/response/{}", request.client_id),
                        QoS::AtLeastOnce,
                        false,
                        response_payload.as_bytes(),
                    )
                    .await?;

                println!(
                    "Assigned Node [{}] to Client [{}] (Current load: {}/{})",
                    node_id, request.client_id, master_info.current_load, master_info.capacity
                );
            }
        } else {
            // Send rejection response if no suitable master found
            let response = RoutingResponse {
                node_id: String::from("none"),
                client_id: request.client_id.clone(),
                status: RoutingStatus::Rejected,
                rejection_reason: Some("No available master nodes".to_string()),
                configuration: None,
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            };

            if let Ok(response_payload) = serde_json::to_string(&response) {
                self.client
                    .publish(
                        format!("routing/response/{}", request.client_id),
                        QoS::AtLeastOnce,
                        false,
                        response_payload.as_bytes(),
                    )
                    .await?;
            }
            println!("No available Nodes for client {}", request.client_id);
        }
        Ok(())
    }

    async fn start_event_loop(&self, mut eventloop: rumqttc::EventLoop) {
        let nodes = Arc::clone(&self.nodes);
        let client = Arc::clone(&self.client);
        let service = self.clone();

        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(notification) => {
                        match notification {
                            Event::Incoming(Packet::Publish(publish)) => {
                                match publish.topic.as_str() {
                                    topic if topic.starts_with("heartbeat/master/") => {
                                        let node_id = topic.split('/').last().unwrap_or("unknown");
                                        if let Ok(mut node_info) =
                                            serde_json::from_slice::<NodeInfo>(&publish.payload)
                                        {
                                            // Preserve current load when updating heartbeat
                                            let current_load = nodes
                                                .lock()
                                                .await
                                                .get(node_id)
                                                .map(|info| info.current_load)
                                                .unwrap_or(0);

                                            node_info.current_load = current_load;
                                            node_info.last_heartbeat = SystemTime::now()
                                                .duration_since(UNIX_EPOCH)
                                                .unwrap()
                                                .as_secs();

                                            nodes
                                                .lock()
                                                .await
                                                .insert(node_id.to_string(), node_info);
                                        }
                                    }
                                    "routing/request" => {
                                        if let Ok(request) = serde_json::from_slice::<RoutingRequest>(
                                            &publish.payload,
                                        ) {
                                            if let Err(e) =
                                                service.handle_routing_request(request).await
                                            {
                                                eprintln!(
                                                    "Failed to handle routing request: {}",
                                                    e
                                                );
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            Event::Incoming(Packet::ConnAck(_)) => {
                                println!("Connected to MQTT broker");
                            }
                            Event::Incoming(Packet::SubAck(_)) => {
                                println!("Subscribed to topics");
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        eprintln!("Connection error: {}", e);
                        time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });
    }

    async fn cleanup_inactive_nodes(&self) {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let timeout = 15; // seconds

        let mut nodes = self.nodes.lock().await;
        let inactive_nodes: Vec<String> = nodes
            .iter()
            .filter(|(_, info)| current_time - info.last_heartbeat > timeout)
            .map(|(id, _)| id.clone())
            .collect();

        // for id in inactive_masters {
        //     masters.remove(&id);
        //     println!("Removed inactive master: {}", id);

        //     // Update master status to inactive
        //     let status_update = serde_json::json!({
        //         "status": NodeStatus::Inactive,
        //         "timestamp": current_time
        //     });

        //     if let Ok(payload) = serde_json::to_string(&status_update) {
        //         let _ = self.client.publish(
        //             format!("master/status/{}", id),
        //             QoS::AtLeastOnce,
        //             false,
        //             payload.as_bytes(),
        //         ).await;
        //     }
        // }

        // Clean up routing table and notify affected slaves
        let mut routing_table = self.routing_table.lock().await;
        let mut affected_slaves = Vec::new();

        routing_table.retain(|client_id, node_id| {
            let keep = nodes.contains_key(node_id);
            if !keep {
                affected_slaves.push(client_id.clone());
            }
            keep
        });

        // Notify affected slaves about master failure
        for client_id in affected_slaves {
            let response = RoutingResponse {
                node_id: String::from("none"),
                client_id: client_id.clone(),
                status: RoutingStatus::Rejected,
                rejection_reason: Some("Node failed to connect".to_string()),
                configuration: None,
                timestamp: current_time,
            };

            if let Ok(payload) = serde_json::to_string(&response) {
                let _ = self
                    .client
                    .publish(
                        format!("routing/response/{}", client_id),
                        QoS::AtLeastOnce,
                        false,
                        payload.as_bytes(),
                    )
                    .await;
            }
        }
    }

    async fn print_status(&self) {
        let nodes = self.nodes.lock().await;
        let routing_table = self.routing_table.lock().await;

        println!("\n=== System Status =============");
        println!("Active Nodes:");
        for (id, info) in nodes.iter() {
            println!(
                "- {} (Load: {}/{}, Status: {:?})",
                id, info.current_load, info.capacity, info.status
            );
            println!("  Version: {}", info.version);
            if !info.metadata.is_empty() {
                println!("  Metadata: {:?}", info.metadata);
            }
        }

        println!("\nActive Routings:");
        for (client_id, node_id) in routing_table.iter() {
            println!("- Client [{}] →  Node [{}]", client_id, node_id);
        }
        println!("================================\n");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Orchestration Service...");

    let service = OrchestrationService::new().await?;
    println!("Orchestration Service initialized");

    // Start periodic cleanup of inactive nodes
    let service_clone = service.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(15));
        loop {
            interval.tick().await;
            service_clone.cleanup_inactive_nodes().await;
        }
    });

    // Start periodic status printing
    let service_clone = service.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            service_clone.print_status().await;
        }
    });

    // Keep the main task running
    loop {
        time::sleep(Duration::from_secs(1)).await;
    }
}
