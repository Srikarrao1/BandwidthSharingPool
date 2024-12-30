use mqtt::common::{DataPacket, DataPayload, DataResponse};
use rumqttc::{Client, MqttOptions, QoS};
use std::{time::Duration, collections::HashMap, sync::{Arc, Mutex}};
use std::thread;
use chrono::Utc;
use std::time::Instant;

// Structure to track sent messages and their responses
#[derive(Debug)]
struct MessageTracker {
    sent_time: Instant,
    data_type: String,
    received_response: bool,
}

fn generate_random_data() -> DataPayload {
    let choice = rand::random::<u8>() % 6;
    match choice {
        0 => DataPayload::Text(format!("Random text message {}", rand::random::<u16>())),
        1 => DataPayload::Number(rand::random::<f64>() * 100.0),
        2 => DataPayload::Coordinates {
            x: rand::random::<f64>() * 100.0,
            y: rand::random::<f64>() * 100.0,
            z: rand::random::<f64>() * 100.0,
        },
        3 => DataPayload::SensorData {
            sensor_id: format!("SENSOR_{}", rand::random::<u16>()),
            temperature: rand::random::<f64>() * 50.0,
            humidity: rand::random::<f64>() * 100.0,
            pressure: rand::random::<f64>() * 1013.0,
        },
        4 => DataPayload::ImageData {
            width: 640,
            height: 480,
            format: "RGB".to_string(),
            data: (0..100).map(|_| rand::random::<u8>()).collect(),
        },
        _ => DataPayload::LogEntry {
            level: ["INFO", "WARN", "ERROR"][rand::random::<usize>() % 3].to_string(),
            message: format!("Log message {}", rand::random::<u16>()),
            timestamp: Utc::now().to_rfc3339(),
        },
    }
}

fn main() {
    // Create shared state for tracking messages
    let message_tracker = Arc::new(Mutex::new(HashMap::<String, MessageTracker>::new()));
    
    let mut mqtt_options = MqttOptions::new(
        format!("master-node-{}", uuid::Uuid::new_v4()),
        "localhost",
        1883,
    );
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    
    let (client, mut connection) = Client::new(mqtt_options, 10);
    let client_clone = client.clone();
    let tracker_clone = Arc::clone(&message_tracker);

    // Handle incoming responses
    thread::spawn(move || {
        for notification in connection.iter() {
            if let Ok(event) = notification {
                match event {
                    rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish)) => {
                        if publish.topic == "data/response" {
                            match serde_json::from_slice::<DataResponse>(&publish.payload) {
                                Ok(response) => {
                                    let mut tracker = tracker_clone.lock().unwrap();
                                    if let Some(tracked_msg) = tracker.get_mut(&response.packet_id) {
                                        tracked_msg.received_response = true;
                                        println!(
                                            "Received response for {} message {}: Status: {}, Processing time: {}ms",
                                            tracked_msg.data_type,
                                            response.packet_id,
                                            response.status,
                                            response.processing_time_ms
                                        );
                                        
                                        // Calculate and log response time
                                        let response_time = tracked_msg.sent_time.elapsed();
                                        println!(
                                            "Total round-trip time: {}ms",
                                            response_time.as_millis()
                                        );
                                    }
                                }
                                Err(e) => eprintln!("Failed to parse response: {:?}", e),
                            }
                        }
                    }
                    rumqttc::Event::Incoming(rumqttc::Packet::PubAck(puback)) => {
                        println!("Received PubAck for packet ID: {}", puback.pkid);
                    }
                    _ => {}
                }
            }
        }
    });

    // Periodically clean up old message tracking entries
    let tracker_cleanup = Arc::clone(&message_tracker);
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(60));
            let mut tracker = tracker_cleanup.lock().unwrap();
            tracker.retain(|_, msg| msg.sent_time.elapsed() < Duration::from_secs(300));
        }
    });

    client.subscribe("data/response", QoS::AtLeastOnce).unwrap();

    loop {
        let data = generate_random_data();
        let data_type = match &data {
            DataPayload::Text(_) => "text",
            DataPayload::Number(_) => "number",
            DataPayload::Coordinates { .. } => "coordinates",
            DataPayload::SensorData { .. } => "sensor_data",
            DataPayload::ImageData { .. } => "image_data",
            DataPayload::LogEntry { .. } => "log_entry",
        };

        let packet = DataPacket {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now().to_rfc3339(),
            data_type: data_type.to_string(),
            payload: data.clone(),
            metadata: {
                let mut map = HashMap::new();
                map.insert("source".to_string(), "master-node".to_string());
                map.insert("version".to_string(), "1.0".to_string());
                map
            },
        };

        // Track the message before sending
        {
            let mut tracker = message_tracker.lock().unwrap();
            tracker.insert(
                packet.id.clone(),
                MessageTracker {
                    sent_time: Instant::now(),
                    data_type: data_type.to_string(),
                    received_response: false,
                },
            );
        }

        match serde_json::to_string(&packet) {
            Ok(payload) => {
                if let Err(e) = client_clone.publish("data/request", QoS::AtLeastOnce, false, payload) {
                    eprintln!("Failed to send data packet: {:?}", e);
                } else {
                    println!("Sent {} : {:?}", data_type, packet.id);
                }
            }
            Err(e) => eprintln!("Failed to serialize packet: {:?}", e),
        }

        thread::sleep(Duration::from_millis(rand::random::<u64>() % 2000 + 1000));
    }
}