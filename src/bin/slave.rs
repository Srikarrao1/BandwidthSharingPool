use mqtt::common::{DataPacket, DataPayload, DataResponse};
use rumqttc::{Client, MqttOptions, QoS};
use std::{time::Duration, sync::atomic::{AtomicU64, Ordering}};
use std::thread;
use std::time::Instant;
use chrono::DateTime;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;


struct ProcessingMetrics {
    processed_count: AtomicU64,
    total_processing_time: AtomicU64,
    text_count: AtomicU64,
    number_count: AtomicU64,
    coordinates_count: AtomicU64,
    sensor_count: AtomicU64,
    image_count: AtomicU64,
    log_count: AtomicU64,
}

impl ProcessingMetrics {
    fn new() -> Self {
        Self {
            processed_count: AtomicU64::new(0),
            total_processing_time: AtomicU64::new(0),
            text_count: AtomicU64::new(0),
            number_count: AtomicU64::new(0),
            coordinates_count: AtomicU64::new(0),
            sensor_count: AtomicU64::new(0),
            image_count: AtomicU64::new(0),
            log_count: AtomicU64::new(0),
        }
    }

    fn update_count(&self, payload: &DataPayload) {
        match payload {
            DataPayload::Text(_) => self.text_count.fetch_add(1, Ordering::Relaxed),
            DataPayload::Number(_) => self.number_count.fetch_add(1, Ordering::Relaxed),
            DataPayload::Coordinates { .. } => self.coordinates_count.fetch_add(1, Ordering::Relaxed),
            DataPayload::SensorData { .. } => self.sensor_count.fetch_add(1, Ordering::Relaxed),
            DataPayload::ImageData { .. } => self.image_count.fetch_add(1, Ordering::Relaxed),
            DataPayload::LogEntry { .. } => self.log_count.fetch_add(1, Ordering::Relaxed),
        };
    }
}

// Keeping the original process_data function
fn process_data(payload: &DataPayload) -> String {
    match payload {
        DataPayload::Text(text) => {
            println!("Processing text data: {}", text);
            format!("Text processed: {} chars", text.len())
        }
        DataPayload::Number(num) => {
            println!("Processing numeric data: {}", num);
            format!("Number processed: {:.2}", num)
        }
        DataPayload::Coordinates { x, y, z } => {
            println!("Processing coordinates: ({}, {}, {})", x, y, z);
            format!("Coordinates processed: distance from origin = {:.2}", 
                (x * x + y * y + z * z).sqrt())
        }
        DataPayload::SensorData { sensor_id, temperature, humidity, pressure } => {
            println!("Processing sensor data from {}", sensor_id);
            format!("Sensor data processed: temp={:.1}°C, humidity={:.1}%, pressure={:.1}hPa",
                temperature, humidity, pressure)
        }
        DataPayload::ImageData { width, height, format, data } => {
            println!("Processing {}x{} image in {} format", width, height, format);
            format!("Image processed: {} bytes", data.len())
        }
        DataPayload::LogEntry { level, message, timestamp } => {
            println!("Processing log entry: [{}] {}", level, message);
            format!("Log entry processed at {}", timestamp)
        }
    }
}


#[derive(Debug, Deserialize, Default)]
struct FlexiblePacket {
    id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    timestamp: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data_type: Option<String>,
    payload: Value,
    #[serde(default)]
    metadata: Option<Metadata>,
}

#[derive(Debug, Deserialize, Default)]
struct Metadata {
    #[serde(default)]
    source: String,
    #[serde(default)]
    version: String,
}

fn convert_payload(value: &Value) -> Option<DataPayload> {
    // First try simple format
    if let Value::Object(map) = value {
        if let Some(text) = map.get("Text") {
            if let Some(text_str) = text.as_str() {
                return Some(DataPayload::Text(text_str.to_string()));
            }
        }
        
        // Try complex formats
        if let Some(img_data) = map.get("ImageData") {
            if let Ok(img) = serde_json::from_value::<ImageData>(img_data.clone()) {
                return Some(DataPayload::ImageData {
                    width: img.width,
                    height: img.height,
                    format: img.format,
                    data: img.data,
                });
            }
        }
        
        if let Some(sensor_data) = map.get("SensorData") {
            if let Ok(sensor) = serde_json::from_value::<SensorData>(sensor_data.clone()) {
                return Some(DataPayload::SensorData {
                    sensor_id: sensor.sensor_id,
                    temperature: sensor.temperature,
                    humidity: sensor.humidity,
                    pressure: sensor.pressure,
                });
            }
        }
        
        if let Some(coord_data) = map.get("Coordinates") {
            if let Ok(coord) = serde_json::from_value::<Coordinates>(coord_data.clone()) {
                return Some(DataPayload::Coordinates {
                    x: coord.x,
                    y: coord.y,
                    z: coord.z,
                });
            }
        }
        
        if let Some(log_data) = map.get("LogEntry") {
            if let Ok(log) = serde_json::from_value::<LogEntry>(log_data.clone()) {
                return Some(DataPayload::LogEntry {
                    level: log.level,
                    message: log.message,
                    timestamp: log.timestamp.to_rfc3339(),
                });
            }
        }
    }
    None
}

#[derive(Debug, Deserialize)]
struct ImageData {
    width: u32,
    height: u32,
    format: String,
    data: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct SensorData {
    sensor_id: String,
    temperature: f64,
    humidity: f64,
    pressure: f64,
}

#[derive(Debug, Deserialize)]
struct Coordinates {
    x: f64,
    y: f64,
    z: f64,
}

#[derive(Debug, Deserialize)]
struct LogEntry {
    level: String,
    message: String,
    timestamp: DateTime<Utc>,
}

fn main() {
    let mut mqtt_options = MqttOptions::new(
        format!("slave-node-{}", uuid::Uuid::new_v4()),
        "localhost",
        1883,
    );
    mqtt_options
        .set_keep_alive(Duration::from_secs(5))
        .set_clean_session(true);

    println!("Connecting to MQTT broker...");
    let (client, mut connection) = Client::new(mqtt_options, 20);
    
    match client.subscribe("data/request", QoS::AtLeastOnce) {
        Ok(_) => println!("Successfully subscribed to data/request"),
        Err(e) => {
            eprintln!("Failed to subscribe: {:?}", e);
            return;
        }
    };

    let client_clone = client.clone();
    let metrics = std::sync::Arc::new(ProcessingMetrics::new());

    // Main processing thread
    thread::spawn(move || {
        println!("Starting message processing...");
        
        for notification in connection.iter() {
            match notification {
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish))) => {
                    println!("\nReceived message on topic: {}", publish.topic);
                    
                    let start_time = Instant::now();
                    let payload_str = String::from_utf8_lossy(&publish.payload);
                    
                    match serde_json::from_str::<FlexiblePacket>(&payload_str) {
                        Ok(packet) => {
                            println!("Successfully parsed message with ID: {}", packet.id);
                            
                            if let Some(data_payload) = convert_payload(&packet.payload) {
                                metrics.processed_count.fetch_add(1, Ordering::Relaxed);
                                metrics.update_count(&data_payload);

                                let result = process_data(&data_payload);
                                let processing_time = start_time.elapsed().as_millis() as u64;
                                metrics.total_processing_time.fetch_add(processing_time, Ordering::Relaxed);

                                let response = DataResponse {
                                    packet_id: packet.id,
                                    received_at: Utc::now().to_rfc3339(),
                                    status: result,
                                    processing_time_ms: processing_time,
                                };

                                if let Ok(response_payload) = serde_json::to_string(&response) {
                                    println!("Sending response: {}", response_payload);
                                    if let Err(e) = client_clone.publish(
                                        "data/response",
                                        QoS::AtLeastOnce,
                                        false,
                                        response_payload,
                                    ) {
                                        eprintln!("Failed to send response: {:?}", e);
                                    } else {
                                        println!("Response sent successfully");
                                    }
                                }
                            } else {
                                eprintln!("Failed to convert payload to DataPayload");
                                println!("Raw payload structure: {:?}", packet.payload);
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to parse message: {:?}", e);
                            eprintln!("Raw payload: {}", payload_str);
                        }
                    }
                }
                Ok(other) => println!("Received other MQTT event: {:?}", other),
                Err(e) => eprintln!("Connection error: {:?}", e),
            }
        }
    });

    // Keep the main thread alive
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}