use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DataPayload {
    Text(String),
    Number(f64),
    Coordinates { x: f64, y: f64, z: f64 },
    SensorData {
        sensor_id: String,
        temperature: f64,
        humidity: f64,
        pressure: f64,
    },
    ImageData {
        width: u32,
        height: u32,
        format: String,
        data: Vec<u8>,
    },
    LogEntry {
        level: String,
        message: String,
        timestamp: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataPacket {
    pub id: String,
    pub timestamp: String,
    pub data_type: String,
    pub payload: DataPayload,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataResponse {
    pub packet_id: String,
    pub received_at: String,
    pub status: String,
    pub processing_time_ms: u64,
}
