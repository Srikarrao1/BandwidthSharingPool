pub mod common {
    use serde::{Deserialize, Serialize};
    use std::fmt;
    use std::{
        collections::HashMap,
        time::{SystemTime, UNIX_EPOCH},
    };
    use uuid::Uuid;
    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub enum DataPayload {
        Text(String),
        Number(f64),
        Coordinates {
            x: f64,
            y: f64,
            z: f64,
        },
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

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct DataPacket {
        pub id: String,
        pub timestamp: String,
        pub data_type: String,
        pub payload: DataPayload,
        pub metadata: HashMap<String, String>,
    }
    #[derive(Debug, Serialize, Deserialize)]
    pub struct DataRequest {
        pub request_id: String,
        pub client_id: String,
        pub data_types: Vec<String>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct DataResponse {
        /// ID of the processed packet
        pub packet_id: String,
        /// Timestamp when the packet was received
        pub received_at: String,
        /// Status of the processing
        pub status: ProcessingStatus,
        /// Time taken to process in milliseconds
        pub processing_time_ms: u64,
        /// Any errors that occurred during processing
        pub errors: Vec<String>,
        /// Processing node information
        pub processor_info: NodeInfo,
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct NodeInfo {
        /// Unique identifier for the node
        pub node_id: String,
        /// Type of the node (Master/Slave/Monitor)
        pub node_type: NodeType,
        /// Unix timestamp of the last heartbeat
        pub last_heartbeat: u64,
        /// Current operational status
        pub status: NodeStatus,
        /// Maximum number of concurrent operations the node can handle
        pub capacity: u32,
        /// Current number of operations being processed
        pub current_load: u32,
        /// Version of the node software
        pub version: String,
        /// Optional metadata as key-value pairs
        #[serde(default)]
        pub metadata: std::collections::HashMap<String, String>,
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct RoutingRequest {
        /// Unique identifier for the slave node
        pub client_id: String,
        /// Type of data this slave can process
        pub data_type: Vec<String>,
        /// Current load and capacity information
        pub node_info: NodeInfo,
        /// Requested master node ID (optional)
        pub preferred_node: Option<String>,
        /// Timestamp of the request
        pub timestamp: u64,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct RoutingResponse {
        /// ID of the master node accepting/rejecting the request
        pub node_id: String,
        /// ID of the slave node being responded to
        pub client_id: String,
        /// Whether the routing request was accepted
        pub status: RoutingStatus,
        /// If rejected, the reason why
        pub rejection_reason: Option<String>,
        /// Configuration for the slave if accepted
        pub configuration: Option<ClientConfiguration>,
        /// Timestamp of the response
        pub timestamp: u64,
    }

    /// Represents the status of a node in the system
    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
    pub enum NodeStatus {
        Active,
        Inactive,
        Maintenance,
        Error,
        Offline,
    }

    /// Represents the type of node in the system
    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
    pub enum NodeType {
        Node,
        Client,
        Monitor,
    }

    impl fmt::Display for NodeType {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            // Convert the enum variant to a string representation
            match self {
                NodeType::Node => write!(f, "Node"),
                NodeType::Client => write!(f, "Client"),
                NodeType::Monitor => write!(f, "Monitor"),
                // Add other variants as needed
            }
        }
    }

    /// Information about a node in the system including its status and capabilities

    impl NodeInfo {
        pub fn new(node_type: NodeType, capacity: u32) -> Self {
            NodeInfo {
                node_id: format!(
                    "{}-{}",
                    node_type.to_string().to_lowercase(),
                    Uuid::new_v4()
                ),
                node_type,
                last_heartbeat: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                status: NodeStatus::Active,
                capacity,
                current_load: 0,
                version: env!("CARGO_PKG_VERSION").to_string(),
                metadata: std::collections::HashMap::new(),
            }
        }
    }

    /// Possible statuses for a routing response
    #[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
    pub enum RoutingStatus {
        Accepted,
        Rejected,
        Pending,
    }

    /// Configuration provided to a slave node upon acceptance
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ClientConfiguration {
        /// Topics the slave should subscribe to
        pub subscribe_topics: Vec<String>,
        /// Topic to publish processed results to
        pub publish_topic: String,
        /// Quality of Service level to use
        pub qos: u8,
        /// Maximum batch size for processing
        pub max_batch_size: u32,
        /// Processing timeout in milliseconds
        pub processing_timeout_ms: u64,
    }

    /// Status of data processing
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub enum ProcessingStatus {
        Processed,
        Failed,
        Timeout,
        InvalidInput,
    }

    impl Default for ProcessingStatus {
        fn default() -> Self {
            ProcessingStatus::Processed
        }
    }
}
