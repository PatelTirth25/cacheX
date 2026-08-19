use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Get { key: String },
    Set { key: String, value: Vec },
    Delete { key: String },
    Ping,
    Info,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Value(Option>),
    Ok,
    Error(String),
    Pong,
    Info {
        node_id: String,
        address: String,
        keys: usize,
        uptime_secs: u64,
    },
}
