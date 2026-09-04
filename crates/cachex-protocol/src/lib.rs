use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Get { key: String },
    Set {
        key: String,
        value: Vec<u8>,
        ttl_secs: Option<u64>,
    },
    Delete { key: String },
    Ping,
    Info,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Response {
    Value(Option<Vec<u8>>),
    Ok,
    Error(String),
    Pong,
    Info {
        node_id: String,
        address: String,
        keys: usize,
        memory_bytes: usize,
        uptime_secs: u64,
    },
}

pub async fn write_framed(
    stream: &mut TcpStream,
    msg: &impl Serialize,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = bincode::serialize(msg)?;
    let len = (payload.len() as u32).to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(&payload).await?;
    Ok(())
}

pub async fn read_framed<T: for<'de> Deserialize<'de>>(
    stream: &mut TcpStream,
) -> Result<T, Box<dyn std::error::Error>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    let msg = bincode::deserialize(&payload)?;
    Ok(msg)
}
