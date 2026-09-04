mod storage;

use std::sync::Arc;
use std::time::Instant;
use storage::{SharedStore, Store};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:7000";
    let listener = TcpListener::bind(addr).await?;

    let store: SharedStore = Arc::new(RwLock::new(Store::new()));
    let start_time = Instant::now();

    println!("CacheX server running on {}", addr);

    loop {
        let (stream, socket_addr) = listener.accept().await?;
        println!("New connection from: {}", socket_addr);

        let store_clone = Arc::clone(&store);
        let start = start_time;

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, store_clone, start).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    store: SharedStore,
    start_time: Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        let len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await?;

        let command: cachex_protocol::Command = bincode::deserialize(&payload)?;

        let response = match command {
            cachex_protocol::Command::Get { key } => {
                let store = store.read().await;
                cachex_protocol::Response::Value(store.get(&key))
            }
            cachex_protocol::Command::Set { key, value } => {
                let mut store = store.write().await;
                store.set(key, value);
                cachex_protocol::Response::Ok
            }
            cachex_protocol::Command::Delete { key } => {
                let mut store = store.write().await;
                if store.delete(&key) {
                    cachex_protocol::Response::Ok
                } else {
                    cachex_protocol::Response::Error("key not found".to_string())
                }
            }
            cachex_protocol::Command::Ping => cachex_protocol::Response::Pong,
            cachex_protocol::Command::Info => {
                let store = store.read().await;
                cachex_protocol::Response::Info {
                    node_id: "node-1".to_string(),
                    address: "127.0.0.1:7000".to_string(),
                    keys: store.len(),
                    uptime_secs: start_time.elapsed().as_secs(),
                }
            }
        };

        let resp_payload = bincode::serialize(&response)?;
        let resp_len = (resp_payload.len() as u32).to_be_bytes();
        stream.write_all(&resp_len).await?;
        stream.write_all(&resp_payload).await?;
    }

    Ok(())
}
