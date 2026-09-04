use std::sync::Arc;
use std::time::Duration;

use cachex_protocol::{Command, Response};
use cachex_server::storage::{Aof, SharedStore, Store};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

const DEFAULT_ADDR: &str = "127.0.0.1:7000";
const DEFAULT_CAPACITY: usize = 10_000;
const DEFAULT_NODE_ID: &str = "node-1";
const AOF_PATH: &str = "cachex.aof";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("CACHEX_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());
    let capacity: usize = std::env::var("CACHEX_CAPACITY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CAPACITY);

    let store: SharedStore = Arc::new(Mutex::new(Store::recover(
        AOF_PATH,
        capacity,
        DEFAULT_NODE_ID.to_string(),
        addr.clone(),
    )));

    {
        let mut s = store.lock().await;
        s.set_aof(Aof::open(AOF_PATH));
    }

    let listener = TcpListener::bind(&addr).await?;
    println!(
        "CacheX server running on {} (capacity: {})",
        addr, capacity
    );

    loop {
        let (stream, peer) = listener.accept().await?;
        println!("New connection from: {}", peer);

        let store = Arc::clone(&store);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, store).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    store: SharedStore,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let command: Command = match cachex_protocol::read_framed(&mut stream).await {
            Ok(cmd) => cmd,
            Err(e) if e.to_string().contains("unexpected end of file") => break,
            Err(e) => return Err(e),
        };

        let response = execute(command, &store).await;
        cachex_protocol::write_framed(&mut stream, &response).await?;
    }

    Ok(())
}

async fn execute(command: Command, store: &SharedStore) -> Response {
    match command {
        Command::Get { key } => {
            let mut s = store.lock().await;
            Response::Value(s.get(&key))
        }
        Command::Set {
            key,
            value,
            ttl_secs,
        } => {
            let mut s = store.lock().await;
            s.set(key, value, ttl_secs.map(Duration::from_secs));
            Response::Ok
        }
        Command::Delete { key } => {
            let mut s = store.lock().await;
            if s.delete(&key) {
                Response::Ok
            } else {
                Response::Error("key not found".to_string())
            }
        }
        Command::Ping => Response::Pong,
        Command::Info => {
            let s = store.lock().await;
            Response::Info {
                node_id: s.node_id().to_string(),
                address: s.address().to_string(),
                keys: s.keys(),
                memory_bytes: s.memory_bytes(),
                uptime_secs: s.uptime_secs(),
            }
        }
    }
}
