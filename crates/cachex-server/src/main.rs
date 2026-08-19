mod storage;

use std::sync::Arc;
use storage::{SharedStore, Store};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

#[tokio::main]
async fn main() -> Result<(), Box> {
    let addr = "127.0.0.1:7000";
    let listener = TcpListener::bind(addr).await?;

    let store: SharedStore = Arc::new(RwLock::new(Store::new()));

    println!("CacheX server running on {}", addr);

    loop {
        let (stream, socket_addr) = listener.accept().await?;
        println!("New connection from: {}", socket_addr);

        let store_clone = Arc::clone(&store);

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, store_clone).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream, store: SharedStore) -> Result<(), Box> {
    // TODO:
    // 1. Read 4 bytes (u32 length prefix)
    // 2. Read N bytes (the bincode payload)
    // 3. Deserialize into cachex_protocol::Command
    // 4. Match on the command, lock the store, and execute
    // 5. Serialize cachex_protocol::Response
    // 6. Write 4 bytes (u32 length prefix) + N bytes payload back to stream

    Ok(())
}
