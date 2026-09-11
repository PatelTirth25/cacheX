use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cachex_client::{ClusterClient, Node, parse_nodes, partitioner_from_env};
use cachex_protocol::{Command, Response, read_framed, write_framed};
use cachex_server::storage::{Aof, SharedStore, Store};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::timeout;

const DEFAULT_ADDR: &str = "127.0.0.1:7000";
const DEFAULT_CAPACITY: usize = 10_000;
const DEFAULT_NODE_ID: &str = "node-1";
const AOF_PATH: &str = "cachex.aof";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_usage();
        return Ok(());
    }
    let addr = option(&args, "--addr", "CACHEX_ADDR", DEFAULT_ADDR);
    let capacity = option(
        &args,
        "--capacity",
        "CACHEX_CAPACITY",
        &DEFAULT_CAPACITY.to_string(),
    )
    .parse::<usize>()?;
    let node_id = option(&args, "--node-id", "CACHEX_NODE_ID", DEFAULT_NODE_ID);
    let aof_path = option(&args, "--aof-path", "CACHEX_AOF_PATH", AOF_PATH);
    let replication_factor = option(
        &args,
        "--replication-factor",
        "CACHEX_REPLICATION_FACTOR",
        "1",
    )
    .parse::<usize>()?
    .clamp(1, 2);
    let partitioner_name = option(&args, "--partitioner", "CACHEX_PARTITIONER", "consistent");
    let partitioner = partitioner_from_env(&partitioner_name)?;
    let nodes = match option_optional(&args, "--nodes", "CACHEX_NODES") {
        Some(value) => parse_nodes(&value)?,
        None => vec![Node {
            id: node_id.clone(),
            address: addr.clone(),
        }],
    };
    if !nodes
        .iter()
        .any(|node| node.id == node_id && node.address == addr)
    {
        return Err(
            format!("CACHEX_NODE_ID/address ({node_id}={addr}) is not in CACHEX_NODES").into(),
        );
    }
    let topology = Arc::new(ClusterClient::new_with_replication_factor(
        nodes,
        partitioner,
        replication_factor,
    )?);
    let store: SharedStore = Arc::new(Mutex::new(Store::recover(
        &aof_path,
        capacity,
        node_id,
        addr.clone(),
    )));
    store.lock().await.set_aof(Aof::open(&aof_path));
    let listener = TcpListener::bind(&addr).await?;
    println!(
        "CacheX server running on {} (capacity: {}, replication factor: {})",
        addr, capacity, replication_factor
    );
    let heartbeat_interval_ms = option(
        &args,
        "--heartbeat-interval-ms",
        "CACHEX_HEARTBEAT_INTERVAL_MS",
        "1000",
    )
    .parse::<u64>()?;
    spawn_heartbeat(Arc::clone(&topology), heartbeat_interval_ms);
    loop {
        let (stream, peer) = listener.accept().await?;
        println!("New connection from: {}", peer);
        let store = Arc::clone(&store);
        let topology = Arc::clone(&topology);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, store, topology, replication_factor).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }
}

fn option(args: &[String], flag: &str, env_name: &str, default: &str) -> String {
    option_optional(args, flag, env_name).unwrap_or_else(|| default.to_string())
}

fn option_optional(args: &[String], flag: &str, env_name: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == flag)
        .map(|pair| pair[1].clone())
        .or_else(|| std::env::var(env_name).ok())
}

fn print_usage() {
    println!("CacheX server options:");
    println!("  --node-id <id>                  Node identity");
    println!("  --addr <host:port>              Listen address");
    println!("  --aof-path <path>               AOF file path");
    println!("  --nodes <id=addr,...>           Static cluster topology");
    println!("  --partitioner <consistent|modulo>");
    println!("  --replication-factor <1|2>");
    println!("  --capacity <number>");
    println!("  --heartbeat-interval-ms <ms>");
}

fn spawn_heartbeat(topology: Arc<ClusterClient>, interval_ms: u64) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
        let mut health: HashMap<String, (u32, u8)> = topology
            .nodes()
            .iter()
            .map(|n| (n.id.clone(), (0, 0)))
            .collect();
        loop {
            ticker.tick().await;
            for node in topology.nodes() {
                let result = timeout(Duration::from_millis((interval_ms / 2).max(100)), async {
                    let mut stream = TcpStream::connect(&node.address).await?;
                    write_framed(&mut stream, &Command::Heartbeat)
                        .await
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                    let _: Response = read_framed(&mut stream)
                        .await
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                    Ok::<(), std::io::Error>(())
                })
                .await;
                let entry = health.entry(node.id.clone()).or_insert((0, 0));
                if result.is_err() {
                    entry.0 += 1;
                    if entry.0 == 1 {
                        entry.1 = 1;
                        eprintln!("heartbeat: node {} SUSPECT", node.id);
                    }
                    if entry.0 >= 3 && entry.1 != 2 {
                        entry.1 = 2;
                        eprintln!("heartbeat: node {} DEAD", node.id);
                    }
                } else {
                    if entry.1 != 0 {
                        eprintln!("heartbeat: node {} RECOVERED", node.id);
                    }
                    *entry = (0, 0);
                }
            }
        }
    });
}

async fn handle_connection(
    mut stream: TcpStream,
    store: SharedStore,
    topology: Arc<ClusterClient>,
    replication_factor: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let command: Command = match read_framed(&mut stream).await {
            Ok(cmd) => cmd,
            Err(e)
                if e.to_string().contains("unexpected end of file")
                    || e.to_string().contains("early eof") =>
            {
                break;
            }
            Err(e) => return Err(e),
        };
        let response = execute(command, &store, &topology, replication_factor).await;
        write_framed(&mut stream, &response).await?;
    }
    Ok(())
}

async fn execute(
    command: Command,
    store: &SharedStore,
    topology: &ClusterClient,
    replication_factor: usize,
) -> Response {
    match command {
        Command::Get { key } => Response::Value(store.lock().await.get(&key)),
        Command::Set {
            key,
            value,
            ttl_secs,
        } => {
            store.lock().await.set(
                key.clone(),
                value.clone(),
                ttl_secs.map(Duration::from_secs),
            );
            replicate(
                topology,
                &key,
                replication_factor,
                Command::ReplicateSet {
                    key: key.clone(),
                    value,
                    ttl_secs,
                },
            )
            .await;
            Response::Ok
        }
        Command::Delete { key } => {
            if store.lock().await.delete(&key) {
                replicate(
                    topology,
                    &key,
                    replication_factor,
                    Command::ReplicateDelete { key: key.clone() },
                )
                .await;
                Response::Ok
            } else {
                Response::Error("key not found".into())
            }
        }
        Command::ReplicateSet {
            key,
            value,
            ttl_secs,
        } => {
            store
                .lock()
                .await
                .set(key, value, ttl_secs.map(Duration::from_secs));
            Response::Ok
        }
        Command::ReplicateDelete { key } => {
            store.lock().await.delete(&key);
            Response::Ok
        }
        Command::Ping | Command::Heartbeat => Response::Pong,
        Command::Info => {
            let s = store.lock().await;
            Response::Info {
                node_id: s.node_id().into(),
                address: s.address().into(),
                keys: s.keys(),
                memory_bytes: s.memory_bytes(),
                uptime_secs: s.uptime_secs(),
            }
        }
    }
}

async fn replicate(
    topology: &ClusterClient,
    key: &str,
    replication_factor: usize,
    command: Command,
) {
    for node in topology
        .replica_nodes_for_key(key, replication_factor)
        .into_iter()
        .skip(1)
    {
        let result = timeout(Duration::from_secs(2), async {
            let mut stream = TcpStream::connect(&node.address).await?;
            write_framed(&mut stream, &command)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            let _: Response = read_framed(&mut stream)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            Ok::<(), std::io::Error>(())
        })
        .await;
        if result.is_err() {
            eprintln!("replication: failed to reach {}", node.id);
        }
    }
}
