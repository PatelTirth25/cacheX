use std::collections::HashMap;
use std::net::SocketAddr;
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::sync::Arc;
use std::time::Duration;

use cachex_client::{ClusterClient, Node, parse_nodes, partitioner_from_env};
use cachex_protocol::{Command, Response, read_framed, write_framed};
use cachex_server::storage::{Aof, SharedStore, Store};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::timeout;

const DEFAULT_ADDR: &str = "127.0.0.1:7000";
const DEFAULT_CAPACITY: usize = 10_000;
const DEFAULT_NODE_ID: &str = "node-1";
const AOF_PATH: &str = "cachex.aof";

type SharedHealth = Arc<Mutex<HashMap<String, NodeHealth>>>;
type SharedManagedServers = Arc<Mutex<HashMap<String, ManagedServer>>>;

struct ManagedServer {
    child: Child,
    address: String,
    dashboard_address: String,
}

#[derive(Clone, Copy)]
struct NodeHealth {
    failures: u32,
    state: NodeState,
}

#[derive(Clone, Copy)]
enum NodeState {
    Starting,
    Healthy,
    Suspect,
    Dead,
}

impl NodeState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Healthy => "healthy",
            Self::Suspect => "suspect",
            Self::Dead => "dead",
        }
    }
}

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
    let dashboard_default = default_dashboard_addr(&addr);
    let dashboard_addr = option(
        &args,
        "--dashboard-addr",
        "CACHEX_DASHBOARD_ADDR",
        &dashboard_default,
    );
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
    let health: SharedHealth = Arc::new(Mutex::new(
        topology
            .nodes()
            .iter()
            .map(|node| {
                (
                    node.id.clone(),
                    NodeHealth {
                        failures: 0,
                        state: NodeState::Starting,
                    },
                )
            })
            .collect(),
    ));
    let managed_servers: SharedManagedServers = Arc::new(Mutex::new(HashMap::new()));
    let store: SharedStore = Arc::new(Mutex::new(Store::recover(
        &aof_path,
        capacity,
        node_id,
        addr.clone(),
    )));
    store.lock().await.set_aof(Aof::open(&aof_path));
    let listener = TcpListener::bind(&addr).await?;
    let dashboard_listener = TcpListener::bind(&dashboard_addr).await?;
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
    spawn_heartbeat(
        Arc::clone(&topology),
        heartbeat_interval_ms,
        Arc::clone(&health),
    );
    println!("Dashboard API available at http://{}", dashboard_addr);
    let dashboard_store = Arc::clone(&store);
    let dashboard_topology = Arc::clone(&topology);
    let dashboard_health = Arc::clone(&health);
    let dashboard_managed_servers = Arc::clone(&managed_servers);
    let dashboard_node_id = store.lock().await.node_id().to_string();
    tokio::spawn(async move {
        if let Err(error) = dashboard_loop(
            dashboard_listener,
            dashboard_store,
            dashboard_topology,
            dashboard_health,
            dashboard_managed_servers,
            dashboard_node_id,
            replication_factor,
            partitioner_name,
            capacity,
            aof_path,
        )
        .await
        {
            eprintln!("Dashboard API stopped: {error}");
        }
    });
    loop {
        let (stream, peer) = listener.accept().await?;
        let store = Arc::clone(&store);
        let topology = Arc::clone(&topology);
        tokio::spawn(async move {
            if let Err(e) =
                handle_connection(stream, peer, store, topology, replication_factor).await
            {
                eprintln!("Connection error: {}", e);
            }
        });
    }
}

fn option(args: &[String], flag: &str, env_name: &str, default: &str) -> String {
    option_optional(args, flag, env_name).unwrap_or_else(|| default.to_string())
}

fn default_dashboard_addr(cache_addr: &str) -> String {
    cache_addr
        .parse::<SocketAddr>()
        .map(|address| format!("{}:{}", address.ip(), address.port().saturating_add(1000)))
        .unwrap_or_else(|_| "127.0.0.1:8000".into())
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
    println!("  --dashboard-addr <host:port>");
}

async fn dashboard_loop(
    listener: TcpListener,
    store: SharedStore,
    topology: Arc<ClusterClient>,
    health: SharedHealth,
    managed_servers: SharedManagedServers,
    local_node_id: String,
    replication_factor: usize,
    partitioner: String,
    capacity: usize,
    aof_path: String,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let (stream, _) = listener.accept().await?;
        let store = Arc::clone(&store);
        let topology = Arc::clone(&topology);
        let health = Arc::clone(&health);
        let managed_servers = Arc::clone(&managed_servers);
        let local_node_id = local_node_id.clone();
        let partitioner = partitioner.clone();
        let aof_path = aof_path.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_dashboard_request(
                stream,
                store,
                topology,
                health,
                managed_servers,
                local_node_id,
                replication_factor,
                partitioner,
                capacity,
                aof_path,
            )
            .await
            {
                eprintln!("Dashboard request error: {error}");
            }
        });
    }
}

async fn handle_dashboard_request(
    mut stream: TcpStream,
    store: SharedStore,
    topology: Arc<ClusterClient>,
    health: SharedHealth,
    managed_servers: SharedManagedServers,
    local_node_id: String,
    replication_factor: usize,
    partitioner: String,
    capacity: usize,
    aof_path: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = read_http_request(&mut stream).await?;
    let mut lines = request.splitn(2, "\r\n");
    let request_line = lines.next().unwrap_or_default();
    let rest = lines.next().unwrap_or_default();
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or_default();
    let path = request_parts.next().unwrap_or_default();
    let body = rest
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .unwrap_or_default();

    if method == "OPTIONS" {
        write_http_response(&mut stream, 204, b"").await?;
        return Ok(());
    }

    match (method, path) {
        ("GET", "/api/health") => {
            write_json_response(&mut stream, 200, &json!({"status": "ok"})).await?;
        }
        ("GET", "/api/overview") => {
            let store = store.lock().await;
            let health = health.lock().await;
            let nodes: Vec<_> = topology
                .nodes()
                .iter()
                .map(|node| {
                    let node_health = health.get(&node.id).copied().unwrap_or(NodeHealth {
                        failures: 0,
                        state: NodeState::Starting,
                    });
                    json!({
                        "id": node.id,
                        "address": node.address,
                        "status": node_health.state.as_str(),
                        "failure_count": node_health.failures,
                        "local": node.id == local_node_id,
                    })
                })
                .collect();
            let overview = json!({
                "node": {
                    "id": store.node_id(),
                    "address": store.address(),
                    "keys": store.keys(),
                    "memory_bytes": store.memory_bytes(),
                    "uptime_secs": store.uptime_secs()
                },
                "nodes": nodes,
                "replication_factor": replication_factor,
                "partitioner": partitioner,
                "capacity": capacity,
                "aof_path": aof_path,
            });
            write_json_response(&mut stream, 200, &overview).await?;
        }
        ("POST", "/api/command") => {
            let payload: serde_json::Value = match serde_json::from_str(body) {
                Ok(value) => value,
                Err(error) => {
                    write_json_response(&mut stream, 400, &json!({"error": error.to_string()}))
                        .await?;
                    return Ok(());
                }
            };
            let command = match dashboard_command(&payload) {
                Ok(command) => command,
                Err(error) => {
                    write_json_response(&mut stream, 400, &json!({"error": error})).await?;
                    return Ok(());
                }
            };
            let route = command_route(&command, &topology, replication_factor);
            match topology.execute(command).await {
                Ok(response) => {
                    write_json_response(
                        &mut stream,
                        200,
                        &json!({"response": response, "route": route}),
                    )
                    .await?;
                }
                Err(error) => {
                    write_json_response(
                        &mut stream,
                        503,
                        &json!({"error": error.to_string(), "route": route}),
                    )
                    .await?;
                }
            }
        }
        ("GET", "/api/servers") => {
            let servers = managed_server_status(&managed_servers).await;
            write_json_response(&mut stream, 200, &json!({"servers": servers})).await?;
        }
        ("POST", "/api/servers/start") => {
            match start_managed_server(body, &managed_servers).await {
                Ok(server) => {
                    write_json_response(&mut stream, 201, &json!({"server": server})).await?
                }
                Err(error) => {
                    write_json_response(&mut stream, 400, &json!({"error": error})).await?
                }
            }
        }
        ("POST", "/api/servers/stop") => match stop_managed_server(body, &managed_servers).await {
            Ok(server) => write_json_response(&mut stream, 200, &json!({"server": server})).await?,
            Err(error) => write_json_response(&mut stream, 400, &json!({"error": error})).await?,
        },
        _ => write_json_response(&mut stream, 404, &json!({"error": "not found"})).await?,
    }
    Ok(())
}

async fn managed_server_status(servers: &SharedManagedServers) -> Vec<serde_json::Value> {
    let mut servers = servers.lock().await;
    servers.retain(|_, server| server.child.try_wait().ok().flatten().is_none());
    servers
        .iter()
        .map(|(node_id, server)| {
            json!({
                "node_id": node_id,
                "pid": server.child.id(),
                "address": server.address,
                "dashboard_address": server.dashboard_address,
                "status": "running",
            })
        })
        .collect()
}

async fn start_managed_server(
    body: &str,
    servers: &SharedManagedServers,
) -> Result<serde_json::Value, String> {
    let payload: serde_json::Value =
        serde_json::from_str(body).map_err(|error| error.to_string())?;
    let node_id = required_text(&payload, "node_id")?.to_string();
    let address = required_text(&payload, "address")?.to_string();
    let dashboard_address = required_text(&payload, "dashboard_address")?.to_string();
    let aof_path = required_text(&payload, "aof_path")?.to_string();
    let nodes = required_text(&payload, "nodes")?;
    let partitioner = payload
        .get("partitioner")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("consistent");
    if partitioner_from_env(partitioner).is_err() {
        return Err("partitioner must be consistent or modulo".into());
    }
    let replication_factor = payload
        .get("replication_factor")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1)
        .clamp(1, 2);
    let capacity = payload
        .get("capacity")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(DEFAULT_CAPACITY as u64);
    let heartbeat_interval_ms = payload
        .get("heartbeat_interval_ms")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1000);

    let parsed_nodes = parse_nodes(nodes).map_err(|error| error.to_string())?;
    if !parsed_nodes
        .iter()
        .any(|node| node.id == node_id && node.address == address)
    {
        return Err("nodes must include the selected node_id and address".into());
    }

    let mut servers = servers.lock().await;
    if servers.contains_key(&node_id) {
        return Err(format!("managed server '{node_id}' is already running"));
    }
    let executable = std::env::current_exe().map_err(|error| error.to_string())?;
    let child = ProcessCommand::new(executable)
        .args([
            "--node-id",
            &node_id,
            "--addr",
            &address,
            "--aof-path",
            &aof_path,
            "--dashboard-addr",
            &dashboard_address,
            "--nodes",
            nodes,
            "--partitioner",
            partitioner,
            "--replication-factor",
            &replication_factor.to_string(),
            "--capacity",
            &capacity.to_string(),
            "--heartbeat-interval-ms",
            &heartbeat_interval_ms.to_string(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| format!("failed to start server: {error}"))?;
    let pid = child.id();
    servers.insert(
        node_id.clone(),
        ManagedServer {
            child,
            address: address.clone(),
            dashboard_address: dashboard_address.clone(),
        },
    );
    Ok(json!({
        "node_id": node_id,
        "pid": pid,
        "address": address,
        "dashboard_address": dashboard_address,
        "status": "starting",
    }))
}

async fn stop_managed_server(
    body: &str,
    servers: &SharedManagedServers,
) -> Result<serde_json::Value, String> {
    let payload: serde_json::Value =
        serde_json::from_str(body).map_err(|error| error.to_string())?;
    let node_id = required_text(&payload, "node_id")?;
    let mut servers = servers.lock().await;
    let mut server = servers
        .remove(node_id)
        .ok_or_else(|| format!("managed server '{node_id}' was not found"))?;
    server
        .child
        .kill()
        .map_err(|error| format!("failed to stop server '{node_id}': {error}"))?;
    let _ = server.child.wait();
    Ok(json!({"node_id": node_id, "status": "stopped"}))
}

fn required_text<'a>(payload: &'a serde_json::Value, field: &str) -> Result<&'a str, String> {
    payload
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("{field} is required"))
}

fn command_route(
    command: &Command,
    topology: &ClusterClient,
    replication_factor: usize,
) -> serde_json::Value {
    let Some(key) = (match command {
        Command::Get { key } | Command::Set { key, .. } | Command::Delete { key } => Some(key),
        _ => None,
    }) else {
        return json!({});
    };
    let replicas = topology
        .replica_nodes_for_key(key, replication_factor)
        .iter()
        .map(|node| json!({"id": node.id, "address": node.address}))
        .collect::<Vec<_>>();
    json!({
        "key": key,
        "primary": replicas.first().cloned().unwrap_or_else(|| json!(null)),
        "replicas": replicas,
    })
}

fn dashboard_command(payload: &serde_json::Value) -> Result<Command, String> {
    let operation = payload
        .get("operation")
        .and_then(serde_json::Value::as_str)
        .ok_or("operation is required")?;
    let key = payload
        .get("key")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    if key.is_empty() {
        return Err("key is required".into());
    }
    match operation.to_ascii_lowercase().as_str() {
        "get" => Ok(Command::Get { key }),
        "delete" => Ok(Command::Delete { key }),
        "set" => {
            let value = payload
                .get("value")
                .and_then(serde_json::Value::as_str)
                .ok_or("value is required")?
                .as_bytes()
                .to_vec();
            let ttl_secs = payload.get("ttl_secs").and_then(serde_json::Value::as_u64);
            Ok(Command::Set {
                key,
                value,
                ttl_secs,
            })
        }
        _ => Err("operation must be GET, SET, or DELETE".into()),
    }
}

async fn read_http_request(stream: &mut TcpStream) -> Result<String, Box<dyn std::error::Error>> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end;
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Ok(String::from_utf8_lossy(&buffer).into_owned());
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            header_end = end + 4;
            break;
        }
        if buffer.len() > 64 * 1024 {
            return Err("HTTP request headers too large".into());
        }
    }
    let headers = String::from_utf8_lossy(&buffer[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            line.strip_prefix("Content-Length:")
                .or_else(|| line.strip_prefix("content-length:"))
        })
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    while buffer.len() < header_end + content_length {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    Ok(String::from_utf8_lossy(&buffer).into_owned())
}

async fn write_json_response(
    stream: &mut TcpStream,
    status: u16,
    value: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let body = serde_json::to_vec(value)?;
    write_http_response(stream, status, &body).await
}

async fn write_http_response(
    stream: &mut TcpStream,
    status: u16,
    body: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let reason = match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "Error",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Headers: Content-Type\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(body).await?;
    Ok(())
}

fn spawn_heartbeat(topology: Arc<ClusterClient>, interval_ms: u64, health: SharedHealth) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
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
                if result.is_err() {
                    let mut health = health.lock().await;
                    let entry = health.entry(node.id.clone()).or_insert(NodeHealth {
                        failures: 0,
                        state: NodeState::Starting,
                    });
                    entry.failures += 1;
                    if entry.failures == 1 {
                        entry.state = NodeState::Suspect;
                        eprintln!("heartbeat: node {} SUSPECT", node.id);
                    }
                    if entry.failures >= 3 && !matches!(entry.state, NodeState::Dead) {
                        entry.state = NodeState::Dead;
                        eprintln!("heartbeat: node {} DEAD", node.id);
                    }
                } else {
                    let mut health = health.lock().await;
                    let entry = health.entry(node.id.clone()).or_insert(NodeHealth {
                        failures: 0,
                        state: NodeState::Starting,
                    });
                    if !matches!(entry.state, NodeState::Starting | NodeState::Healthy) {
                        eprintln!("heartbeat: node {} RECOVERED", node.id);
                    }
                    *entry = NodeHealth {
                        failures: 0,
                        state: NodeState::Healthy,
                    };
                }
            }
        }
    });
}

async fn handle_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
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
        if !matches!(
            command,
            Command::Heartbeat | Command::ReplicateSet { .. } | Command::ReplicateDelete { .. }
        ) {
            println!("Client command from {}: {}", peer, command_name(&command));
        }
        let response = execute(command, &store, &topology, replication_factor).await;
        write_framed(&mut stream, &response).await?;
    }
    Ok(())
}

fn command_name(command: &Command) -> &'static str {
    match command {
        Command::Get { .. } => "GET",
        Command::Set { .. } => "SET",
        Command::Delete { .. } => "DELETE",
        Command::Ping => "PING",
        Command::Info => "INFO",
        Command::ReplicateSet { .. } => "REPLICATE_SET",
        Command::ReplicateDelete { .. } => "REPLICATE_DELETE",
        Command::Heartbeat => "HEARTBEAT",
    }
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
            let local_node_id = store.lock().await.node_id().to_string();
            store.lock().await.set(
                key.clone(),
                value.clone(),
                ttl_secs.map(Duration::from_secs),
            );
            replicate(
                topology,
                &key,
                replication_factor,
                &local_node_id,
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
            let local_node_id = store.lock().await.node_id().to_string();
            if store.lock().await.delete(&key) {
                replicate(
                    topology,
                    &key,
                    replication_factor,
                    &local_node_id,
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
    local_node_id: &str,
    command: Command,
) {
    for node in topology
        .replica_nodes_for_key(key, replication_factor)
        .into_iter()
        .filter(|node| node.id != local_node_id)
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
