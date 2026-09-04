use std::collections::BTreeMap;
use std::fmt;
use std::net::ToSocketAddrs;

use cachex_protocol::{Command, Response, read_framed, write_framed};
use tokio::net::TcpStream;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub id: String,
    pub address: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionerKind {
    Modulo,
    Consistent,
}

impl PartitionerKind {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "modulo" => Some(Self::Modulo),
            "consistent" | "consistent-hash" | "consistent-hashing" => Some(Self::Consistent),
            _ => None,
        }
    }
}

pub trait Partitioner: Send + Sync {
    fn node_id(&self, key: &str) -> &str;
    fn node_count(&self) -> usize;
}

pub fn stable_hash(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

pub struct ModuloPartitioner {
    node_ids: Vec<String>,
}

impl ModuloPartitioner {
    pub fn new(nodes: &[Node]) -> Result<Self, ClientError> {
        if nodes.is_empty() {
            return Err(ClientError::InvalidTopology(
                "cluster cannot be empty".into(),
            ));
        }
        Ok(Self {
            node_ids: nodes.iter().map(|node| node.id.clone()).collect(),
        })
    }
}

impl Partitioner for ModuloPartitioner {
    fn node_id(&self, key: &str) -> &str {
        &self.node_ids[(stable_hash(key) as usize) % self.node_ids.len()]
    }
    fn node_count(&self) -> usize {
        self.node_ids.len()
    }
}

pub struct ConsistentHashPartitioner {
    ring: BTreeMap<u64, String>,
    node_count: usize,
}

impl ConsistentHashPartitioner {
    pub fn new(nodes: &[Node]) -> Result<Self, ClientError> {
        if nodes.is_empty() {
            return Err(ClientError::InvalidTopology(
                "cluster cannot be empty".into(),
            ));
        }
        const VIRTUAL_NODES: usize = 128;
        let mut ring = BTreeMap::new();
        for node in nodes {
            for replica in 0..VIRTUAL_NODES {
                let mut token = stable_hash(&format!("{}#{}", node.id, replica));
                while ring.contains_key(&token) {
                    token = token.wrapping_add(1);
                }
                ring.insert(token, node.id.clone());
            }
        }
        Ok(Self {
            ring,
            node_count: nodes.len(),
        })
    }
}

impl Partitioner for ConsistentHashPartitioner {
    fn node_id(&self, key: &str) -> &str {
        let hash = stable_hash(key);
        self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, node_id)| node_id.as_str())
            .expect("consistent hash ring is never empty")
    }
    fn node_count(&self) -> usize {
        self.node_count
    }
}

#[derive(Debug)]
pub enum ClientError {
    InvalidTopology(String),
    InvalidPartitioner(String),
    Io(std::io::Error),
    Protocol(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTopology(message) => write!(f, "invalid cluster topology: {message}"),
            Self::InvalidPartitioner(message) => write!(f, "invalid partitioner: {message}"),
            Self::Io(error) => write!(f, "connection error: {error}"),
            Self::Protocol(error) => write!(f, "protocol error: {error}"),
        }
    }
}
impl std::error::Error for ClientError {}

pub struct ClusterClient {
    nodes: Vec<Node>,
    partitioner: Box<dyn Partitioner>,
}

impl ClusterClient {
    pub fn new(nodes: Vec<Node>, kind: PartitionerKind) -> Result<Self, ClientError> {
        validate_nodes(&nodes)?;
        let partitioner: Box<dyn Partitioner> = match kind {
            PartitionerKind::Modulo => Box::new(ModuloPartitioner::new(&nodes)?),
            PartitionerKind::Consistent => Box::new(ConsistentHashPartitioner::new(&nodes)?),
        };
        Ok(Self { nodes, partitioner })
    }
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }
    pub fn node_for_key(&self, key: &str) -> &Node {
        let node_id = self.partitioner.node_id(key);
        self.nodes
            .iter()
            .find(|node| node.id == node_id)
            .expect("partitioner returned an unknown node ID")
    }
    pub async fn execute(&self, command: Command) -> Result<Response, ClientError> {
        let node = match &command {
            Command::Get { key } | Command::Set { key, .. } | Command::Delete { key } => {
                self.node_for_key(key)
            }
            Command::Ping | Command::Info => &self.nodes[0],
        };
        let mut stream = TcpStream::connect(&node.address)
            .await
            .map_err(ClientError::Io)?;
        write_framed(&mut stream, &command)
            .await
            .map_err(|e| ClientError::Protocol(e.to_string()))?;
        read_framed(&mut stream)
            .await
            .map_err(|e| ClientError::Protocol(e.to_string()))
    }
}

pub fn parse_nodes(input: &str) -> Result<Vec<Node>, ClientError> {
    let nodes: Result<Vec<_>, _> = input
        .split(',')
        .map(|item| {
            let (id, address) = item.split_once('=').ok_or_else(|| {
                ClientError::InvalidTopology(format!("expected id=address, got '{item}'"))
            })?;
            Ok(Node {
                id: id.trim().into(),
                address: address.trim().into(),
            })
        })
        .collect();
    let nodes = nodes?;
    validate_nodes(&nodes)?;
    Ok(nodes)
}

pub fn validate_nodes(nodes: &[Node]) -> Result<(), ClientError> {
    if nodes.is_empty() {
        return Err(ClientError::InvalidTopology(
            "cluster cannot be empty".into(),
        ));
    }
    let mut ids = std::collections::HashSet::new();
    let mut addresses = std::collections::HashSet::new();
    for node in nodes {
        if node.id.is_empty() {
            return Err(ClientError::InvalidTopology(
                "node ID cannot be empty".into(),
            ));
        }
        if node.address.is_empty() {
            return Err(ClientError::InvalidTopology(format!(
                "node '{}' has an empty address",
                node.id
            )));
        }
        if !ids.insert(&node.id) {
            return Err(ClientError::InvalidTopology(format!(
                "duplicate node ID '{}'",
                node.id
            )));
        }
        if !addresses.insert(&node.address) {
            return Err(ClientError::InvalidTopology(format!(
                "duplicate node address '{}'",
                node.address
            )));
        }
        if node
            .address
            .to_socket_addrs()
            .map_err(|_| {
                ClientError::InvalidTopology(format!("invalid address '{}'", node.address))
            })?
            .next()
            .is_none()
        {
            return Err(ClientError::InvalidTopology(format!(
                "invalid address '{}'",
                node.address
            )));
        }
    }
    Ok(())
}

pub fn partitioner_from_env(value: &str) -> Result<PartitionerKind, ClientError> {
    PartitionerKind::parse(value).ok_or_else(|| ClientError::InvalidPartitioner(value.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn nodes(count: usize) -> Vec<Node> {
        (0..count)
            .map(|i| Node {
                id: format!("node-{i}"),
                address: format!("127.0.0.1:{}", 7000 + i),
            })
            .collect()
    }
    #[test]
    fn validates_topology_at_construction() {
        assert!(parse_nodes("").is_err());
        assert!(parse_nodes("node-a=127.0.0.1:7000,node-a=127.0.0.1:7001").is_err());
        assert!(parse_nodes("node-a=127.0.0.1:7000,node-b=127.0.0.1:7000").is_err());
        assert!(parse_nodes("node-a=not-an-address").is_err());
        assert_eq!(parse_nodes("node-a=127.0.0.1:7000").unwrap().len(), 1);
    }
    #[test]
    fn modulo_is_deterministic_and_bounded() {
        let p = ModuloPartitioner::new(&nodes(3)).unwrap();
        assert_eq!(p.node_id("same-key"), p.node_id("same-key"));
        assert!(nodes(3).iter().any(|node| node.id == p.node_id("same-key")));
    }
    #[test]
    fn consistent_hashing_is_deterministic() {
        let p1 = ConsistentHashPartitioner::new(&nodes(3)).unwrap();
        let p2 = ConsistentHashPartitioner::new(&nodes(3)).unwrap();
        for i in 0..1000 {
            assert_eq!(
                p1.node_id(&format!("key-{i}")),
                p2.node_id(&format!("key-{i}"))
            );
        }
    }
    #[test]
    fn measures_redistribution_for_three_to_four_nodes() {
        let modulo3 = ModuloPartitioner::new(&nodes(3)).unwrap();
        let modulo4 = ModuloPartitioner::new(&nodes(4)).unwrap();
        let consistent3 = ConsistentHashPartitioner::new(&nodes(3)).unwrap();
        let consistent4 = ConsistentHashPartitioner::new(&nodes(4)).unwrap();
        let mut modulo_moved = 0;
        let mut consistent_moved = 0;
        for i in 0..10_000 {
            let key = format!("key-{i}");
            modulo_moved += (modulo3.node_id(&key) != modulo4.node_id(&key)) as usize;
            consistent_moved += (consistent3.node_id(&key) != consistent4.node_id(&key)) as usize;
        }
        assert!(
            consistent_moved < modulo_moved,
            "consistent={consistent_moved}, modulo={modulo_moved}"
        );
    }

    #[test]
    fn consistent_hashing_is_independent_of_node_order() {
        let original = nodes(3);
        let reordered = vec![
            original[2].clone(),
            original[0].clone(),
            original[1].clone(),
        ];
        let first = ConsistentHashPartitioner::new(&original).unwrap();
        let second = ConsistentHashPartitioner::new(&reordered).unwrap();
        for i in 0..10_000 {
            let key = format!("key-{i}");
            assert_eq!(first.node_id(&key), second.node_id(&key));
        }
    }

    #[test]
    fn removing_a_node_moves_only_its_keys() {
        let before = ConsistentHashPartitioner::new(&nodes(4)).unwrap();
        let after = ConsistentHashPartitioner::new(&nodes(3)).unwrap();
        for i in 0..10_000 {
            let key = format!("key-{i}");
            let old = before.node_id(&key);
            let new = after.node_id(&key);
            if old != "node-3" {
                assert_eq!(old, new);
            }
        }
    }
}
