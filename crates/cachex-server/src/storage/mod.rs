mod aof;
mod lru;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

pub use self::aof::{Aof, AofEntry};
use self::lru::LruList;

pub type SharedStore = Arc<Mutex<Store>>;

#[derive(Debug, Clone)]
pub struct Entry {
    pub value: Vec<u8>,
    #[allow(dead_code)]
    pub created_at: Instant,
    pub expiration: Option<Instant>,
}

pub struct Store {
    entries: HashMap<String, Entry>,
    lru: LruList,
    capacity: usize,
    node_id: String,
    address: String,
    start_time: Instant,
    aof: Option<Aof>,
}

impl Store {
    pub fn new(capacity: usize, node_id: String, address: String) -> Self {
        Self {
            entries: HashMap::new(),
            lru: LruList::new(),
            capacity,
            node_id,
            address,
            start_time: Instant::now(),
            aof: None,
        }
    }

    pub fn set_aof(&mut self, aof: Aof) {
        self.aof = Some(aof);
    }

    pub fn recover(aof_path: &str, capacity: usize, node_id: String, address: String) -> Self {
        let mut store = Self::new(capacity, node_id, address);
        let now = Instant::now();

        match Aof::recover(aof_path) {
            Ok(entries) => {
                let mut recovered = 0;
                let mut expired = 0;
                let mut deleted = 0;
                for entry in entries {
                    match entry {
                        aof::RecoverEntry::Delete { key } => {
                            store.entries.remove(&key);
                            store.lru.remove(&key);
                            deleted += 1;
                        }
                        aof::RecoverEntry::Set {
                            key,
                            value,
                            expiration,
                        } => {
                            if let Some(exp) = expiration {
                                if exp <= now {
                                    expired += 1;
                                    continue;
                                }
                            }
                            store.insert_entry(
                                key,
                                Entry {
                                    value,
                                    created_at: now,
                                    expiration,
                                },
                            );
                            recovered += 1;
                        }
                    }
                }
                println!(
                    "AOF recovery: {} entries restored, {} expired skipped, {} deletes applied",
                    recovered, expired, deleted
                );
            }
            Err(e) => {
                eprintln!("AOF recovery skipped: {}", e);
            }
        }

        store
    }

    pub fn get(&mut self, key: &str) -> Option<Vec<u8>> {
        let entry = self.entries.get(key)?;
        if self.is_expired(entry) {
            self.remove(key);
            return None;
        }
        self.lru.access(key);
        Some(entry.value.clone())
    }

    pub fn set(&mut self, key: String, value: Vec<u8>, ttl: Option<Duration>) {
        let expiration = ttl.map(|d| Instant::now() + d);

        if self.entries.len() >= self.capacity && !self.entries.contains_key(&key) {
            self.evict();
        }

        if self.entries.contains_key(&key) {
            self.lru.access(&key);
        } else {
            self.lru.push_front(key.clone());
        }

        self.entries.insert(
            key.clone(),
            Entry {
                value,
                created_at: Instant::now(),
                expiration,
            },
        );

        if let Some(ref mut aof) = self.aof {
            let stored_value = self.entries[&key].value.clone();
            let expire_at = ttl.map(|d| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + d.as_secs()
            });
            aof.append(&aof::AofEntry::Set {
                key,
                value: stored_value,
                expire_at,
            });
        }
    }

    pub fn delete(&mut self, key: &str) -> bool {
        let existed = self.entries.remove(key).is_some();
        if existed {
            self.lru.remove(key);
            if let Some(ref mut aof) = self.aof {
                aof.append(&aof::AofEntry::Delete {
                    key: key.to_string(),
                });
            }
        }
        existed
    }

    pub fn keys(&self) -> usize {
        self.entries.len()
    }

    pub fn memory_bytes(&self) -> usize {
        self.entries
            .iter()
            .map(|(k, e)| k.len() + e.value.len() + 64)
            .sum()
    }

    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    #[allow(dead_code)]
    pub fn all_entries(&self) -> &HashMap<String, Entry> {
        &self.entries
    }

    fn is_expired(&self, entry: &Entry) -> bool {
        entry
            .expiration
            .map(|exp| exp <= Instant::now())
            .unwrap_or(false)
    }

    fn remove(&mut self, key: &str) {
        self.entries.remove(key);
        self.lru.remove(key);
    }

    fn evict(&mut self) {
        while self.entries.len() >= self.capacity {
            match self.lru.pop_back() {
                Some(key) => {
                    self.entries.remove(&key);
                }
                None => break,
            }
        }
    }

    fn insert_entry(&mut self, key: String, entry: Entry) {
        self.lru.push_front(key.clone());
        self.entries.insert(key, entry);
    }
}
