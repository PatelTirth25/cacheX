use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum AofEntry {
    Set {
        key: String,
        value: Vec<u8>,
        /** absolute wall-clock expiry in UNIX epoch seconds */
        expire_at: Option<u64>,
    },
    Delete {
        key: String,
    },
}

fn wall_epoch_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub struct Aof {
    writer: BufWriter<File>,
}

impl Aof {
    pub fn open(path: &str) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("failed to open AOF file");

        Self {
            writer: BufWriter::new(file),
        }
    }

    pub fn recover(path: &str) -> Result<Vec<RecoverEntry>, Box<dyn std::error::Error>> {
        if !Path::new(path).exists() {
            return Ok(Vec::new());
        }

        let data = std::fs::read(path)?;
        let mut entries = Vec::new();
        let mut offset = 0;

        while offset + 4 <= data.len() {
            let len = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + len > data.len() {
                eprintln!(
                    "AOF: truncated entry at offset {}, discarding rest",
                    offset - 4
                );
                break;
            }

            match bincode::deserialize::<AofEntry>(&data[offset..offset + len]) {
                Ok(entry) => {
                    entries.push(RecoverEntry::from_aof(entry));
                }
                Err(e) => {
                    eprintln!("AOF: corrupted entry at offset {}, skipping: {}", offset, e);
                }
            }
            offset += len;
        }

        Ok(entries)
    }

    pub fn append(&mut self, entry: &AofEntry) {
        if let Ok(payload) = bincode::serialize(entry) {
            let len = (payload.len() as u32).to_be_bytes();
            let _ = self.writer.write_all(&len);
            let _ = self.writer.write_all(&payload);
            let _ = self.writer.flush();
        }
    }
}

#[derive(Debug, Clone)]
pub enum RecoverEntry {
    Set {
        key: String,
        value: Vec<u8>,
        expiration: Option<Instant>,
    },
    Delete {
        key: String,
    },
}

impl RecoverEntry {
    fn from_aof(entry: AofEntry) -> Self {
        match entry {
            AofEntry::Set {
                key,
                value,
                expire_at,
            } => {
                let expiration = expire_at.map(|epoch| {
                    let elapsed = wall_epoch_now();
                    let remaining = epoch.saturating_sub(elapsed);
                    Instant::now() + Duration::from_secs(remaining)
                });
                Self::Set {
                    key,
                    value,
                    expiration,
                }
            }
            AofEntry::Delete { key } => Self::Delete { key },
        }
    }
}
