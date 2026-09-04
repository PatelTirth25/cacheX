use std::process::{Child, Command as StdCommand};
use std::time::Duration;

use cachex_protocol::{Command, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const BIN: &str = env!("CARGO_BIN_EXE_cachex-server");

struct ServerHandle {
    child: Child,
    addr: String,
    _workdir: std::path::PathBuf,
}

impl ServerHandle {
    async fn start(capacity: usize) -> Self {
        let port = free_port();
        let addr = format!("127.0.0.1:{}", port);

        // Isolate each server in its own temp dir so AOF state doesn't leak
        // across tests (all subprocesses would otherwise share cachex.aof).
        let workdir = std::env::temp_dir().join(format!(
            "cachex_test_server_{}_{}",
            std::process::id(),
            port
        ));
        std::fs::create_dir_all(&workdir).expect("failed to create test workdir");

        let child = StdCommand::new(BIN)
            .current_dir(&workdir)
            .env("CACHEX_ADDR", &addr)
            .env("CACHEX_CAPACITY", capacity.to_string())
            .spawn()
            .expect("failed to spawn cachex-server");

        wait_until_ready(&addr).await;
        ServerHandle {
            child,
            addr,
            _workdir: workdir,
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self._workdir);
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn wait_until_ready(addr: &str) {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("server did not come up at {}", addr);
}

async fn request(
    stream: &mut TcpStream,
    cmd: &Command,
) -> Result<Response, Box<dyn std::error::Error>> {
    let payload = bincode::serialize(cmd)?;
    let len = (payload.len() as u32).to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(&payload).await?;

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let resp_len = u32::from_be_bytes(len_buf) as usize;
    let mut resp = vec![0u8; resp_len];
    stream.read_exact(&mut resp).await?;

    Ok(bincode::deserialize(&resp)?)
}

#[tokio::test]
async fn ping_returns_pong() {
    let server = ServerHandle::start(100).await;
    let mut stream = TcpStream::connect(&server.addr).await.unwrap();
    let resp = request(&mut stream, &Command::Ping).await.unwrap();
    assert!(matches!(resp, Response::Pong));
}

#[tokio::test]
async fn set_then_get_roundtrip() {
    let server = ServerHandle::start(100).await;
    let mut stream = TcpStream::connect(&server.addr).await.unwrap();

    request(
        &mut stream,
        &Command::Set {
            key: "name".to_string(),
            value: b"Tirth".to_vec(),
            ttl_secs: None,
        },
    )
    .await
    .unwrap();

    let resp = request(
        &mut stream,
        &Command::Get {
            key: "name".to_string(),
        },
    )
    .await
    .unwrap();
    assert_eq!(resp, Response::Value(Some(b"Tirth".to_vec())));

    let resp = request(
        &mut stream,
        &Command::Get {
            key: "missing".to_string(),
        },
    )
    .await
    .unwrap();
    assert_eq!(resp, Response::Value(None));
}

#[tokio::test]
async fn delete_removes_value() {
    let server = ServerHandle::start(100).await;
    let mut stream = TcpStream::connect(&server.addr).await.unwrap();

    request(
        &mut stream,
        &Command::Set {
            key: "temp".to_string(),
            value: b"x".to_vec(),
            ttl_secs: None,
        },
    )
    .await
    .unwrap();

    let resp = request(
        &mut stream,
        &Command::Delete {
            key: "temp".to_string(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(resp, Response::Ok));

    let resp = request(
        &mut stream,
        &Command::Get {
            key: "temp".to_string(),
        },
    )
    .await
    .unwrap();
    assert_eq!(resp, Response::Value(None));
}

#[tokio::test]
async fn ttl_expires_key_over_tcp() {
    let server = ServerHandle::start(100).await;
    let mut stream = TcpStream::connect(&server.addr).await.unwrap();

    request(
        &mut stream,
        &Command::Set {
            key: "short".to_string(),
            value: b"v".to_vec(),
            ttl_secs: Some(1),
        },
    )
    .await
    .unwrap();

    let resp = request(
        &mut stream,
        &Command::Get {
            key: "short".to_string(),
        },
    )
    .await
    .unwrap();
    assert_eq!(resp, Response::Value(Some(b"v".to_vec())));

    tokio::time::sleep(Duration::from_millis(1200)).await;

    let resp = request(
        &mut stream,
        &Command::Get {
            key: "short".to_string(),
        },
    )
    .await
    .unwrap();
    assert_eq!(resp, Response::Value(None));
}

#[tokio::test]
async fn info_reports_metadata() {
    let server = ServerHandle::start(100).await;
    let mut stream = TcpStream::connect(&server.addr).await.unwrap();

    request(
        &mut stream,
        &Command::Set {
            key: "a".to_string(),
            value: b"1".to_vec(),
            ttl_secs: None,
        },
    )
    .await
    .unwrap();

    let resp = request(&mut stream, &Command::Info).await.unwrap();
    match resp {
        Response::Info {
            node_id,
            keys,
            memory_bytes,
            ..
        } => {
            assert_eq!(node_id, "node-1");
            assert_eq!(keys, 1);
            assert!(memory_bytes > 0);
        }
        other => panic!("expected Info, got {:?}", other),
    }
}

#[tokio::test]
async fn lru_evicts_over_tcp() {
    let server = ServerHandle::start(2).await;
    let mut stream = TcpStream::connect(&server.addr).await.unwrap();

    for (k, v) in [("a", "1"), ("b", "2")] {
        request(
            &mut stream,
            &Command::Set {
                key: k.to_string(),
                value: v.as_bytes().to_vec(),
                ttl_secs: None,
            },
        )
        .await
        .unwrap();
    }
    request(
        &mut stream,
        &Command::Get {
            key: "a".to_string(),
        },
    )
    .await
    .unwrap();
    request(
        &mut stream,
        &Command::Set {
            key: "c".to_string(),
            value: b"3".to_vec(),
            ttl_secs: None,
        },
    )
    .await
    .unwrap();

    let resp = request(
        &mut stream,
        &Command::Get {
            key: "b".to_string(),
        },
    )
    .await
    .unwrap();
    assert_eq!(resp, Response::Value(None)); // b was LRU and got evicted
}

#[tokio::test]
async fn multiple_clients_concurrent_operations() {
    let server = ServerHandle::start(1000).await;
    let mut handles = Vec::new();

    for i in 0..20 {
        let addr = server.addr.clone();
        handles.push(tokio::spawn(async move {
            let mut stream = TcpStream::connect(&addr).await.unwrap();
            for j in 0..50 {
                let key = format!("c{}:{}", i, j);
                let val = format!("v{}", j);
                let set = request(
                    &mut stream,
                    &Command::Set {
                        key: key.clone(),
                        value: val.as_bytes().to_vec(),
                        ttl_secs: None,
                    },
                )
                .await
                .unwrap();
                assert!(matches!(set, Response::Ok));

                let get = request(&mut stream, &Command::Get { key }).await.unwrap();
                assert_eq!(get, Response::Value(Some(val.as_bytes().to_vec())));
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}
