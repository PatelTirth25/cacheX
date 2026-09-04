use cachex_protocol::{Command, Response, read_framed, write_framed};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn command_serializes_and_roundtrips() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let cmd: Command = read_framed(&mut stream).await.unwrap();
        write_framed(&mut stream, &cmd).await.unwrap();
    });

    let mut client = TcpStream::connect(&addr).await.unwrap();
    write_framed(
        &mut client,
        &Command::Set {
            key: "k".to_string(),
            value: b"v".to_vec(),
            ttl_secs: None,
        },
    )
    .await
    .unwrap();

    let echoed: Command = read_framed(&mut client).await.unwrap();
    assert_eq!(
        echoed,
        Command::Set {
            key: "k".to_string(),
            value: b"v".to_vec(),
            ttl_secs: None,
        }
    );

    server.await.unwrap();
}

#[tokio::test]
async fn response_serializes_and_roundtrips() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let resp: Response = read_framed(&mut stream).await.unwrap();
        write_framed(&mut stream, &resp).await.unwrap();
    });

    let mut client = TcpStream::connect(&addr).await.unwrap();
    let expected = Response::Value(Some(b"hello world".to_vec()));
    write_framed(&mut client, &expected).await.unwrap();

    let echoed: Response = read_framed(&mut client).await.unwrap();
    assert_eq!(echoed, expected);

    server.await.unwrap();
}

#[tokio::test]
async fn all_command_variants_roundtrip() {
    let commands = vec![
        Command::Get {
            key: "a".to_string(),
        },
        Command::Set {
            key: "b".to_string(),
            value: b"xyz".to_vec(),
            ttl_secs: Some(60),
        },
        Command::Delete {
            key: "c".to_string(),
        },
        Command::Ping,
        Command::Info,
    ];

    for cmd in commands {
        let payload = bincode::serialize(&cmd).unwrap();
        let decoded: Command = bincode::deserialize(&payload).unwrap();
        assert_eq!(cmd, decoded);
    }
}

#[tokio::test]
async fn framing_handles_large_payloads() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let cmd: Command = read_framed(&mut stream).await.unwrap();
        write_framed(&mut stream, &cmd).await.unwrap();
    });

    let big = vec![b'x'; 200_000];
    let mut client = TcpStream::connect(&addr).await.unwrap();
    write_framed(
        &mut client,
        &Command::Set {
            key: "big".to_string(),
            value: big.clone(),
            ttl_secs: None,
        },
    )
    .await
    .unwrap();

    let echoed: Command = read_framed(&mut client).await.unwrap();
    match echoed {
        Command::Set { value, .. } => assert_eq!(value, big),
        other => panic!("expected Set, got {:?}", other),
    }

    server.await.unwrap();
}
