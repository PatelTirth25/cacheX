use cachex_cli::{format_response, parse_command, parse_ttl};
use cachex_protocol::{Command, Response};

#[test]
fn parses_get() {
    assert_eq!(
        parse_command("GET user1"),
        Some(Command::Get {
            key: "user1".to_string()
        })
    );
    assert_eq!(
        parse_command("get name"),
        Some(Command::Get {
            key: "name".to_string()
        })
    );
}

#[test]
fn parses_set_without_ttl() {
    assert_eq!(
        parse_command("SET key value"),
        Some(Command::Set {
            key: "key".to_string(),
            value: b"value".to_vec(),
            ttl_secs: None,
        })
    );
}

#[test]
fn parses_set_with_ttl() {
    assert_eq!(
        parse_command("SET session abc 60"),
        Some(Command::Set {
            key: "session".to_string(),
            value: b"abc".to_vec(),
            ttl_secs: Some(60),
        })
    );
}

#[test]
fn parses_delete() {
    assert_eq!(
        parse_command("DELETE key"),
        Some(Command::Delete {
            key: "key".to_string()
        })
    );
}

#[test]
fn parses_ping_and_info() {
    assert_eq!(parse_command("PING"), Some(Command::Ping));
    assert_eq!(parse_command("ping"), Some(Command::Ping));
    assert_eq!(parse_command("INFO"), Some(Command::Info));
}

#[test]
fn rejects_invalid_commands() {
    assert_eq!(parse_command(""), None);
    assert_eq!(parse_command("GET"), None);
    assert_eq!(parse_command("DELETE"), None);
    assert_eq!(parse_command("SET"), None);
    assert_eq!(parse_command("BOGUS foo"), None);
}

#[test]
fn parses_ttl_formats() {
    assert_eq!(parse_ttl("30s"), Some(std::time::Duration::from_secs(30)));
    assert_eq!(parse_ttl("5m"), Some(std::time::Duration::from_secs(300)));
    assert_eq!(parse_ttl("2h"), Some(std::time::Duration::from_secs(7200)));
    assert_eq!(parse_ttl("120"), Some(std::time::Duration::from_secs(120)));
    assert_eq!(parse_ttl("abc"), None);
}

#[test]
fn formats_value_response() {
    assert_eq!(
        format_response(&Response::Value(Some(b"hi".to_vec()))),
        "VALUE hi"
    );
    assert_eq!(format_response(&Response::Value(None)), "(nil)");
    assert_eq!(format_response(&Response::Ok), "OK");
    assert_eq!(format_response(&Response::Pong), "PONG");
    assert_eq!(
        format_response(&Response::Error("boom".to_string())),
        "ERROR: boom"
    );
}

#[test]
fn formats_binary_value_as_byte_count() {
    let bytes = vec![0u8, 159, 146, 150];
    assert_eq!(
        format_response(&Response::Value(Some(bytes))),
        "VALUE (4 bytes)"
    );
}

#[test]
fn formats_info_response() {
    let out = format_response(&Response::Info {
        node_id: "node-1".to_string(),
        address: "127.0.0.1:7000".to_string(),
        keys: 3,
        memory_bytes: 100,
        uptime_secs: 42,
    });
    assert!(out.contains("node_id:      node-1"));
    assert!(out.contains("keys:         3"));
    assert!(out.contains("memory:       100 bytes"));
    assert!(out.contains("uptime:       42s"));
}
