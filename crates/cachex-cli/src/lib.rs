use std::time::Duration;

use cachex_protocol::{Command, Response};

pub fn parse_command(input: &str) -> Option<Command> {
    let parts: Vec<&str> = input.splitn(4, ' ').collect();
    let verb = parts[0].to_uppercase();

    match verb.as_str() {
        "GET" if parts.len() == 2 && !parts[1].is_empty() => Some(Command::Get {
            key: parts[1].to_string(),
        }),
        "SET" if parts.len() >= 3 => {
            let key = parts[1].to_string();
            let value = parts[2].as_bytes().to_vec();
            let ttl = if parts.len() == 4 {
                parse_ttl(parts[3])
            } else {
                None
            };
            Some(Command::Set {
                key,
                value,
                ttl_secs: ttl.map(|d| d.as_secs()),
            })
        }
        "DELETE" if parts.len() == 2 && !parts[1].is_empty() => Some(Command::Delete {
            key: parts[1].to_string(),
        }),
        "PING" if parts.len() == 1 => Some(Command::Ping),
        "INFO" if parts.len() == 1 => Some(Command::Info),
        _ => None,
    }
}

pub fn parse_ttl(s: &str) -> Option<Duration> {
    let s = s.trim();
    if let Some(n) = s.strip_suffix("s") {
        n.parse::<u64>().ok().map(Duration::from_secs)
    } else if let Some(n) = s.strip_suffix("m") {
        n.parse::<u64>().ok().map(|m| Duration::from_secs(m * 60))
    } else if let Some(n) = s.strip_suffix("h") {
        n.parse::<u64>().ok().map(|h| Duration::from_secs(h * 3600))
    } else {
        s.parse::<u64>().ok().map(Duration::from_secs)
    }
}

pub fn format_response(response: &Response) -> String {
    match response {
        Response::Value(Some(v)) => match String::from_utf8(v.clone()) {
            Ok(s) => format!("VALUE {}", s),
            Err(_) => format!("VALUE ({} bytes)", v.len()),
        },
        Response::Value(None) => "(nil)".to_string(),
        Response::Ok => "OK".to_string(),
        Response::Error(msg) => format!("ERROR: {}", msg),
        Response::Pong => "PONG".to_string(),
        Response::Info {
            node_id,
            address,
            keys,
            memory_bytes,
            uptime_secs,
        } => format!(
            "node_id:      {}\naddress:      {}\nkeys:         {}\nmemory:       {} bytes\nuptime:       {}s",
            node_id, address, keys, memory_bytes, uptime_secs
        ),
    }
}
