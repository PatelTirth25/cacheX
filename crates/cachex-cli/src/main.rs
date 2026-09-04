use std::io::{self, Write};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use cachex_protocol::{Command, Response};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:7000";
    let mut stream = TcpStream::connect(addr).await?;

    println!("Connected to CacheX at {}", addr);

    loop {
        print!("> ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim();

        if input.is_empty() {
            continue;
        }
        if input == "exit" || input == "quit" {
            break;
        }

        let command = match parse_command(input) {
            Some(cmd) => cmd,
            None => {
                eprintln!("Unknown command. Usage: GET <key> | SET <key> <value> | DELETE <key> | PING | INFO");
                continue;
            }
        };

        let payload = bincode::serialize(&command)?;
        let len = (payload.len() as u32).to_be_bytes();
        stream.write_all(&len).await?;
        stream.write_all(&payload).await?;

        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let resp_len = u32::from_be_bytes(len_buf) as usize;
        let mut resp_payload = vec![0u8; resp_len];
        stream.read_exact(&mut resp_payload).await?;

        let response: Response = bincode::deserialize(&resp_payload)?;
        print_response(&response);
    }

    Ok(())
}

fn parse_command(input: &str) -> Option<Command> {
    let parts: Vec<&str> = input.splitn(3, ' ').collect();
    match parts[0].to_uppercase().as_str() {
        "GET" if parts.len() == 2 => Some(Command::Get {
            key: parts[1].to_string(),
        }),
        "SET" if parts.len() >= 3 => Some(Command::Set {
            key: parts[1].to_string(),
            value: parts[2].as_bytes().to_vec(),
        }),
        "DELETE" if parts.len() == 2 => Some(Command::Delete {
            key: parts[1].to_string(),
        }),
        "PING" => Some(Command::Ping),
        "INFO" => Some(Command::Info),
        _ => None,
    }
}

fn print_response(response: &Response) {
    match response {
        Response::Value(Some(v)) => {
            if let Ok(s) = String::from_utf8(v.clone()) {
                println!("VALUE {}", s);
            } else {
                println!("VALUE ({} bytes)", v.len());
            }
        }
        Response::Value(None) => println!("(nil)"),
        Response::Ok => println!("OK"),
        Response::Error(msg) => println!("ERROR {}", msg),
        Response::Pong => println!("PONG"),
        Response::Info {
            node_id,
            address,
            keys,
            uptime_secs,
        } => {
            println!("node_id:    {}", node_id);
            println!("address:    {}", address);
            println!("keys:       {}", keys);
            println!("uptime:     {}s", uptime_secs);
        }
    }
}
