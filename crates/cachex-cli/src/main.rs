use std::io::{self, Write};

use cachex_cli::{format_response, parse_command};
use cachex_protocol::Response;
use tokio::net::TcpStream;

const DEFAULT_ADDR: &str = "127.0.0.1:7000";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("CACHEX_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());
    let mut stream = TcpStream::connect(&addr).await?;
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

        match parse_command(input) {
            Some(command) => {
                cachex_protocol::write_framed(&mut stream, &command).await?;
                let response: Response = cachex_protocol::read_framed(&mut stream).await?;
                println!("{}", format_response(&response));
            }
            None => {
                eprintln!("Unknown command.");
                print_usage();
            }
        }
    }

    Ok(())
}

fn print_usage() {
    println!("Usage:");
    println!("  GET <key>");
    println!("  SET <key> <value> [TTL]");
    println!("    TTL formats: 30s, 5m, 2h, or plain seconds");
    println!("  DELETE <key>");
    println!("  PING");
    println!("  INFO");
    println!("  exit");
}
