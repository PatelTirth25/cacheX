use std::io::{self, Write};

use cachex_cli::{format_response, parse_command};
use cachex_client::{ClusterClient, Node, PartitionerKind, parse_nodes, partitioner_from_env};

const DEFAULT_ADDR: &str = "127.0.0.1:7000";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("CACHEX_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());
    let client = if let Ok(nodes) = std::env::var("CACHEX_NODES") {
        let kind = partitioner_from_env(
            &std::env::var("CACHEX_PARTITIONER").unwrap_or_else(|_| "consistent".into()),
        )?;
        ClusterClient::new(parse_nodes(&nodes)?, kind)?
    } else {
        ClusterClient::new(
            vec![Node {
                id: "node-1".into(),
                address: addr.clone(),
            }],
            PartitionerKind::Modulo,
        )?
    };
    println!(
        "Connected to CacheX ({})",
        client
            .nodes()
            .iter()
            .map(|n| n.address.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

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
                let response = client.execute(command).await?;
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
    println!();
    println!("Cluster mode:");
    println!("  CACHEX_NODES=node-a=127.0.0.1:7001,node-b=127.0.0.1:7002");
    println!("  CACHEX_PARTITIONER=consistent|modulo (default: consistent)");
    println!("Single-node mode uses CACHEX_ADDR (default: 127.0.0.1:7000).");
}
