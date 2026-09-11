use std::io::{self, Write};

use cachex_cli::{format_response, parse_command};
use cachex_client::{ClusterClient, Node, PartitionerKind, parse_nodes, partitioner_from_env};

const DEFAULT_ADDR: &str = "127.0.0.1:7000";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_usage();
        return Ok(());
    }
    let addr = option(&args, "--addr", "CACHEX_ADDR", DEFAULT_ADDR);
    let client = if let Some(nodes) = option_optional(&args, "--nodes", "CACHEX_NODES") {
        let kind = partitioner_from_env(&option(
            &args,
            "--partitioner",
            "CACHEX_PARTITIONER",
            "consistent",
        ))?;
        ClusterClient::new_with_replication_factor(
            parse_nodes(&nodes)?,
            kind,
            option(
                &args,
                "--replication-factor",
                "CACHEX_REPLICATION_FACTOR",
                "1",
            )
            .parse()?,
        )?
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

fn option(args: &[String], flag: &str, env_name: &str, default: &str) -> String {
    option_optional(args, flag, env_name).unwrap_or_else(|| default.to_string())
}

fn option_optional(args: &[String], flag: &str, env_name: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == flag)
        .map(|pair| pair[1].clone())
        .or_else(|| std::env::var(env_name).ok())
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
    println!();
    println!("Command-line options:");
    println!("  --addr <host:port>");
    println!("  --nodes <id=addr,...>");
    println!("  --partitioner <consistent|modulo>");
    println!("  --replication-factor <1|2>");
}
