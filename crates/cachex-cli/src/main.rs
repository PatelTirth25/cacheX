use tokio::net::TcpStream;
use std::io::{self, Write};

#[tokio::main]
async fn main() -> Result<(), Box> {
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

        // TODO:
        // 1. Parse string input into cachex_protocol::Command
        // 2. Serialize to bincode
        // 3. Send length-prefixed bytes over `stream`
        // 4. Read length-prefixed response
        // 5. Deserialize to cachex_protocol::Response and print
    }

    Ok(())
}
