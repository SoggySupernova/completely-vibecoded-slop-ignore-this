mod central;
mod gatt_ids;
mod mesh;
mod peripheral;
mod protocol;

use clap::Parser;
use mesh::{Mesh, UiEvent};
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Parser, Debug)]
#[command(name = "blemesh", about = "Simple BLE mesh chat (flood + de-dup, no encryption)")]
struct Args {
    /// Display name shown to other nodes. Defaults to a random Guest-#### name.
    #[arg(short, long)]
    name: Option<String>,
}

#[tokio::main]
async fn main() -> bluer::Result<()> {
    let args = Args::parse();
    let nickname = args.name.unwrap_or_else(|| {
        let n: u16 = rand::random::<u16>() % 10000;
        format!("Guest-{:04}", n)
    });

    println!("== BLE Mesh Chat ==");
    println!("Nickname : {}", nickname);
    println!("Model    : flood-all-connections with de-duplication, no encryption");
    println!("Type a message and press Enter to send. Ctrl+D to quit.");
    println!();

    let session = bluer::Session::new().await?;
    let adapter = session.default_adapter().await?;
    adapter.set_powered(true).await?;
    println!(
        "Using adapter {} ({})",
        adapter.name(),
        adapter.address().await?
    );

    let (mesh, mut ui_rx) = Mesh::new(nickname.clone());

    // UI printer task: prints incoming chat messages and link-count changes.
    tokio::spawn(async move {
        while let Some(evt) = ui_rx.recv().await {
            match evt {
                UiEvent::Message { sender, text } => {
                    println!("[{}] {}", sender, text);
                }
                UiEvent::LinkCountChanged(n) => {
                    println!("(mesh links: {})", n);
                }
            }
        }
    });

    // Peripheral role: advertise + serve GATT so others can connect to us.
    let peripheral_adapter = adapter.clone();
    let peripheral_mesh = mesh.clone();
    let peripheral_name = format!("blemesh-{}", nickname);
    tokio::spawn(async move {
        if let Err(e) = peripheral::run(peripheral_adapter, peripheral_mesh, peripheral_name).await
        {
            eprintln!("peripheral role stopped: {e}");
        }
    });

    // Central role: scan + connect to other nodes' peripheral side.
    let central_adapter = adapter.clone();
    let central_mesh = mesh.clone();
    tokio::spawn(async move {
        if let Err(e) = central::run(central_adapter, central_mesh).await {
            eprintln!("central role stopped: {e}");
        }
    });

    // Chat input loop: read lines from stdin, flood each as a new message.
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let text = line.trim();
        if text.is_empty() {
            continue;
        }
        mesh.send_local(text).await;
    }

    Ok(())
}
