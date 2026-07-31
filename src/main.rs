mod central;
mod gatt_ids;
mod mesh;
mod peer_registry;
mod peripheral;
mod protocol;

use clap::Parser;
use mesh::{Mesh, UiEvent};
use peer_registry::PeerRegistry;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Parser, Debug)]
#[command(name = "blemesh", about = "Simple BLE mesh chat (flood + de-dup, no encryption)")]
struct Args {
    /// Display name shown to other nodes. Defaults to a random Guest-#### name.
    #[arg(short, long)]
    name: Option<String>,

    /// Disable the peripheral role (no advertising, no GATT server). Useful
    /// for isolating whether your adapter can't hold a stable central
    /// connection while also advertising -- a common hardware/firmware
    /// limitation on cheaper Bluetooth controllers.
    #[arg(long)]
    no_peripheral: bool,

    /// Disable the central role (no scanning, no outgoing connections).
    #[arg(long)]
    no_central: bool,
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

    // Register a permissive agent so BlueZ auto-accepts pairing/service
    // authorization instead of failing or falling back to a desktop's
    // interactive agent. We explicitly implement every handler (rather
    // than leaving them all `None`, which only gives a NoInputNoOutput
    // agent) because some peers insist on Numeric Comparison pairing,
    // which a NoInputNoOutput agent can't complete at all -- it needs a
    // Display+YesNo-capable agent, even if that agent (as here) just
    // blindly accepts without actually displaying/checking anything.
    // This app has no encryption or bonding trust model to begin with, so
    // auto-accepting is the correct behavior here, not a shortcut.
    let _agent_handle = session
        .register_agent(bluer::agent::Agent {
            request_default: true,
            request_confirmation: Some(Box::new(|req| {
                Box::pin(async move {
                    println!(
                        "[agent] auto-confirming passkey {:06} for {}",
                        req.passkey, req.device
                    );
                    Ok(())
                })
            })),
            request_authorization: Some(Box::new(|req| {
                Box::pin(async move {
                    println!("[agent] auto-authorizing pairing with {}", req.device);
                    Ok(())
                })
            })),
            authorize_service: Some(Box::new(|req| {
                Box::pin(async move {
                    println!(
                        "[agent] auto-authorizing service {} for {}",
                        req.service, req.device
                    );
                    Ok(())
                })
            })),
            request_passkey: Some(Box::new(|req| {
                Box::pin(async move {
                    println!("[agent] providing dummy passkey for {}", req.device);
                    Ok(0)
                })
            })),
            display_passkey: Some(Box::new(|req| {
                Box::pin(async move {
                    println!(
                        "[agent] (ignoring) displaying passkey {:06} for {}",
                        req.passkey, req.device
                    );
                    Ok(())
                })
            })),
            ..Default::default()
        })
        .await?;
    println!("Registered permissive pairing agent (no real encryption/bonding trust model)");

    let adapter = session.default_adapter().await?;
    adapter.set_powered(true).await?;
    let our_addr = adapter.address().await?;
    println!("Using adapter {} ({})", adapter.name(), our_addr);

    let registry = Arc::new(PeerRegistry::new(our_addr));
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
    if args.no_peripheral {
        println!("Peripheral role disabled (--no-peripheral): not advertising, no GATT server.");
    } else {
        let peripheral_adapter = adapter.clone();
        let peripheral_mesh = mesh.clone();
        let peripheral_registry = registry.clone();
        let peripheral_name = format!("blemesh-{}", nickname);
        tokio::spawn(async move {
            if let Err(e) = peripheral::run(
                peripheral_adapter,
                peripheral_mesh,
                peripheral_registry,
                peripheral_name,
            )
            .await
            {
                eprintln!("peripheral role stopped: {e}");
            }
        });
    }

    // Central role: scan + connect to other nodes' peripheral side.
    if args.no_central {
        println!("Central role disabled (--no-central): not scanning, no outgoing connections.");
    } else {
        let central_adapter = adapter.clone();
        let central_mesh = mesh.clone();
        let central_registry = registry.clone();
        tokio::spawn(async move {
            if let Err(e) = central::run(central_adapter, central_mesh, central_registry).await {
                eprintln!("central role stopped: {e}");
            }
        });
    }

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
