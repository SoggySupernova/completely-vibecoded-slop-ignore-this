//! Central role: scans for other nodes advertising our custom service UUID,
//! connects to them, and bridges their characteristic's write/notify IO
//! into the mesh as another link. This is the complement of peripheral.rs:
//! together they let a node simultaneously accept incoming connections
//! *and* initiate outgoing ones, which is what a flood mesh needs.
//!
//! IMPORTANT: only one physical BLE connection can exist between any two
//! addresses at a time. We use `PeerRegistry::we_should_initiate` so that,
//! for any given pair of nodes, only one side ever dials out -- see
//! peer_registry.rs for why this matters.

use crate::gatt_ids::{CHAT_CHAR_UUID, SERVICE_UUID};
use crate::mesh::Mesh;
use crate::peer_registry::PeerRegistry;
use bluer::{Adapter, Address, AdapterEvent, Device};
use futures::{pin_mut, StreamExt};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub async fn run(adapter: Adapter, mesh: Arc<Mesh>, registry: Arc<PeerRegistry>) -> bluer::Result<()> {
    let discover = adapter.discover_devices().await?;
    pin_mut!(discover);

    while let Some(evt) = discover.next().await {
        if let AdapterEvent::DeviceAdded(addr) = evt {
            if !registry.we_should_initiate(addr) {
                // The peer's address wins the tie-break; it's responsible
                // for connecting to us instead. Don't even look at it.
                continue;
            }
            let device = match adapter.device(addr) {
                Ok(d) => d,
                Err(_) => continue,
            };
            let mesh = mesh.clone();
            let registry = registry.clone();
            tokio::spawn(async move {
                if let Err(e) = try_bridge_device(device, addr, mesh, registry.clone()).await {
                    eprintln!("[central] connection to {addr} failed: {e}");
                    registry.release(addr).await;
                }
            });
        }
    }

    Ok(())
}

async fn try_bridge_device(
    device: Device,
    addr: Address,
    mesh: Arc<Mesh>,
    registry: Arc<PeerRegistry>,
) -> bluer::Result<()> {
    let uuids = device.uuids().await?.unwrap_or_default();
    if !uuids.contains(&SERVICE_UUID) {
        return Ok(());
    }

    // Claim this address before touching the radio. If something else
    // (a duplicate DeviceAdded event, or the peripheral side) already has
    // an active link to this address, back off instead of racing it.
    if !registry.claim(addr).await {
        return Ok(());
    }

    if !device.is_connected().await? {
        device.connect().await?;
    }

    let mut target_char = None;
    for service in device.services().await? {
        if service.uuid().await? == SERVICE_UUID {
            for ch in service.characteristics().await? {
                if ch.uuid().await? == CHAT_CHAR_UUID {
                    target_char = Some(ch);
                }
            }
        }
    }
    let characteristic = match target_char {
        Some(c) => c,
        None => {
            registry.release(addr).await;
            return Ok(());
        }
    };

    let write_io = characteristic.write_io().await?;
    let notify_io = characteristic.notify_io().await?;

    let handle = mesh.register_link().await;
    let link_id = handle.id;
    println!("[central] link established with {addr} (link {link_id})");

    // Reader task: notifications from the peer -> mesh.
    let mesh_r = mesh.clone();
    let mut notify_io = notify_io;
    let reader_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 512];
        loop {
            match notify_io.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => mesh_r.handle_incoming(link_id, &buf[..n]).await,
                Err(e) => {
                    eprintln!("[central] notify read error on link {link_id}: {e}");
                    break;
                }
            }
        }
    });

    // Writer task: mesh broadcasts -> written to the peer.
    let mut write_io = write_io;
    let mut outgoing_rx = handle.outgoing_rx;
    let writer_task = tokio::spawn(async move {
        while let Some(data) = outgoing_rx.recv().await {
            if let Err(e) = write_io.write_all(&data).await {
                eprintln!("[central] write error on link {link_id}: {e}");
                break;
            }
        }
    });

    let _ = reader_task.await;
    let _ = writer_task.await;

    println!("[central] link with {addr} (link {link_id}) closed");
    mesh.unregister_link(link_id).await;
    registry.release(addr).await;

    Ok(())
}
