//! Central role: scans for other nodes advertising our custom service UUID,
//! connects to them, and bridges their characteristic's write/notify IO
//! into the mesh as another link. This is the complement of peripheral.rs:
//! together they let a node simultaneously accept incoming connections
//! *and* initiate outgoing ones, which is what a flood mesh needs.

use crate::gatt_ids::{CHAT_CHAR_UUID, SERVICE_UUID};
use crate::mesh::Mesh;
use bluer::{Adapter, Address, AdapterEvent, Device};
use futures::{pin_mut, StreamExt};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub async fn run(adapter: Adapter, mesh: Arc<Mesh>) -> bluer::Result<()> {
    // Tracks addresses we've already bridged into the mesh, so we don't
    // open duplicate connections/links to the same peer.
    let connected: Arc<Mutex<HashSet<Address>>> = Arc::new(Mutex::new(HashSet::new()));

    let discover = adapter.discover_devices().await?;
    pin_mut!(discover);

    while let Some(evt) = discover.next().await {
        if let AdapterEvent::DeviceAdded(addr) = evt {
            let already = connected.lock().unwrap().contains(&addr);
            if already {
                continue;
            }
            let device = match adapter.device(addr) {
                Ok(d) => d,
                Err(_) => continue,
            };
            let mesh = mesh.clone();
            let connected = connected.clone();
            tokio::spawn(async move {
                if let Err(_e) = try_bridge_device(device, addr, mesh, connected).await {
                    // Connection attempt failed or device didn't have our
                    // service; nothing to do, it may show up again later.
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
    connected: Arc<Mutex<HashSet<Address>>>,
) -> bluer::Result<()> {
    let uuids = device.uuids().await?.unwrap_or_default();
    if !uuids.contains(&SERVICE_UUID) {
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
        None => return Ok(()),
    };

    // Mark as connected only once we've actually found the characteristic,
    // so a device without our service doesn't permanently block retries.
    {
        let mut set = connected.lock().unwrap();
        if set.contains(&addr) {
            return Ok(()); // a concurrent task beat us to it
        }
        set.insert(addr);
    }

    let write_io = characteristic.write_io().await?;
    let notify_io = characteristic.notify_io().await?;

    let handle = mesh.register_link().await;
    let link_id = handle.id;

    // Reader task: notifications from the peer -> mesh.
    let mesh_r = mesh.clone();
    let mut notify_io = notify_io;
    let reader_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 512];
        loop {
            match notify_io.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => mesh_r.handle_incoming(link_id, &buf[..n]).await,
                Err(_) => break,
            }
        }
    });

    // Writer task: mesh broadcasts -> written to the peer.
    let mesh_w = mesh.clone();
    let mut write_io = write_io;
    let mut outgoing_rx = handle.outgoing_rx;
    let writer_task = tokio::spawn(async move {
        while let Some(data) = outgoing_rx.recv().await {
            if write_io.write_all(&data).await.is_err() {
                break;
            }
        }
        let _ = mesh_w; // kept alive for symmetry / future use
    });

    let _ = reader_task.await;
    let _ = writer_task.await;

    mesh.unregister_link(link_id).await;
    connected.lock().unwrap().remove(&addr);

    Ok(())
}
