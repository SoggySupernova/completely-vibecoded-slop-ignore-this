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
//!
//! Every BlueZ D-Bus call we make (Connect, resolving services, acquiring
//! the write/notify IO) is wrapped in a timeout. Without this, a wedged
//! D-Bus call or an out-of-range/uncooperative peer can hang a connection
//! attempt indefinitely with no useful diagnostic beyond "Timeout waiting
//! for reply" -- wrapping it ourselves gives a clear, per-stage error and
//! lets us retry instead of leaving the task stuck forever.

use crate::gatt_ids::{CHAT_CHAR_UUID, SERVICE_UUID};
use crate::mesh::Mesh;
use crate::peer_registry::PeerRegistry;
use bluer::{Adapter, Address, AdapterEvent, Device, Error, ErrorKind};
use futures::{pin_mut, StreamExt};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

/// How long we'll wait on any single BlueZ D-Bus call before giving up on
/// this connection attempt and retrying later.
const OP_TIMEOUT: Duration = Duration::from_secs(12);

/// How long to wait before retrying a device we failed to bridge, or
/// reconnecting one whose link closed normally.
const RETRY_BACKOFF: Duration = Duration::from_secs(5);

enum BridgeOutcome {
    /// Device doesn't advertise our service; not a peer, stop retrying it.
    NotAPeer,
    /// We had a working link and it closed (peer went away, etc).
    LinkClosed,
}

pub async fn run(adapter: Adapter, mesh: Arc<Mesh>, registry: Arc<PeerRegistry>) -> bluer::Result<()> {
    // Addresses we've already spawned a persistent retry-loop task for.
    // (Separate from the registry's "active link" claim -- this just
    // prevents spawning a second retry loop for the same address.)
    let mut loop_spawned: HashSet<Address> = HashSet::new();

    let discover = adapter.discover_devices().await?;
    pin_mut!(discover);

    while let Some(evt) = discover.next().await {
        if let AdapterEvent::DeviceAdded(addr) = evt {
            if !registry.we_should_initiate(addr) {
                // The peer's address wins the tie-break; it connects to us.
                continue;
            }
            if !loop_spawned.insert(addr) {
                continue; // already have a retry loop running for this address
            }
            println!("[central] discovered {addr}, we will initiate (address tie-break)");
            let adapter = adapter.clone();
            let mesh = mesh.clone();
            let registry = registry.clone();
            tokio::spawn(async move {
                connect_retry_loop(adapter, addr, mesh, registry).await;
            });
        }
    }

    Ok(())
}

/// Keeps trying to bridge `addr` into the mesh, with backoff, until it
/// turns out not to be one of our peers at all.
async fn connect_retry_loop(adapter: Adapter, addr: Address, mesh: Arc<Mesh>, registry: Arc<PeerRegistry>) {
    loop {
        if !registry.claim(addr).await {
            // Peripheral side already has (or is establishing) a link to
            // this address -- nothing for us to do right now.
            println!("[central] {addr} already linked via peripheral side, will re-check later");
            tokio::time::sleep(RETRY_BACKOFF).await;
            continue;
        }

        let device = match adapter.device(addr) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("[central] couldn't get device handle for {addr}: {e}");
                registry.release(addr).await;
                tokio::time::sleep(RETRY_BACKOFF).await;
                continue;
            }
        };

        let mut give_up = false;
        match try_bridge_device(&device, addr, mesh.clone()).await {
            Ok(BridgeOutcome::NotAPeer) => {
                registry.release(addr).await;
                give_up = true;
            }
            Ok(BridgeOutcome::LinkClosed) => {
                println!("[central] link with {addr} closed normally, will retry in {RETRY_BACKOFF:?}");
                registry.release(addr).await;
            }
            Err(e) => {
                eprintln!("[central] connection to {addr} failed: {e}, will retry in {RETRY_BACKOFF:?}");
                registry.release(addr).await;
            }
        }

        // Force a clean slate before retrying (or giving up). This
        // matters especially after a timeout: our client-side timeout
        // only stops us waiting, it doesn't cancel the underlying D-Bus
        // call, so an AcquireWrite / AcquireNotify can still complete on
        // BlueZ's side after we've given up on it. Left alone, that
        // orphans an acquired GATT resource that the next attempt then
        // collides with ("already acquired" / "not permitted").
        // Disconnecting forces BlueZ to release everything tied to this
        // connection regardless of what state our side thinks it's in.
        if device.is_connected().await.unwrap_or(false) {
            println!("[central] disconnecting {addr} to clear any GATT state");
            if let Err(e) = device.disconnect().await {
                eprintln!("[central] disconnect of {addr} failed: {e}");
            }
        }

        if give_up {
            return; // don't keep retrying something that isn't one of our peers
        }

        tokio::time::sleep(RETRY_BACKOFF).await;
    }
}

fn timeout_err(stage: &str) -> Error {
    Error {
        kind: ErrorKind::Failed,
        message: format!("timed out after {OP_TIMEOUT:?} waiting for {stage}"),
    }
}

async fn try_bridge_device(device: &Device, addr: Address, mesh: Arc<Mesh>) -> bluer::Result<BridgeOutcome> {
    let uuids = device.uuids().await?.unwrap_or_default();
    if !uuids.contains(&SERVICE_UUID) {
        return Ok(BridgeOutcome::NotAPeer);
    }
    println!("[central] {addr} advertises our service");

    if !device.is_connected().await? {
        println!("[central] {addr} connecting...");
        timeout(OP_TIMEOUT, device.connect())
            .await
            .map_err(|_| timeout_err("Connect()"))??;
    }
    println!("[central] {addr} connected, resolving GATT services...");

    let services = timeout(OP_TIMEOUT, device.services())
        .await
        .map_err(|_| timeout_err("service resolution"))??;

    let mut target_char = None;
    for service in services {
        if service.uuid().await? == SERVICE_UUID {
            for ch in service.characteristics().await? {
                if ch.uuid().await? == CHAT_CHAR_UUID {
                    target_char = Some(ch);
                }
            }
        }
    }
    let characteristic = match target_char {
        Some(c) => {
            println!("[central] {addr} found chat characteristic");
            c
        }
        None => {
            eprintln!("[central] {addr} advertised our service but characteristic wasn't found");
            return Ok(BridgeOutcome::NotAPeer);
        }
    };

    let write_io = timeout(OP_TIMEOUT, characteristic.write_io())
        .await
        .map_err(|_| timeout_err("write_io()"))??;
    println!("[central] {addr} write IO ready (mtu={})", write_io.mtu());

    let notify_io = timeout(OP_TIMEOUT, characteristic.notify_io())
        .await
        .map_err(|_| timeout_err("notify_io()"))??;
    println!("[central] {addr} notify IO ready (mtu={})", notify_io.mtu());

    let handle = mesh.register_link().await;
    let link_id = handle.id;
    println!("[central] {addr} mesh link {link_id} established");

    // Reader task: notifications from the peer -> mesh.
    let mesh_r = mesh.clone();
    let mut notify_io = notify_io;
    let reader_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 512];
        loop {
            match notify_io.read(&mut buf).await {
                Ok(0) => {
                    println!("[central] {addr} notify stream ended (link {link_id})");
                    break;
                }
                Ok(n) => mesh_r.handle_incoming(link_id, &buf[..n]).await,
                Err(e) => {
                    eprintln!("[central] {addr} notify read error on link {link_id}: {e}");
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
                eprintln!("[central] {addr} write error on link {link_id}: {e}");
                break;
            }
        }
    });

    let _ = reader_task.await;
    let _ = writer_task.await;

    println!("[central] link with {addr} (link {link_id}) closed");
    mesh.unregister_link(link_id).await;

    Ok(BridgeOutcome::LinkClosed)
}
