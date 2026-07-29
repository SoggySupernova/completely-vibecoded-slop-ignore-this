//! Peripheral role: advertises our custom service and runs a GATT server so
//! *other* nodes' central role can find and connect to us. Each connected
//! central becomes one link in the mesh.

use crate::gatt_ids::{CHAT_CHAR_UUID, SERVICE_UUID};
use crate::mesh::Mesh;
use crate::peer_registry::PeerRegistry;
use bluer::adv::Advertisement;
use bluer::gatt::local::{
    characteristic_control, Application, Characteristic, CharacteristicControlEvent,
    CharacteristicNotify, CharacteristicNotifyMethod, CharacteristicWrite, CharacteristicWriteMethod,
    Service,
};
use bluer::gatt::{CharacteristicReader, CharacteristicWriter};
use bluer::{Adapter, Address};
use futures::{pin_mut, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncReadExt;

/// Tracks the reader/writer IO for one connected central, and the mesh
/// LinkId once we've registered it.
struct PeerConn {
    link_id: Option<crate::mesh::LinkId>,
}

pub async fn run(
    adapter: Adapter,
    mesh: Arc<Mesh>,
    registry: Arc<PeerRegistry>,
    local_name: String,
) -> bluer::Result<()> {
    let le_advertisement = Advertisement {
        service_uuids: vec![SERVICE_UUID].into_iter().collect(),
        discoverable: Some(true),
        local_name: Some(local_name),
        ..Default::default()
    };
    let _adv_handle = adapter.advertise(le_advertisement).await?;

    let (char_control, char_handle) = characteristic_control();
    let app = Application {
        services: vec![Service {
            uuid: SERVICE_UUID,
            primary: true,
            characteristics: vec![Characteristic {
                uuid: CHAT_CHAR_UUID,
                write: Some(CharacteristicWrite {
                    write_without_response: true,
                    method: CharacteristicWriteMethod::Io,
                    ..Default::default()
                }),
                notify: Some(CharacteristicNotify {
                    notify: true,
                    method: CharacteristicNotifyMethod::Io,
                    ..Default::default()
                }),
                control_handle: char_handle,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let _app_handle = adapter.serve_gatt_application(app).await?;

    // Keep the advertisement/app handles alive for the lifetime of this
    // task by holding them in scope until the loop below exits.
    pin_mut!(char_control);
    let mut peers: HashMap<Address, PeerConn> = HashMap::new();

    loop {
        match char_control.next().await {
            Some(CharacteristicControlEvent::Write(req)) => {
                let addr = req.device_address();
                let reader = match req.accept() {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let link_id = ensure_link(&mesh, &registry, &mut peers, addr).await;
                spawn_reader_task(mesh.clone(), registry.clone(), addr, link_id, reader);
            }
            Some(CharacteristicControlEvent::Notify(writer)) => {
                let addr = writer.device_address();
                let link_id = ensure_link(&mesh, &registry, &mut peers, addr).await;
                spawn_writer_task(mesh.clone(), registry.clone(), addr, link_id, writer);
            }
            None => break,
        }
    }

    Ok(())
}

/// Returns the LinkId for a device, registering a new mesh link the first
/// time we see this address (via either a Write or Notify event).
async fn ensure_link(
    mesh: &Arc<Mesh>,
    registry: &Arc<PeerRegistry>,
    peers: &mut HashMap<Address, PeerConn>,
    addr: Address,
) -> crate::mesh::LinkId {
    if let Some(peer) = peers.get(&addr) {
        if let Some(id) = peer.link_id {
            return id;
        }
    }
    // This should normally succeed: by the tie-break rule in
    // peer_registry.rs, only peers with a *lower* address should ever be
    // dialing in to us. If claim() unexpectedly fails here it means our
    // own central role already opened an outgoing link to this same
    // address -- log it since it indicates the tie-break didn't hold
    // (e.g. both sides raced before either had discovered the other).
    if !registry.claim(addr).await {
        eprintln!(
            "[peripheral] warning: {addr} connected to us but we already have an active link to it"
        );
    }
    let handle = mesh.register_link().await;
    let id = handle.id;
    println!("[peripheral] link established with {addr} (link {id})");
    peers.insert(addr, PeerConn { link_id: Some(id) });
    // Stash the outgoing receiver under this LinkId. It's picked up by
    // spawn_writer_task once the Notify event arrives for this device (the
    // Write and Notify events for the same central can arrive in either
    // order, so we can't hand it off directly here).
    STASH.insert(id, handle.outgoing_rx).await;
    id
}

// A small global stash mapping LinkId -> outgoing receiver, needed because
// ensure_link (called from both Write and Notify branches) can't hand the
// receiver to a writer task that may not exist yet.
static STASH: once_stash::Stash = once_stash::Stash::new();

mod once_stash {
    use crate::mesh::LinkId;
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    pub struct Stash(Mutex<Option<HashMap<LinkId, tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>>>>);

    impl Stash {
        pub const fn new() -> Self {
            Stash(Mutex::const_new(None))
        }

        pub async fn insert(
            &self,
            id: LinkId,
            rx: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
        ) {
            let mut guard = self.0.lock().await;
            guard.get_or_insert_with(HashMap::new).insert(id, rx);
        }

        pub async fn take(
            &self,
            id: LinkId,
        ) -> Option<tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>> {
            let mut guard = self.0.lock().await;
            guard.as_mut().and_then(|m| m.remove(&id))
        }
    }
}

fn spawn_reader_task(
    mesh: Arc<Mesh>,
    registry: Arc<PeerRegistry>,
    addr: Address,
    link_id: crate::mesh::LinkId,
    mut reader: CharacteristicReader,
) {
    tokio::spawn(async move {
        let mut buf = vec![0u8; 512];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break, // stream closed
                Ok(n) => mesh.handle_incoming(link_id, &buf[..n]).await,
                Err(e) => {
                    eprintln!("[peripheral] read error on link {link_id} ({addr}): {e}");
                    break;
                }
            }
        }
        println!("[peripheral] read side of link {link_id} ({addr}) closed");
        mesh.unregister_link(link_id).await;
        registry.release(addr).await;
    });
}

fn spawn_writer_task(
    mesh: Arc<Mesh>,
    registry: Arc<PeerRegistry>,
    addr: Address,
    link_id: crate::mesh::LinkId,
    mut writer: CharacteristicWriter,
) {
    tokio::spawn(async move {
        let mut rx = match STASH.take(link_id).await {
            Some(rx) => rx,
            None => return,
        };
        use tokio::io::AsyncWriteExt;
        while let Some(data) = rx.recv().await {
            if let Err(e) = writer.write_all(&data).await {
                eprintln!("[peripheral] write error on link {link_id} ({addr}): {e}");
                break;
            }
        }
        println!("[peripheral] write side of link {link_id} ({addr}) closed");
        mesh.unregister_link(link_id).await;
        registry.release(addr).await;
    });
}
