//! Prevents the central and peripheral roles from independently opening
//! two physical BLE connections to the same peer address.
//!
//! A single BLE controller can only maintain one LE ACL connection per
//! peer address. If our central role connects out to a device *and* that
//! same device's central role connects to our peripheral side around the
//! same time, both connection attempts race on the underlying radio --
//! this is what produces the "sometimes ATT 0x0e, sometimes GATT services
//! not resolved, sometimes instant disconnect" grab-bag of errors.
//!
//! The fix: for any given pair of devices, exactly one side should be the
//! initiator. We use a simple deterministic tie-break (lower Bluetooth
//! address initiates) so both nodes agree on who connects to whom without
//! needing to talk to each other first.

use bluer::Address;
use std::collections::HashSet;
use tokio::sync::{watch, Mutex};

pub struct PeerRegistry {
    our_addr: Address,
    active: Mutex<HashSet<Address>>,
    scan_pause_count: Mutex<u32>,
    scan_allowed_tx: watch::Sender<bool>,
}

impl PeerRegistry {
    pub fn new(our_addr: Address) -> Self {
        let (scan_allowed_tx, _rx) = watch::channel(true);
        PeerRegistry {
            our_addr,
            active: Mutex::new(HashSet::new()),
            scan_pause_count: Mutex::new(0),
            scan_allowed_tx,
        }
    }

    /// True if *we* are the one who should dial out to `peer` when both
    /// sides can see each other. The other side will see `false` for the
    /// same pair and should stay passive (peripheral-only) for it.
    pub fn we_should_initiate(&self, peer: Address) -> bool {
        self.our_addr < peer
    }

    /// Try to claim `peer` as having an active mesh link. Returns `true`
    /// if this is a new claim (caller should proceed), `false` if a link
    /// to this address is already active (caller should back off).
    pub async fn claim(&self, peer: Address) -> bool {
        self.active.lock().await.insert(peer)
    }

    pub async fn release(&self, peer: Address) {
        self.active.lock().await.remove(&peer);
    }

    /// Request that active scanning be paused. Reference-counted: nested
    /// pause/resume calls (e.g. from multiple simultaneous connection
    /// attempts) are safe -- scanning only resumes once every outstanding
    /// pause has been released.
    ///
    /// This exists because a single BLE radio holding an active scan while
    /// also trying to establish/maintain a fresh connection is a common
    /// source of instability on real hardware: the connection can be
    /// dropped in the first fraction of a second, before any GATT traffic
    /// even happens, because the scan window steals radio time the new
    /// connection needs to complete its initial supervision handshake.
    pub async fn pause_scanning(&self) {
        let mut count = self.scan_pause_count.lock().await;
        *count += 1;
        if *count == 1 {
            let _ = self.scan_allowed_tx.send(false);
        }
    }

    pub async fn resume_scanning(&self) {
        let mut count = self.scan_pause_count.lock().await;
        if *count > 0 {
            *count -= 1;
        }
        if *count == 0 {
            let _ = self.scan_allowed_tx.send(true);
        }
    }

    pub fn subscribe_scan_allowed(&self) -> watch::Receiver<bool> {
        self.scan_allowed_tx.subscribe()
    }
}
