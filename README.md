# BLE Mesh Chat (Linux)

A minimal Bluetooth LE mesh chat client for Linux, using BlueZ directly
via the `bluer` crate. No encryption, no central server — pure
**flood-all-connections with de-duplication**.

## How it works

Every node runs two BLE roles at once, both wired into one shared
mesh core (`src/mesh.rs`):

- **Peripheral** (`src/peripheral.rs`) — advertises a custom GATT service
  and accepts incoming connections from other nodes' central role.
- **Central** (`src/central.rs`) — scans for other nodes advertising that
  same service, and connects out to them.

Each BLE connection (in either direction) becomes one "link" registered
with the mesh. The mesh core:

1. On receiving bytes on any link, decodes a `Packet` (see
   `src/protocol.rs`), checks a de-dup cache keyed by the packet's random
   message ID.
2. If already seen -> silently dropped.
3. If new -> printed locally, then re-broadcast to every *other* link with
   `ttl` decremented by one (dropped once `ttl` hits 0).
4. Messages you type locally get a fresh message ID and TTL of 7 hops,
   and are flooded to all links immediately.

Packets are capped at 400 bytes so a single message always fits in one
GATT notification without needing fragmentation — a deliberate v1
simplification.

## Requirements

- Linux with BlueZ (`bluetoothd` running) and a Bluetooth adapter.
- Rust + Cargo (this was built/tested against rustc 1.75 from Ubuntu's
  apt repos; a newer toolchain works too).
- `libdbus-1-dev` and `pkg-config` (bluer talks to BlueZ over D-Bus).

```bash
sudo apt-get install libdbus-1-dev pkg-config build-essential
```

## Build & run

```bash
cargo build --release
sudo setcap 'cap_net_admin,cap_net_raw+eip' target/release/blemesh
./target/release/blemesh --name Alice
```

(`setcap` lets the binary manage the Bluetooth adapter without running
the whole thing as root; alternatively just run it with `sudo`.)

Run it on two machines (or two adapters) in range of each other, type a
message on one, and it should appear on the other within a couple of
seconds — the time it takes BlueZ to discover, connect, and negotiate
the GATT link.

## Why two nodes were racing each other

Early testing between two machines surfaced flaky, non-deterministic
errors: `ATT error 0x0e`, `"GATT services have not been resolved"`, and
links that dropped the instant a message was sent. Root cause: a BLE
radio can only hold **one** physical connection per peer address, but the
central and peripheral roles didn't know about each other — so both
nodes' central roles would try to dial out to each other at the same
time, and the two connection attempts collided on the radio.

Fix (`src/peer_registry.rs`): a deterministic tie-break — whichever node
has the lower Bluetooth address is the only one that ever dials out for
that pair; the other stays passive and simply accepts the incoming
connection on its peripheral side. Since each link already carries both
directions of traffic (write + notify on the same characteristic), one
physical connection per pair is all that's needed.

## Troubleshooting: "Notify acquired" / "not permitted" on retry

This was a real bug introduced by the timeout wrapping described above:
`tokio::time::timeout()` only makes *our* code stop waiting on a D-Bus
call, it doesn't cancel the call itself. If `AcquireNotify` (or
`AcquireWrite`) was just slow rather than genuinely stuck, it can still
succeed on BlueZ's side after we've already given up and moved on to a
retry. That leaves a real, acquired GATT resource on that characteristic
that nothing ever releases (since the `CharacteristicWriter`/`Reader` we
would've dropped to trigger cleanup never made it back to us) — so the
next attempt collides with "already acquired: not permitted".

Fixed by forcing a full `device.disconnect()` after every attempt —
success, failure, or "not a peer" — before retrying or giving up.
Disconnecting tears down the whole ACL connection, which forces BlueZ to
release any Acquire'd write/notify sessions regardless of what state our
side thinks they're in, guaranteeing a clean slate for the next attempt.

## Troubleshooting: a brief "access request" popup on the peer, then failure

This is BlueZ asking a *desktop* pairing agent (GNOME's bluetooth applet,
etc.) to authorize the connection/service — and since nobody clicks it in
the split second it's shown, BlueZ treats it as denied and the connect
attempt fails or hangs until timeout. This app now registers its own
agent (`bluer::agent::Agent` with `request_default: true` and every
handler left as default) right at startup, which makes BlueZ route these
requests to *us* instead, and — since the whole point of this app is "no
encryption, no bonding" — we accept everything automatically rather than
prompting. You should no longer see any popup at all; if you still do,
something else on the system (e.g. a second registered agent with higher
priority) may be intercepting the request first.

## Troubleshooting: D-Bus timeouts on connect

If you see `internal error: D-Bus error org.freedesktop.DBus.Error.Timeout`
on a connection attempt, that's `bluetoothd` itself not responding to a
method call (Connect, service resolution, or acquiring the write/notify
IO) within a reasonable time — not a BLE-level rejection. Common causes,
roughly in order of likelihood:

- **The peer is out of range or the signal is marginal.** LE connection
  establishment retries at the link layer before giving up; weak signal
  makes this slow and eventually times out.
- **The adapter is being asked to scan and connect at the same time.**
  Some controllers/firmware handle concurrent central-scan +
  connection-establishment poorly. If this happens consistently even at
  close range, try testing with discovery paused (temporarily comment out
  the central role) to see if connects become reliable — that would
  confirm this is your adapter's limitation rather than the app's logic.
- **A stale device object.** If BlueZ still has a cached, unreachable
  device entry, `bluetoothd` may hang trying to reconnect to it. Running
  `bluetoothctl remove <addr>` and letting discovery re-add it fresh can
  help.

The app now wraps every BlueZ call (`Connect`, service resolution,
`write_io`/`notify_io`) in a 12-second timeout and retries with a 5-second
backoff indefinitely (as long as the device keeps advertising our
service), instead of hanging on a single stuck call. Every stage also
logs on success now, not just failure — you should see a line for each
step (`connecting...`, `connected, resolving GATT services...`, `found
chat characteristic`, `write IO ready (mtu=...)`, `mesh link N
established`, etc.) so it's obvious exactly where a given attempt is
getting stuck.

## Known limitations (deliberate, for v1)

- **No encryption.** Anyone in range can read every message. This was an
  explicit "start simple" choice — add a Noise/X25519 handshake per-link
  later if you want privacy.
- **No message fragmentation.** Messages over ~350 characters get
  rejected rather than split across multiple packets.
- **Linux only.** Cross-platform mesh needs a per-OS peripheral-mode
  implementation (BlueZ's GATT-server-over-D-Bus API used here has no
  equivalent single crate on macOS/Windows) — see the discussion earlier
  in this conversation for why.
- **No connection-count limits.** In a dense area a node could end up
  central-connecting to many peripherals; a real deployment would want a
  max-links cap and maybe RSSI-based peer selection.
- **No persistent identity.** Nickname is passed on the command line and
  isn't cryptographically tied to anything, so anyone can claim any name.

## File layout

```
src/
  main.rs        entrypoint: wires everything up, stdin chat loop
  mesh.rs         transport-agnostic flood + de-dup core
  protocol.rs     packet wire format (encode/decode)
  peripheral.rs   BLE peripheral role (GATT server + advertising)
  central.rs      BLE central role (scan + connect + GATT client)
  peer_registry.rs address tie-break to prevent duplicate connections
  gatt_ids.rs     shared service/characteristic UUIDs
```
