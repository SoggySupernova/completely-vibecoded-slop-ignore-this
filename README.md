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
  gatt_ids.rs     shared service/characteristic UUIDs
```
