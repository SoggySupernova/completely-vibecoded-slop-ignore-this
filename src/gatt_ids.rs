//! Fixed, made-up 128-bit UUIDs identifying our custom mesh-chat GATT
//! service and its single characteristic. Every node advertises/looks for
//! the same service UUID so peers can find each other.

use uuid::Uuid;

pub const SERVICE_UUID: Uuid = Uuid::from_u128(0x7a1e8f00_0001_4000_8000_00805f9b34fb);
pub const CHAT_CHAR_UUID: Uuid = Uuid::from_u128(0x7a1e8f00_0002_4000_8000_00805f9b34fb);
