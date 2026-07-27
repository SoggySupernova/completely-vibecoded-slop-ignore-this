//! Wire format for mesh chat packets.
//!
//! Layout (all little-endian):
//!   [0..16)   msg_id       : 16 bytes, random UUID  -> used for de-dup
//!   [16]      ttl          : u8, decremented on every hop, dropped at 0
//!   [17]      sender_len   : u8
//!   [18..18+sender_len)     sender name, UTF-8
//!   next 2 bytes            payload_len : u16 LE
//!   next payload_len bytes  payload, UTF-8
//!
//! Kept deliberately simple (no encryption, no compression) per the "start
//! simple" brief. A single packet is capped at MAX_PACKET_SIZE so it fits
//! in a single BLE notification without needing fragmentation.

use uuid::Uuid;

/// Hard cap chosen to comfortably fit inside a single GATT notification
/// even at a conservative negotiated ATT MTU. Notifications (unlike
/// writes) are not auto-queued by BlueZ, so we can't rely on long-write
/// semantics for the peripheral -> central direction.
pub const MAX_PACKET_SIZE: usize = 400;

/// Default time-to-live for a freshly authored message: number of hops
/// it's allowed to be re-flooded before nodes stop forwarding it.
pub const DEFAULT_TTL: u8 = 7;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    pub msg_id: [u8; 16],
    pub ttl: u8,
    pub sender: String,
    pub payload: String,
}

#[derive(Debug)]
pub enum DecodeError {
    TooShort,
    BadSenderLen,
    BadPayloadLen,
    Utf8,
}

impl Packet {
    pub fn new(sender: &str, payload: &str) -> Self {
        Packet {
            msg_id: *Uuid::new_v4().as_bytes(),
            ttl: DEFAULT_TTL,
            sender: sender.to_string(),
            payload: payload.to_string(),
        }
    }

    pub fn msg_id_hex(&self) -> String {
        hex_encode(&self.msg_id)
    }

    /// Returns a copy of this packet with ttl decremented by one.
    /// Callers should check `ttl > 0` before deciding to forward.
    pub fn with_decremented_ttl(&self) -> Option<Packet> {
        if self.ttl == 0 {
            None
        } else {
            let mut p = self.clone();
            p.ttl -= 1;
            Some(p)
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, &'static str> {
        let sender_bytes = self.sender.as_bytes();
        let payload_bytes = self.payload.as_bytes();

        if sender_bytes.len() > 255 {
            return Err("sender name too long");
        }
        if payload_bytes.len() > u16::MAX as usize {
            return Err("payload too long");
        }

        let mut buf = Vec::with_capacity(18 + sender_bytes.len() + 2 + payload_bytes.len());
        buf.extend_from_slice(&self.msg_id);
        buf.push(self.ttl);
        buf.push(sender_bytes.len() as u8);
        buf.extend_from_slice(sender_bytes);
        buf.extend_from_slice(&(payload_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(payload_bytes);

        if buf.len() > MAX_PACKET_SIZE {
            return Err("encoded packet exceeds MAX_PACKET_SIZE");
        }
        Ok(buf)
    }

    pub fn decode(buf: &[u8]) -> Result<Packet, DecodeError> {
        if buf.len() < 18 {
            return Err(DecodeError::TooShort);
        }
        let mut msg_id = [0u8; 16];
        msg_id.copy_from_slice(&buf[0..16]);
        let ttl = buf[16];
        let sender_len = buf[17] as usize;

        let sender_start = 18;
        let sender_end = sender_start + sender_len;
        if buf.len() < sender_end + 2 {
            return Err(DecodeError::BadSenderLen);
        }
        let sender = std::str::from_utf8(&buf[sender_start..sender_end])
            .map_err(|_| DecodeError::Utf8)?
            .to_string();

        let payload_len =
            u16::from_le_bytes([buf[sender_end], buf[sender_end + 1]]) as usize;
        let payload_start = sender_end + 2;
        let payload_end = payload_start + payload_len;
        if buf.len() < payload_end {
            return Err(DecodeError::BadPayloadLen);
        }
        let payload = std::str::from_utf8(&buf[payload_start..payload_end])
            .map_err(|_| DecodeError::Utf8)?
            .to_string();

        Ok(Packet {
            msg_id,
            ttl,
            sender,
            payload,
        })
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let p = Packet::new("alice", "hello mesh");
        let enc = p.encode().unwrap();
        let dec = Packet::decode(&enc).unwrap();
        assert_eq!(p, dec);
    }

    #[test]
    fn ttl_decrement() {
        let mut p = Packet::new("bob", "hi");
        p.ttl = 1;
        let next = p.with_decremented_ttl().unwrap();
        assert_eq!(next.ttl, 0);
        assert!(next.with_decremented_ttl().is_none());
    }
}
