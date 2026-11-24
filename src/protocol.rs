//! Wire protocol for QR-Transfer.
//!
//! Every packet is serialised to compact JSON (short field names to save QR
//! capacity) and then turned into a QR code.
//!
//! Stop-and-wait flow:
//!
//!   Sender                       Receiver
//!   ──────────────────────────────────────
//!   [INIT QR]  ──────────────►  save metadata
//!              ◄──────────────  [INIT_ACK QR]
//!   [DATA(0)]  ──────────────►  store chunk 0
//!              ◄──────────────  [ACK(0)]
//!   [DATA(1)]  ──────────────►  store chunk 1
//!              ◄──────────────  [ACK(1)]
//!      …                          …
//!   [DATA(n)]  ──────────────►  store chunk n, assemble, verify, save
//!              ◄──────────────  [FIN]
//!   done ✓

use serde::{Deserialize, Serialize};

// ── Tunables ──────────────────────────────────────────────────────────────────

/// Raw bytes per chunk before base64 encoding.
///
/// base64(128) = 172 chars  +  JSON envelope ≈ 196 bytes.
/// That fits comfortably in QR version 10 at EC level M (max 213 bytes),
/// which renders as ~65 columns × 33 rows in the terminal (including quiet zone)
/// — large enough for reliable webcam focus on any standard laptop terminal.
///
/// Increase cautiously via `--chunk-size`: larger chunks reduce round trips
/// but produce denser QR codes that need wider terminals and sharper focus.
pub const CHUNK_SIZE: usize = 128;

/// How long to wait for a response before retransmitting the same QR (ms).
pub const RETRANSMIT_MS: u64 = 5_000;

/// Polling granularity inside the recv loop (ms).
pub const POLL_MS: u64 = 50;

// ── Packet enum ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "t")]
pub enum Packet {
    /// Sender → Receiver: announce a new file transfer.
    #[serde(rename = "I")]
    Init {
        /// Original filename (basename only)
        #[serde(rename = "f")]
        filename: String,
        /// Total file size in bytes
        #[serde(rename = "z")]
        total_size: u64,
        /// Total number of chunks
        #[serde(rename = "n")]
        chunk_count: u32,
        /// SHA-256 hex digest of the complete file (verified after assembly)
        #[serde(rename = "c")]
        checksum: String,
    },

    /// Receiver → Sender: INIT received, please start streaming chunks.
    #[serde(rename = "IA")]
    InitAck,

    /// Sender → Receiver: one chunk of payload.
    #[serde(rename = "D")]
    Data {
        /// 0-indexed sequence number
        #[serde(rename = "s")]
        seq: u32,
        /// base64-encoded raw bytes
        #[serde(rename = "d")]
        data: String,
    },

    /// Receiver → Sender: chunk `seq` was stored successfully; send `seq + 1`.
    #[serde(rename = "A")]
    Ack {
        #[serde(rename = "s")]
        seq: u32,
    },

    /// Receiver → Sender: chunk `seq` was rejected or is missing; retransmit.
    #[serde(rename = "N")]
    Nak {
        #[serde(rename = "s")]
        seq: u32,
    },

    /// Receiver → Sender: all chunks received, file verified and saved.
    #[serde(rename = "F")]
    Fin {
        /// Path where the file was written
        #[serde(rename = "p")]
        saved_path: String,
    },

    /// Either side: fatal error, transfer aborted.
    #[serde(rename = "E")]
    Error {
        #[serde(rename = "m")]
        msg: String,
    },
}

impl Packet {
    pub fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn from_json(s: &str) -> anyhow::Result<Self> {
        Ok(serde_json::from_str(s)?)
    }
}
