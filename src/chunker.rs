//! File chunking (sender) and reassembly with SHA-256 verification (receiver).
//! #TODO: allow more graceful handling of large files that don't fit in memory (e.g. by streaming)

use std::path::Path;
use sha2::{Digest, Sha256};
use base64::{engine::general_purpose::STANDARD as B64, Engine};

use crate::protocol::CHUNK_SIZE;

// ── Sender side ───────────────────────────────────────────────────────────────

pub struct FileChunker {
    pub filename:    String,
    pub total_size:  u64,
    pub chunk_count: u32,
    pub checksum:    String,
    chunks:          Vec<Vec<u8>>,
}

impl FileChunker {
    /// Read the file, compute checksum, and split into chunks.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let bytes = std::fs::read(path)?;
        let total_size = bytes.len() as u64;

        let checksum = {
            let mut h = Sha256::new();
            h.update(&bytes);
            hex::encode(h.finalize())
        };

        let chunks: Vec<Vec<u8>> = bytes.chunks(CHUNK_SIZE).map(|c| c.to_vec()).collect();
        let chunk_count = chunks.len() as u32;

        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file")
            .to_string();

        Ok(Self { filename, total_size, chunk_count, checksum, chunks })
    }

    /// Returns the base64-encoded payload for chunk `seq`, or `None` if out of range.
    pub fn chunk_b64(&self, seq: u32) -> Option<String> {
        self.chunks.get(seq as usize).map(|c| B64.encode(c))
    }
}


// ── Receiver side ─────────────────────────────────────────────────────────────

pub struct FileAssembler {
    pub filename:          String,
    pub total_size:        u64,
    pub chunk_count:       u32,
    pub expected_checksum: String,
    chunks:                Vec<Option<Vec<u8>>>,
    pub received:          u32,
}

impl FileAssembler {
    pub fn new(
        filename:  String,
        total_size: u64,
        chunk_count: u32,
        checksum:  String,
    ) -> Self {
        let chunks = vec![None; chunk_count as usize];
        Self {
            filename,
            total_size,
            chunk_count,
            expected_checksum: checksum,
            chunks,
            received: 0,
        }
    }

    /// Decode base64, store the chunk. Returns `true` if it was new (not a duplicate).
    pub fn store(&mut self, seq: u32, data_b64: &str) -> anyhow::Result<bool> {
        anyhow::ensure!(
            seq < self.chunk_count,
            "seq {} out of range (total {})", seq, self.chunk_count
        );
        let raw = B64.decode(data_b64)?;
        if self.chunks[seq as usize].is_none() {
            self.chunks[seq as usize] = Some(raw);
            self.received += 1;
            Ok(true)
        } else {
            Ok(false) // duplicate — safe to re-ACK
        }
    }

    pub fn is_complete(&self) -> bool {
        self.received == self.chunk_count
    }

    /// Concatenate all chunks, verify SHA-256, return the assembled bytes.
    pub fn assemble(&self) -> anyhow::Result<Vec<u8>> {
        let mut data: Vec<u8> = Vec::with_capacity(self.total_size as usize);
        for (i, chunk) in self.chunks.iter().enumerate() {
            match chunk {
                Some(c) => data.extend_from_slice(c),
                None    => anyhow::bail!("chunk {} is missing", i),
            }
        }

        let actual = {
            let mut h = Sha256::new();
            h.update(&data);
            hex::encode(h.finalize())
        };

        anyhow::ensure!(
            actual == self.expected_checksum,
            "checksum mismatch!\n  expected: {}\n  got:      {}",
            self.expected_checksum,
            actual
        );

        Ok(data)
    }
}
