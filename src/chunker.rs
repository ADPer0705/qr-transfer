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
    /// Read the file, compute checksum, and split into chunks of `CHUNK_SIZE` bytes.
    #[allow(dead_code)]
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        Self::load_with_chunk_size(path, CHUNK_SIZE)
    }

    /// Read the file, compute checksum, and split into chunks of `chunk_size` bytes.
    pub fn load_with_chunk_size(path: &Path, chunk_size: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(chunk_size > 0, "chunk_size must be greater than 0");
        let bytes = std::fs::read(path)?;
        let total_size = bytes.len() as u64;

        let checksum = {
            let mut h = Sha256::new();
            h.update(&bytes);
            hex::encode(h.finalize())
        };

        let chunks: Vec<Vec<u8>> = bytes.chunks(chunk_size).map(|c| c.to_vec()).collect();
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_file(name: &str, contents: &[u8]) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("qrt_chunker_test_{}_{}", std::process::id(), nanos));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn multi_chunk_roundtrip() {
        let payload: Vec<u8> = (0u8..=255).cycle().take(400).collect();
        let path = temp_file("multi.bin", &payload);

        let chunker = FileChunker::load_with_chunk_size(&path, 128).unwrap();
        assert_eq!(chunker.total_size, 400);
        assert_eq!(chunker.chunk_count, 4);

        let mut asm = FileAssembler::new(
            chunker.filename.clone(),
            chunker.total_size,
            chunker.chunk_count,
            chunker.checksum.clone(),
        );
        assert!(!asm.is_complete());

        for seq in 0..chunker.chunk_count {
            let b64 = chunker.chunk_b64(seq).unwrap();
            assert!(asm.store(seq, &b64).unwrap());
        }
        assert!(asm.is_complete());
        assert_eq!(asm.assemble().unwrap(), payload);

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn duplicate_out_of_range_corrupt_and_checksum_errors() {
        let payload = b"hello qr-transfer world!".to_vec();
        let path = temp_file("check.txt", &payload);
        let chunker = FileChunker::load_with_chunk_size(&path, 10).unwrap();

        let mut asm = FileAssembler::new(
            chunker.filename.clone(),
            chunker.total_size,
            chunker.chunk_count,
            chunker.checksum.clone(),
        );

        let b0 = chunker.chunk_b64(0).unwrap();
        assert!(asm.store(0, &b0).unwrap());
        assert_eq!(asm.received, 1);

        // Duplicate chunk should return Ok(false) without incrementing received
        assert!(!asm.store(0, &b0).unwrap());
        assert_eq!(asm.received, 1);

        // Out-of-range seq should fail
        assert!(asm.store(chunker.chunk_count, &b0).is_err());

        // Corrupt base64 should fail
        assert!(asm.store(1, "!!!not-valid-base64!!!").is_err());

        // Store remaining chunks, then verify checksum mismatch is caught
        for seq in 1..chunker.chunk_count {
            let b = chunker.chunk_b64(seq).unwrap();
            asm.store(seq, &b).unwrap();
        }
        assert!(asm.assemble().is_ok());

        let mut bad_asm = FileAssembler::new(
            chunker.filename.clone(),
            chunker.total_size,
            chunker.chunk_count,
            "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        );
        for seq in 0..chunker.chunk_count {
            let b = chunker.chunk_b64(seq).unwrap();
            bad_asm.store(seq, &b).unwrap();
        }
        assert!(bad_asm.assemble().is_err());

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn empty_file_roundtrip() {
        let path = temp_file("empty.txt", b"");
        let chunker = FileChunker::load_with_chunk_size(&path, 128).unwrap();
        assert_eq!(chunker.total_size, 0);
        assert_eq!(chunker.chunk_count, 0);
        assert!(chunker.chunk_b64(0).is_none());

        let asm = FileAssembler::new(
            chunker.filename.clone(),
            chunker.total_size,
            chunker.chunk_count,
            chunker.checksum.clone(),
        );
        assert!(asm.is_complete());
        assert_eq!(asm.assemble().unwrap(), Vec::<u8>::new());

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}
