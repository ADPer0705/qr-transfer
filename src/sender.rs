//! Sender state machine.
//!
//! Protocol from the sender's perspective:
//!
//!   Phase::Init
//!     Show INIT QR ──► wait for InitAck
//!     On timeout    ──► retransmit INIT QR
//!
//!   Phase::Data(seq)
//!     Show DATA(seq) QR ──► wait for Ack(seq)
//!     On Nak(seq) / timeout ──► retransmit DATA(seq)
//!     On Ack(seq) ──► advance to Data(seq+1) or WaitFin
//!
//!   Phase::WaitFin
//!     Show nothing; wait for Fin from receiver.

use crate::{
    chunker::FileChunker,
    display,
    protocol::{Packet, POLL_MS, RETRANSMIT_MS},
    scanner,
};
use anyhow::{bail, Result};
use std::{path::PathBuf, sync::mpsc::RecvTimeoutError, time::{Duration, Instant}};

#[derive(Debug)]
enum Phase {
    Init,
    Data(u32),
    WaitFin,
}

pub fn run(file: PathBuf, camera_index: u32) -> Result<()> {
    // ── Load file ─────────────────────────────────────────────────────────────
    let chunker = FileChunker::load(&file)?;
    eprintln!(
        "  Loaded '{}' — {} bytes in {} chunk(s)  [SHA-256: {}…]",
        chunker.filename,
        chunker.total_size,
        chunker.chunk_count,
        &chunker.checksum[..12],
    );

    // ── Start camera scanner ──────────────────────────────────────────────────
    eprintln!("  Opening camera {} …", camera_index);
    let rx = scanner::spawn(camera_index)?;
    eprintln!("  Camera ready. Starting transfer.\n");

    // ── Pre-serialise the INIT packet (never changes) ─────────────────────────
    let init_json = Packet::Init {
        filename:    chunker.filename.clone(),
        total_size:  chunker.total_size,
        chunk_count: chunker.chunk_count,
        checksum:    chunker.checksum.clone(),
    }
    .to_json()?;

    // ── State machine ─────────────────────────────────────────────────────────
    let mut phase = Phase::Init;
    let mut retries: u32 = 0;

    loop {
        // ── Build + display QR for current phase ──────────────────────────────
        match &phase {
            Phase::Init => {
                display::qr(
                    &init_json,
                    &[
                        "▶  INIT — broadcasting file metadata",
                        &format!("   file      : '{}'", chunker.filename),
                        &format!("   size      : {} bytes", chunker.total_size),
                        &format!("   chunks    : {}", chunker.chunk_count),
                        &format!("   retry #{}", retries),
                        "",
                        "   Point the receiver's camera at this screen.",
                    ],
                )?;
            }

            Phase::Data(seq) => {
                let data = chunker.chunk_b64(*seq).expect("seq in range");
                let pkt  = Packet::Data { seq: *seq, data }.to_json()?;
                display::qr(
                    &pkt,
                    &[
                        &format!(
                            "▶  DATA  chunk {}/{}  (retry #{})",
                            seq + 1,
                            chunker.chunk_count,
                            retries,
                        ),
                        &display::progress(seq + 1, chunker.chunk_count),
                    ],
                )?;
            }

            Phase::WaitFin => {
                display::info(&[
                    "⏳  All chunks sent — waiting for receiver to verify and confirm.",
                ])?;
            }
        }

        // ── Poll for response with retransmit timeout ─────────────────────────
        let deadline = Instant::now() + Duration::from_millis(RETRANSMIT_MS);
        let mut advanced = false;

        'poll: loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                // Timeout — retransmit (outer loop re-displays same QR)
                retries += 1;
                break 'poll;
            }

            let wait = remaining.min(Duration::from_millis(POLL_MS));
            match rx.recv_timeout(wait) {
                // ── Happy path ────────────────────────────────────────────────
                Ok(Packet::InitAck) => {
                    if matches!(phase, Phase::Init) {
                        eprintln!("  ✓ INIT acknowledged");
                        phase   = Phase::Data(0);
                        retries = 0;
                        advanced = true;
                        break 'poll;
                    }
                }

                Ok(Packet::Ack { seq: acked }) => {
                    if let Phase::Data(current) = phase {
                        if acked == current {
                            let next = current + 1;
                            if next < chunker.chunk_count {
                                eprintln!("  ✓ ACK chunk {}", acked);
                                phase   = Phase::Data(next);
                                retries = 0;
                            } else {
                                eprintln!("  ✓ ACK chunk {} (last)", acked);
                                phase = Phase::WaitFin;
                            }
                            advanced = true;
                            break 'poll;
                        }
                    }
                }

                // ── Fin arrives (receiver confirmed) ──────────────────────────
                Ok(Packet::Fin { saved_path }) => {
                    display::info(&[
                        "🎉  Transfer complete!",
                        &format!("   Receiver saved the file as: {}", saved_path),
                        &format!("   SHA-256 verified ✓  ({}…)", &chunker.checksum[..16]),
                    ])?;
                    return Ok(());
                }

                // ── NAK → force immediate retransmit ──────────────────────────
                Ok(Packet::Nak { seq }) => {
                    eprintln!("  ✗ NAK for chunk {}", seq);
                    retries += 1;
                    break 'poll;
                }

                Ok(Packet::Error { msg }) => {
                    bail!("Receiver reported an error: {}", msg);
                }

                // Stale / unexpected packets from the other session — ignore
                Ok(_) => {}

                Err(RecvTimeoutError::Timeout) => {} // keep polling
                Err(RecvTimeoutError::Disconnected) => {
                    bail!("Scanner thread disconnected unexpectedly");
                }
            }
        }

        // Give the other side time to display its response QR if we just
        // advanced — avoids the camera catching a blank screen mid-transition.
        if advanced {
            std::thread::sleep(Duration::from_millis(300));
        }
    }
}
