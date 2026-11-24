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
    display::{self, DisplaySink, TerminalDisplay},
    protocol::{Packet, POLL_MS, RETRANSMIT_MS},
    scanner,
};
use anyhow::{bail, Result};
use std::{
    path::PathBuf,
    sync::mpsc::{self, RecvTimeoutError},
    time::{Duration, Instant},
};

#[derive(Debug)]
enum Phase {
    Init,
    Data(u32),
    WaitFin,
}

pub fn run(file: PathBuf, camera_index: u32, chunk_size: usize) -> Result<()> {
    // ── Load file ─────────────────────────────────────────────────────────────
    let chunker = FileChunker::load_with_chunk_size(&file, chunk_size)?;
    eprintln!(
        "  Loaded '{}' — {} bytes in {} chunk(s)  [SHA-256: {}…]",
        chunker.filename,
        chunker.total_size,
        chunker.chunk_count,
        &chunker.checksum[..12],
    );

    // ── Start camera scanner ──────────────────────────────────────────────────
    eprintln!("  Opening camera {} …", camera_index);
    let (rx, _frame_count) = scanner::spawn(camera_index)?;
    eprintln!("  Camera ready. Starting transfer.\n");

    let mut display = TerminalDisplay;
    run_with_io(
        chunker,
        &rx,
        &mut display,
        Duration::from_millis(RETRANSMIT_MS),
        Duration::from_millis(300),
    )
}

pub(crate) fn run_with_io<D: DisplaySink>(
    chunker: FileChunker,
    rx: &mpsc::Receiver<Packet>,
    display: &mut D,
    retransmit: Duration,
    advance_delay: Duration,
) -> Result<()> {
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
                display.show_qr(
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
                display.show_qr(
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
                display.show_info(&[
                    "⏳  All chunks sent — waiting for receiver to verify and confirm.",
                ])?;
            }
        }

        // ── Poll for response with retransmit timeout ─────────────────────────
        let deadline = Instant::now() + retransmit;
        let mut advanced = false;

        'poll: loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                // Timeout — retransmit (outer loop updates retry counter on screen)
                retries += 1;
                break 'poll;
            }

            let wait = remaining.min(Duration::from_millis(POLL_MS));
            match rx.recv_timeout(wait) {
                // ── Happy path ────────────────────────────────────────────────
                Ok(Packet::InitAck) => {
                    if matches!(phase, Phase::Init) {
                        phase = if chunker.chunk_count == 0 {
                            Phase::WaitFin
                        } else {
                            Phase::Data(0)
                        };
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
                                phase   = Phase::Data(next);
                                retries = 0;
                            } else {
                                phase = Phase::WaitFin;
                            }
                            advanced = true;
                            break 'poll;
                        }
                    }
                }

                // ── Fin arrives (receiver confirmed) ──────────────────────────
                Ok(Packet::Fin { saved_path }) => {
                    display.show_info(&[
                        "🎉  Transfer complete!",
                        &format!("   Receiver saved the file as: {}", saved_path),
                        &format!("   SHA-256 verified ✓  ({}…)", &chunker.checksum[..16]),
                    ])?;
                    return Ok(());
                }

                // ── NAK → rewind only if receiver asks for a different chunk ──
                Ok(Packet::Nak { seq }) => {
                    if let Phase::Data(current) = phase {
                        if seq < chunker.chunk_count && seq != current {
                            phase = Phase::Data(seq);
                            retries += 1;
                            break 'poll;
                        }
                    }
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
        if advanced && !advance_delay.is_zero() {
            std::thread::sleep(advance_delay);
        }
    }
}
