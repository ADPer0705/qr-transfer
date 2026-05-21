//! Receiver state machine.
//!
//! Protocol from the receiver's perspective:
//!
//!   Phase 1 — WaitInit
//!     Scan until a valid INIT packet arrives.
//!     Show INIT_ACK QR so the sender can advance.
//!     (If sender retransmits INIT, re-show INIT_ACK.)
//!
//!   Phase 2 — ReceiveChunks(expected_seq)
//!     Scan for DATA(expected_seq).
//!     On arrival → store chunk, show ACK(seq).
//!     On duplicate (seq < expected) → re-show last ACK.
//!     On skip (seq > expected) → show NAK(expected).
//!     On INIT retransmit → re-show INIT_ACK.
//!     On timeout → re-show last ACK/NAK.
//!
//!   Phase 3 — Done
//!     Assemble, verify, save file.
//!     Show FIN QR.

use crate::{
    chunker::FileAssembler,
    display,
    protocol::{Packet, POLL_MS, RETRANSMIT_MS},
    scanner,
};
use anyhow::{bail, Result};
use std::{path::{PathBuf, Path}, sync::mpsc::RecvTimeoutError, time::{Duration, Instant}};

pub fn run(output_dir: PathBuf, camera_index: u32) -> Result<()> {
    std::fs::create_dir_all(&output_dir)?;

    eprintln!("  Opening camera {} …", camera_index);
    let rx = scanner::spawn(camera_index)?;
    eprintln!("  Camera ready.\n");

    // ── Phase 1: Wait for INIT ────────────────────────────────────────────────
    display::info(&[
        "👂  Waiting for INIT from sender.",
        "   Point the sender's screen at this camera.",
    ])?;

    let mut assembler: FileAssembler = loop {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(Packet::Init { filename, total_size, chunk_count, checksum }) => {
                eprintln!(
                    "  ✓ INIT received: '{}' ({} bytes, {} chunks)",
                    filename, total_size, chunk_count
                );
                break FileAssembler::new(filename, total_size, chunk_count, checksum);
            }
            Ok(_) | Err(RecvTimeoutError::Timeout) => {} // keep scanning
            Err(RecvTimeoutError::Disconnected) => bail!("Scanner disconnected"),
        }
    };

    // ── Show INIT_ACK ─────────────────────────────────────────────────────────
    let init_ack_json = Packet::InitAck.to_json()?;
    
    // Extract a cloned copy of the filename to give to the closure
    let filename_for_display = assembler.filename.clone();
    
    // Make a photocopy of the json string for the closure to own
    let json_for_display = init_ack_json.clone();
    
    let show_init_ack = move |extra: &str| -> anyhow::Result<()> {
        display::qr(
            &json_for_display, // Use the cloned copy here
            &[
                &format!("◀  INIT_ACK — ready to receive '{}'", filename_for_display),
                extra,
                "   Point this screen at the sender's camera.",
            ],
        )
    };
    show_init_ack("")?;

    // ── Phase 2: Receive chunks ───────────────────────────────────────────────
    let mut expected_seq: u32 = 0;

    // The last response QR we showed — kept so we can re-display on timeout.
    let mut last_response_json = init_ack_json.clone();
    let mut last_response_lines: Vec<String> = vec![
        format!("◀  INIT_ACK — ready to receive '{}'", assembler.filename),
        "".to_string(),
        "   Point this screen at the sender's camera.".to_string(),
    ];

    loop {
        let deadline = Instant::now() + Duration::from_millis(RETRANSMIT_MS);

        'poll: loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                // Timeout — re-show last response so the sender can re-scan it
                let refs: Vec<&str> = last_response_lines.iter().map(|s| s.as_str()).collect();
                display::qr(&last_response_json, &refs)?;
                break 'poll;
            }

            let wait = remaining.min(Duration::from_millis(POLL_MS));
            match rx.recv_timeout(wait) {
                // ── INIT retransmit ───────────────────────────────────────────
                Ok(Packet::Init { .. }) => {
                    // Sender didn't see our INIT_ACK — re-show it.
                    show_init_ack("(re-showing after sender retransmit)")?;
                    last_response_json  = init_ack_json.clone();
                    last_response_lines = vec![
                        format!("◀  INIT_ACK — ready to receive '{}'", assembler.filename),
                        "(re-showing after sender retransmit)".to_string(),
                        "   Point this screen at the sender's camera.".to_string(),
                    ];
                    break 'poll;
                }

                // ── Data chunk ────────────────────────────────────────────────
                Ok(Packet::Data { seq, data }) => {
                    if seq == expected_seq {
                        match assembler.store(seq, &data) {
                            Ok(_) => {
                                eprintln!(
                                    "  ✓ chunk {}/{} stored",
                                    seq + 1, assembler.chunk_count
                                );
                            }
                            Err(e) => {
                                // Corrupt chunk (bad base64 etc.) — NAK it.
                                eprintln!("  ✗ chunk {} corrupt: {}", seq, e);
                                let nak = Packet::Nak { seq: expected_seq }.to_json()?;
                                display::qr(
                                    &nak,
                                    &[&format!("◀  NAK {} — chunk corrupt, please retransmit", seq)],
                                )?;
                                last_response_json  = nak;
                                last_response_lines = vec![
                                    format!("◀  NAK {} — chunk corrupt, please retransmit", seq),
                                ];
                                break 'poll;
                            }
                        }

                        // ── All chunks received? ───────────────────────────────
                        if assembler.is_complete() {
                            return finish(assembler, &output_dir, &rx);
                        }

                        // ACK this chunk
                        expected_seq += 1;
                        let ack = Packet::Ack { seq }.to_json()?;
                        let status = vec![
                            format!("◀  ACK {} — chunk received", seq),
                            display::progress(seq + 1, assembler.chunk_count),
                            "   Point this screen at the sender's camera.".to_string(),
                        ];
                        let refs: Vec<&str> = status.iter().map(|s| s.as_str()).collect();
                        display::qr(&ack, &refs)?;
                        last_response_json  = ack;
                        last_response_lines = status;
                        break 'poll;

                    } else if seq < expected_seq {
                        // Duplicate — sender didn't see our ACK; re-show it.
                        eprintln!("  ↩ duplicate chunk {}, re-sending ACK", seq);
                        let refs: Vec<&str> = last_response_lines.iter().map(|s| s.as_str()).collect();
                        display::qr(&last_response_json, &refs)?;
                        break 'poll;

                    } else {
                        // Out-of-order chunk — NAK the one we actually need.
                        eprintln!(
                            "  ✗ got chunk {} but expected {} — sending NAK",
                            seq, expected_seq
                        );
                        let nak = Packet::Nak { seq: expected_seq }.to_json()?;
                        let status = vec![
                            format!("◀  NAK {} — expected chunk {}", seq, expected_seq),
                        ];
                        let refs: Vec<&str> = status.iter().map(|s| s.as_str()).collect();
                        display::qr(&nak, &refs)?;
                        last_response_json  = nak;
                        last_response_lines = status;
                        break 'poll;
                    }
                }

                Ok(Packet::Error { msg }) => {
                    bail!("Sender reported an error: {}", msg);
                }

                Ok(_) | Err(RecvTimeoutError::Timeout) => {} // keep scanning
                Err(RecvTimeoutError::Disconnected) => bail!("Scanner disconnected"),
            }
        }
    }
}

/// Assemble + verify + save the file, then display the FIN QR.
fn finish(
    assembler: FileAssembler,
    output_dir: &Path,
    rx: &std::sync::mpsc::Receiver<Packet>,
) -> Result<()> {
    display::info(&["🔧  All chunks received — assembling and verifying…"])?;

    let bytes = assembler.assemble()?; // SHA-256 verified inside

    // Avoid clobbering an existing file
    let mut out_path = output_dir.join(&assembler.filename);
    if out_path.exists() {
        let stem = out_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("file");
        let ext = out_path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| format!(".{}", e))
            .unwrap_or_default();
        out_path = output_dir.join(format!("{}_recv{}", stem, ext));
    }
    std::fs::write(&out_path, &bytes)?;

    let saved_path = out_path.to_string_lossy().to_string();
    eprintln!("  ✅ File saved: {}", saved_path);

    let fin = Packet::Fin { saved_path: saved_path.clone() }.to_json()?;

    // Hold FIN on screen until the sender scans it (or 30 s elapse)
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        display::qr(
            &fin,
            &[
                "◀  FIN — file received, verified, and saved!",
                &format!("   saved to : {}", saved_path),
                &format!("   SHA-256  : {}…", &assembler.expected_checksum[..16]),
                "",
                "   Waiting for sender to scan this QR…",
            ],
        )?;

        if std::time::Instant::now() > deadline {
            break;
        }

        // Re-show every 2 s in case the sender missed the QR
        match rx.recv_timeout(Duration::from_secs(2)) {
            Ok(Packet::Data { .. }) => {
                // Sender still thinks it's sending — keep showing FIN
            }
            _ => {}
        }
    }

    display::info(&["🎉  Done! You can close this terminal."])?;
    Ok(())
}
