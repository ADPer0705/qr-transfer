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
    display::{self, DisplaySink, TerminalDisplay},
    protocol::{Packet, POLL_MS},
    scanner,
};
use anyhow::{bail, Result};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    mpsc::{self, RecvTimeoutError},
};
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

pub fn run(output_dir: PathBuf, camera_index: u32) -> Result<()> {
    std::fs::create_dir_all(&output_dir)?;

    eprintln!("  Opening camera {} …", camera_index);
    let (rx, frame_count) = scanner::spawn(camera_index)?;
    eprintln!("  Camera ready.\n");

    let mut display = TerminalDisplay;
    run_with_io(
        output_dir,
        &rx,
        Some(&frame_count),
        &mut display,
        Duration::from_secs(30),
    )
}

pub(crate) fn run_with_io<D: DisplaySink>(
    output_dir: PathBuf,
    rx: &mpsc::Receiver<Packet>,
    frame_count: Option<&AtomicU64>,
    display: &mut D,
    fin_hold: Duration,
) -> Result<()> {
    std::fs::create_dir_all(&output_dir)?;

    // ── Phase 1: Wait for INIT ────────────────────────────────────────────────

    let mut assembler: FileAssembler = loop {
        let n = frame_count
            .map(|fc| fc.load(Ordering::Relaxed))
            .unwrap_or(0);
        display.show_info(&[
            "👂  Waiting for INIT from sender.",
            &format!("   📷 scanning... (frame #{})", n),
            "   Point the sender's screen at this camera.",
        ])?;

        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(Packet::Init { filename, total_size, chunk_count, checksum }) => {
                break FileAssembler::new(filename, total_size, chunk_count, checksum);
            }
            Ok(_) | Err(RecvTimeoutError::Timeout) => {} // keep scanning
            Err(RecvTimeoutError::Disconnected) => bail!("Scanner disconnected"),
        }
    };

    // Empty (0-byte) files have no DATA chunks — verify and show FIN right away.
    if assembler.is_complete() {
        return finish(assembler, &output_dir, rx, display, fin_hold);
    }

    // ── Show INIT_ACK ─────────────────────────────────────────────────────────
    let init_ack_json = Packet::InitAck.to_json()?;
    let init_ack_lines = [
        format!("◀  INIT_ACK — ready to receive '{}'", assembler.filename),
        String::new(),
        "   Point this screen at the sender's camera.".to_string(),
    ];
    let init_ack_refs: Vec<&str> = init_ack_lines.iter().map(|s| s.as_str()).collect();
    display.show_qr(&init_ack_json, &init_ack_refs)?;

    // ── Phase 2: Receive chunks ───────────────────────────────────────────────
    let mut expected_seq: u32 = 0;

    // Keep track of what QR is currently displayed on screen so repeated scans
    // of the same sender QR never cause a 30 FPS clear/redraw flicker storm.
    let mut last_shown_qr = init_ack_json.clone();
    let mut last_ack_json = init_ack_json.clone();
    let mut last_ack_lines: Vec<String> = init_ack_lines.to_vec();

    loop {
        match rx.recv_timeout(Duration::from_millis(POLL_MS)) {
            // ── INIT retransmit ───────────────────────────────────────────────
            Ok(Packet::Init { .. }) => {
                // Only redraw if screen isn't already showing INIT_ACK.
                if last_shown_qr != init_ack_json {
                    let refs: Vec<&str> = init_ack_lines.iter().map(|s| s.as_str()).collect();
                    display.show_qr(&init_ack_json, &refs)?;
                    last_shown_qr = init_ack_json.clone();
                }
            }

            // ── Data chunk ────────────────────────────────────────────────────
            Ok(Packet::Data { seq, data }) => {
                if seq == expected_seq {
                    match assembler.store(seq, &data) {
                        Ok(_) => {}
                        Err(e) => {
                            // Corrupt chunk (bad base64 etc.) — NAK it (once).
                            let nak = Packet::Nak { seq: expected_seq }.to_json()?;
                            if last_shown_qr != nak {
                                let msg = format!(
                                    "◀  NAK {} — chunk corrupt ({}), please retransmit",
                                    seq, e
                                );
                                display.show_qr(&nak, &[&msg])?;
                                last_shown_qr = nak;
                            }
                            continue;
                        }
                    }

                    // ── All chunks received? ───────────────────────────────────
                    if assembler.is_complete() {
                        return finish(assembler, &output_dir, rx, display, fin_hold);
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
                    display.show_qr(&ack, &refs)?;
                    last_shown_qr = ack.clone();
                    last_ack_json = ack;
                    last_ack_lines = status;
                } else if seq < expected_seq {
                    // Duplicate — sender didn't see our ACK; only redraw if needed.
                    if last_shown_qr != last_ack_json {
                        let refs: Vec<&str> = last_ack_lines.iter().map(|s| s.as_str()).collect();
                        display.show_qr(&last_ack_json, &refs)?;
                        last_shown_qr = last_ack_json.clone();
                    }
                } else {
                    // Out-of-order chunk — NAK the one we actually need (once).
                    let nak = Packet::Nak { seq: expected_seq }.to_json()?;
                    if last_shown_qr != nak {
                        let msg = format!("◀  NAK {} — expected chunk {}", expected_seq, expected_seq);
                        display.show_qr(&nak, &[&msg])?;
                        last_shown_qr = nak;
                    }
                }
            }

            Ok(Packet::Error { msg }) => {
                bail!("Sender reported an error: {}", msg);
            }

            Ok(_) | Err(RecvTimeoutError::Timeout) => {} // keep QR steady on screen
            Err(RecvTimeoutError::Disconnected) => bail!("Scanner disconnected"),
        }
    }
}

fn sanitize_filename(raw: &str) -> &str {
    Path::new(raw)
        .file_name()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty() && *s != "." && *s != "..")
        .unwrap_or("received_file")
}

fn unique_output_path(output_dir: &Path, filename: &str) -> PathBuf {
    let safe_name = sanitize_filename(filename);
    let out_path = output_dir.join(safe_name);
    if !out_path.exists() {
        return out_path;
    }

    let stem = out_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("file");
    let ext = out_path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| format!(".{}", e))
        .unwrap_or_default();

    let first = output_dir.join(format!("{}_recv{}", stem, ext));
    if !first.exists() {
        return first;
    }

    let mut counter = 2u32;
    loop {
        let candidate = output_dir.join(format!("{}_recv_{}{}", stem, counter, ext));
        if !candidate.exists() {
            return candidate;
        }
        counter += 1;
    }
}

/// Assemble + verify + save the file, then display the FIN QR.
fn finish<D: DisplaySink>(
    assembler: FileAssembler,
    output_dir: &Path,
    rx: &mpsc::Receiver<Packet>,
    display: &mut D,
    fin_hold: Duration,
) -> Result<()> {
    display.show_info(&["🔧  All chunks received — assembling and verifying…"])?;

    let bytes = match assembler.assemble() {
        Ok(b) => b,
        Err(e) => {
            let msg = e.to_string();
            if let Ok(err_json) = (Packet::Error { msg: msg.clone() }).to_json() {
                let _ = display.show_qr(
                    &err_json,
                    &[
                        "❌  Verification failed!",
                        &format!("   {}", msg),
                    ],
                );
                let hold = fin_hold.min(Duration::from_secs(5));
                let deadline = Instant::now() + hold;
                while Instant::now() < deadline {
                    let rem = deadline.saturating_duration_since(Instant::now());
                    match rx.recv_timeout(rem.min(Duration::from_millis(100))) {
                        Err(RecvTimeoutError::Disconnected) => break,
                        _ => {}
                    }
                }
            }
            return Err(e);
        }
    };

    let out_path = unique_output_path(output_dir, &assembler.filename);
    std::fs::write(&out_path, &bytes)?;

    let saved_path = out_path.to_string_lossy().to_string();
    let fin = Packet::Fin { saved_path: saved_path.clone() }.to_json()?;

    // Display FIN QR once and hold rock-solid until fin_hold elapses
    display.show_qr(
        &fin,
        &[
            "◀  FIN — file received, verified, and saved!",
            &format!("   saved to : {}", saved_path),
            &format!("   SHA-256  : {}…", &assembler.expected_checksum[..16]),
            "",
            "   Waiting for sender to scan this QR…",
        ],
    )?;

    let deadline = Instant::now() + fin_hold;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let wait = remaining.min(Duration::from_millis(100));
        match rx.recv_timeout(wait) {
            Err(RecvTimeoutError::Disconnected) => break,
            _ => {}
        }
    }

    display.show_info(&["🎉  Done! You can close this terminal."])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{chunker::FileChunker, sender};
    use image::{GrayImage, Luma};
    use qrcode::{EcLevel, QrCode};
    use std::sync::{
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
        Arc,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    fn render_qr_luma(json: &str, scale: u32) -> GrayImage {
        let code = QrCode::with_error_correction_level(json.as_bytes(), EcLevel::M).unwrap();
        let quiet = 4u32;
        let mods = code.width() as u32;
        let size = (mods + quiet * 2) * scale;
        let mut img = GrayImage::from_pixel(size, size, Luma([255u8]));
        for my in 0..mods {
            for mx in 0..mods {
                if code[(mx as usize, my as usize)] == qrcode::Color::Dark {
                    for dy in 0..scale {
                        for dx in 0..scale {
                            img.put_pixel(
                                (mx + quiet) * scale + dx,
                                (my + quiet) * scale + dy,
                                Luma([0u8]),
                            );
                        }
                    }
                }
            }
        }
        img
    }

    /// Simulated optical channel: encodes displayed JSON to a QR image and
    /// decodes it via `scanner::decode_packet_from_luma` before delivering to peer.
    struct OpticalDisplay {
        peer_tx: mpsc::Sender<Packet>,
        qr_redraws: Arc<AtomicUsize>,
    }

    impl DisplaySink for OpticalDisplay {
        fn show_qr(&mut self, data: &str, _status: &[&str]) -> Result<()> {
            self.qr_redraws.fetch_add(1, AtomicOrdering::SeqCst);
            let img = render_qr_luma(data, 4);
            let pkt = scanner::decode_packet_from_luma(&img)
                .expect("optical loopback failed to decode displayed QR");
            let _ = self.peer_tx.send(pkt);
            Ok(())
        }

        fn show_info(&mut self, _lines: &[&str]) -> Result<()> {
            Ok(())
        }
    }

    fn temp_workspace(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("qrt_e2e_{}_{}_{}", tag, std::process::id(), nanos));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn run_optical_loopback(
        src_path: &Path,
        out_dir: &Path,
        chunk_size: usize,
    ) -> (usize, usize) {
        let (s_to_r_tx, s_to_r_rx) = mpsc::channel::<Packet>();
        let (r_to_s_tx, r_to_s_rx) = mpsc::channel::<Packet>();

        let s_redraws = Arc::new(AtomicUsize::new(0));
        let r_redraws = Arc::new(AtomicUsize::new(0));

        let mut s_disp = OpticalDisplay {
            peer_tx: s_to_r_tx,
            qr_redraws: s_redraws.clone(),
        };
        let mut r_disp = OpticalDisplay {
            peer_tx: r_to_s_tx,
            qr_redraws: r_redraws.clone(),
        };

        let chunker = FileChunker::load_with_chunk_size(src_path, chunk_size).unwrap();
        let out_dir_clone = out_dir.to_path_buf();

        let recv_handle = std::thread::spawn(move || {
            run_with_io(
                out_dir_clone,
                &s_to_r_rx,
                None,
                &mut r_disp,
                Duration::from_millis(80),
            )
        });

        let send_handle = std::thread::spawn(move || {
            sender::run_with_io(
                chunker,
                &r_to_s_rx,
                &mut s_disp,
                Duration::from_millis(250),
                Duration::ZERO,
            )
        });

        send_handle.join().unwrap().unwrap();
        recv_handle.join().unwrap().unwrap();

        (
            s_redraws.load(AtomicOrdering::SeqCst),
            r_redraws.load(AtomicOrdering::SeqCst),
        )
    }

    #[test]
    fn e2e_optical_loopback_demo_txt() {
        let work = temp_workspace("demo");
        let out_dir = work.join("out");
        let demo_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("demo.txt");

        run_optical_loopback(&demo_path, &out_dir, crate::protocol::CHUNK_SIZE);

        let expected = std::fs::read(&demo_path).unwrap();
        let actual = std::fs::read(out_dir.join("demo.txt")).unwrap();
        assert_eq!(actual, expected);

        let _ = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn e2e_optical_loopback_binary_1000_bytes() {
        let work = temp_workspace("bin1000");
        let src_path = work.join("payload.bin");
        let out_dir = work.join("out");
        let payload: Vec<u8> = (0..1000).map(|i| ((i * 37 + 13) % 256) as u8).collect();
        std::fs::write(&src_path, &payload).unwrap();

        let (s_redraws, r_redraws) = run_optical_loopback(&src_path, &out_dir, 128);
        assert_eq!(std::fs::read(out_dir.join("payload.bin")).unwrap(), payload);
        // 1 INIT + 8 DATA = 9 sender QRs; 1 INIT_ACK + 7 ACKs + 1 FIN = 9 receiver QRs
        assert_eq!(s_redraws, 9);
        assert_eq!(r_redraws, 9);

        let _ = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn e2e_optical_loopback_empty_file() {
        let work = temp_workspace("empty");
        let src_path = work.join("empty.dat");
        let out_dir = work.join("out");
        std::fs::write(&src_path, b"").unwrap();

        run_optical_loopback(&src_path, &out_dir, 128);
        assert_eq!(std::fs::read(out_dir.join("empty.dat")).unwrap(), b"");

        let _ = std::fs::remove_dir_all(&work);
    }

    #[test]
    fn receiver_does_not_flicker_on_duplicate_scans() {
        let work = temp_workspace("dup");
        let src_path = work.join("dup.txt");
        let out_dir = work.join("out");
        let payload = b"0123456789abcdef0123456789abcdef".to_vec();
        std::fs::write(&src_path, &payload).unwrap();

        let chunker = FileChunker::load_with_chunk_size(&src_path, 16).unwrap();
        let (s_to_r_tx, s_to_r_rx) = mpsc::channel::<Packet>();
        let (r_to_s_tx, r_to_s_rx) = mpsc::channel::<Packet>();

        let r_redraws = Arc::new(AtomicUsize::new(0));
        let mut r_disp = OpticalDisplay {
            peer_tx: r_to_s_tx,
            qr_redraws: r_redraws.clone(),
        };

        let out_clone = out_dir.clone();
        let recv_handle = std::thread::spawn(move || {
            run_with_io(
                out_clone,
                &s_to_r_rx,
                None,
                &mut r_disp,
                Duration::from_millis(80),
            )
        });

        let init = Packet::Init {
            filename: chunker.filename.clone(),
            total_size: chunker.total_size,
            chunk_count: chunker.chunk_count,
            checksum: chunker.checksum.clone(),
        };
        // Send INIT, wait for INIT_ACK, then spam 10 duplicate INITs
        s_to_r_tx.send(init.clone()).unwrap();
        assert_eq!(r_to_s_rx.recv_timeout(Duration::from_secs(2)).unwrap(), Packet::InitAck);
        for _ in 0..10 {
            s_to_r_tx.send(init.clone()).unwrap();
        }

        // Send chunk 0, wait for ACK(0), then spam 10 duplicate chunk 0s
        let d0 = Packet::Data { seq: 0, data: chunker.chunk_b64(0).unwrap() };
        s_to_r_tx.send(d0.clone()).unwrap();
        assert_eq!(
            r_to_s_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            Packet::Ack { seq: 0 }
        );
        for _ in 0..10 {
            s_to_r_tx.send(d0.clone()).unwrap();
        }

        // Send chunk 1 (last), expect FIN
        let d1 = Packet::Data { seq: 1, data: chunker.chunk_b64(1).unwrap() };
        s_to_r_tx.send(d1).unwrap();
        match r_to_s_rx.recv_timeout(Duration::from_secs(2)).unwrap() {
            Packet::Fin { .. } => {}
            other => panic!("expected Fin, got {:?}", other),
        }
        drop(s_to_r_tx);

        recv_handle.join().unwrap().unwrap();
        assert_eq!(std::fs::read(out_dir.join("dup.txt")).unwrap(), payload);
        // Only 3 QR renders total (INIT_ACK, ACK(0), FIN) — zero duplicate redraws!
        assert_eq!(r_redraws.load(AtomicOrdering::SeqCst), 3);

        let _ = std::fs::remove_dir_all(&work);
    }
}
