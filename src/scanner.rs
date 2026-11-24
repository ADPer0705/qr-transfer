//! Camera capture + QR decoding, running on a background thread.
//!
//! Call [`spawn`] to start the thread; it sends decoded [`Packet`]s over the
//! returned [`std::sync::mpsc::Receiver`].  Invalid frames and malformed JSON
//! are silently dropped — only well-formed packets reach the caller.

use crate::protocol::Packet;
use anyhow::Result;
use image::{DynamicImage, GrayImage};
use nokhwa::{
    pixel_format::{LumaFormat, RgbFormat},
    utils::{ApiBackend, CameraIndex, RequestedFormat, RequestedFormatType},
    Camera,
};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, mpsc,
};
use std::{thread, time::Duration};

fn try_decode_both(img: &GrayImage) -> Option<Packet> {
    for invert in [false, true] {
        let candidate = if invert {
            let mut inv = img.clone();
            image::imageops::invert(&mut inv);
            inv
        } else {
            img.clone()
        };

        let mut prepared = rqrr::PreparedImage::prepare(candidate);
        for grid in prepared.detect_grids() {
            if let Ok((_meta, content)) = grid.decode() {
                if let Ok(pkt) = Packet::from_json(&content) {
                    return Some(pkt);
                }
            }
        }
    }
    None
}

/// Attempt to detect and decode a protocol `Packet` QR code from a grayscale frame.
///
/// Tries up to 1280px width first (preserving module sharpness for dense QRs),
/// then falls back to 640px width (smaller local threshold window for close-ups
/// or uneven screen lighting). Both normal and inverted polarities are checked.
pub fn decode_packet_from_luma(gray: &GrayImage) -> Option<Packet> {
    const PRIMARY_MAX_WIDTH: u32 = 1280;
    const FALLBACK_MAX_WIDTH: u32 = 640;

    let resized_primary;
    let primary: &GrayImage = if gray.width() > PRIMARY_MAX_WIDTH {
        let scale = PRIMARY_MAX_WIDTH as f64 / gray.width() as f64;
        let h = ((gray.height() as f64 * scale).round() as u32).max(1);
        resized_primary = image::imageops::resize(
            gray,
            PRIMARY_MAX_WIDTH,
            h,
            image::imageops::FilterType::Triangle,
        );
        &resized_primary
    } else {
        gray
    };

    if let Some(pkt) = try_decode_both(primary) {
        return Some(pkt);
    }

    if primary.width() > FALLBACK_MAX_WIDTH {
        let scale = FALLBACK_MAX_WIDTH as f64 / primary.width() as f64;
        let h = ((primary.height() as f64 * scale).round() as u32).max(1);
        let fallback = image::imageops::resize(
            primary,
            FALLBACK_MAX_WIDTH,
            h,
            image::imageops::FilterType::Triangle,
        );
        if let Some(pkt) = try_decode_both(&fallback) {
            return Some(pkt);
        }
    }

    None
}

/// Print all cameras detected by the OS.
pub fn list_cameras() -> Result<()> {
    let cameras = nokhwa::query(ApiBackend::Auto)?;
    if cameras.is_empty() {
        println!("No cameras found.");
    } else {
        println!("Available cameras:");
        for c in &cameras {
            println!("  [{}]  {}", c.index(), c.human_name());
        }
    }
    Ok(())
}

/// Spawn the scanner background thread.
///
/// Opens camera `index`, BLOCKS until the camera is confirmed open (or fails),
/// then hands back a `Receiver<Packet>` the caller can poll.
///
/// The thread runs until the `Sender` half of the channel is dropped (i.e.
/// the main thread exits or `rx` is dropped).
pub fn spawn(camera_index: u32) -> Result<(mpsc::Receiver<Packet>, Arc<AtomicU64>)> {
    let (pkt_tx, pkt_rx) = mpsc::channel::<Packet>();
    let frame_count = Arc::new(AtomicU64::new(0));
    let fc = frame_count.clone();

    // sync_channel(0) = rendezvous: send blocks until recv is called.
    // Guarantees spawn() doesn't return until the camera is actually open.
    let (init_tx, init_rx) = mpsc::sync_channel::<Result<(), String>>(0);

    let index = CameraIndex::Index(camera_index);

    thread::spawn(move || {
        #[cfg(target_os = "macos")]
        nokhwa::nokhwa_initialize(|_granted| {});

        // ── Camera init ───────────────────────────────────────────────────────
        let hi_res = RequestedFormat::new::<RgbFormat>(RequestedFormatType::AbsoluteHighestResolution);
        let fallback_fmt = RequestedFormat::new::<RgbFormat>(RequestedFormatType::None);
        let mut camera = match Camera::new(index.clone(), hi_res)
            .or_else(|_| Camera::new(index, fallback_fmt))
        {
            Ok(c)  => c,
            Err(e) => {
                let _ = init_tx.send(Err(format!("Camera::new failed: {}", e)));
                return;
            }
        };

        if let Err(e) = camera.open_stream() {
            let _ = init_tx.send(Err(format!("open_stream failed: {}", e)));
            return;
        }

        // Signal success — main thread unblocks here.
        if init_tx.send(Ok(())).is_err() {
            return; // caller already gone
        }

        // ── Scan loop ─────────────────────────────────────────────────────────
        loop {
            fc.fetch_add(1, Ordering::Relaxed);

            let frame = match camera.frame() {
                Ok(f)  => f,
                Err(_) => { thread::sleep(Duration::from_millis(50)); continue; }
            };

            // Try RgbFormat first; fall back to LumaFormat for cameras that
            // output grayscale natively (saves the RGB→luma conversion).
            let gray = match frame.decode_image::<RgbFormat>() {
                Ok(rgb) => DynamicImage::ImageRgb8(rgb).into_luma8(),
                Err(_) => match frame.decode_image::<LumaFormat>() {
                    Ok(luma) => luma,
                    Err(_) => continue,
                },
            };

            if let Some(pkt) = decode_packet_from_luma(&gray) {
                if pkt_tx.send(pkt).is_err() {
                    return; // main thread dropped rx — exit cleanly
                }
            }

            // ~30 fps cap — camera is already running at max resolution so
            // there's no value in spinning faster than the display refreshes.
            thread::sleep(Duration::from_millis(33));
        }
    });

    // Block until the thread reports init success or failure.
    match init_rx.recv() {
        Ok(Ok(())) => Ok((pkt_rx, frame_count)),
        Ok(Err(msg)) => anyhow::bail!("Camera init failed: {}", msg),
        Err(_) => anyhow::bail!("Camera thread panicked during init"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::display;
    use base64::{engine::general_purpose::STANDARD as B64, Engine};
    use image::Luma;
    use qrcode::{EcLevel, QrCode};

    fn render_qr_luma(json: &str, scale: u32, invert: bool) -> GrayImage {
        let code = QrCode::with_error_correction_level(json.as_bytes(), EcLevel::M).unwrap();
        let quiet = 4u32;
        let mods = code.width() as u32;
        let size = (mods + quiet * 2) * scale;
        let bg = if invert { 0u8 } else { 255u8 };
        let fg = if invert { 255u8 } else { 0u8 };
        let mut img = GrayImage::from_pixel(size, size, Luma([bg]));

        for my in 0..mods {
            for mx in 0..mods {
                if code[(mx as usize, my as usize)] == qrcode::Color::Dark {
                    for dy in 0..scale {
                        for dx in 0..scale {
                            img.put_pixel(
                                (mx + quiet) * scale + dx,
                                (my + quiet) * scale + dy,
                                Luma([fg]),
                            );
                        }
                    }
                }
            }
        }
        img
    }

    fn embed_centered(qr: &GrayImage, width: u32, height: u32, bg: u8) -> GrayImage {
        let mut frame = GrayImage::from_pixel(width, height, Luma([bg]));
        let x0 = (width.saturating_sub(qr.width())) / 2;
        let y0 = (height.saturating_sub(qr.height())) / 2;
        image::imageops::overlay(&mut frame, qr, x0 as i64, y0 as i64);
        frame
    }

    /// Rasterize the Unicode Dense1x2 string emitted by `display::render_qr_string`
    /// into a simulated terminal screenshot (width = cell_w * cols, height = cell_h * rows).
    fn rasterize_dense1x2(rendered: &str, cell_w: u32, cell_h: u32, dark_fg: u8, light_bg: u8) -> GrayImage {
        let lines: Vec<Vec<char>> = rendered.lines().map(|l| l.chars().collect()).collect();
        let rows = lines.len() as u32;
        let cols = lines.iter().map(|l| l.len()).max().unwrap_or(0) as u32;
        let pad = 16u32;
        let mut img = GrayImage::from_pixel(
            cols * cell_w + pad * 2,
            rows * cell_h + pad * 2,
            Luma([light_bg]),
        );

        let top_h = cell_h / 2;
        for (r, line) in lines.iter().enumerate() {
            for (c, ch) in line.iter().enumerate() {
                let (top, bot) = match ch {
                    '█' => (dark_fg, dark_fg),
                    '▀' => (dark_fg, light_bg),
                    '▄' => (light_bg, dark_fg),
                    ' ' => (light_bg, light_bg),
                    _ => (light_bg, light_bg),
                };
                let x0 = pad + (c as u32) * cell_w;
                let y0 = pad + (r as u32) * cell_h;
                for dy in 0..cell_h {
                    let val = if dy < top_h { top } else { bot };
                    for dx in 0..cell_w {
                        img.put_pixel(x0 + dx, y0 + dy, Luma([val]));
                    }
                }
            }
        }
        img
    }

    #[test]
    fn decodes_all_packet_variants_normal_and_inverted() {
        let data_b64 = B64.encode(vec![0xABu8; 128]);
        let packets = vec![
            Packet::Init {
                filename: "demo.txt".into(),
                total_size: 166,
                chunk_count: 2,
                checksum: "a".repeat(64),
            },
            Packet::InitAck,
            Packet::Data { seq: 0, data: data_b64 },
            Packet::Ack { seq: 0 },
            Packet::Nak { seq: 1 },
            Packet::Fin { saved_path: "/tmp/demo.txt".into() },
            Packet::Error { msg: "checksum mismatch".into() },
        ];

        for pkt in packets {
            let json = pkt.to_json().unwrap();
            for invert in [false, true] {
                let img = render_qr_luma(&json, 4, invert);
                let decoded = decode_packet_from_luma(&img)
                    .unwrap_or_else(|| panic!("failed to decode {:?} (invert={})", pkt, invert));
                assert_eq!(decoded, pkt);
            }
        }
    }

    #[test]
    fn decodes_from_1080p_and_480p_simulated_webcam_frames() {
        let pkt = Packet::Data {
            seq: 3,
            data: B64.encode(vec![42u8; 128]),
        };
        let json = pkt.to_json().unwrap();

        // 1920x1080 frame with 6px modules
        let qr_1080 = render_qr_luma(&json, 6, false);
        let frame_1080 = embed_centered(&qr_1080, 1920, 1080, 40);
        assert_eq!(decode_packet_from_luma(&frame_1080), Some(pkt.clone()));

        // 640x480 frame with 4px modules
        let qr_480 = render_qr_luma(&json, 4, false);
        let frame_480 = embed_centered(&qr_480, 640, 480, 30);
        assert_eq!(decode_packet_from_luma(&frame_480), Some(pkt));
    }

    #[test]
    fn decodes_dense1x2_terminal_rendering_both_polarities() {
        let pkt = Packet::Data {
            seq: 0,
            data: B64.encode(vec![0x5Au8; 128]),
        };
        let json = pkt.to_json().unwrap();
        let rendered = display::render_qr_string(&json).unwrap();

        // Black-on-white (forced ANSI colors)
        let bow = rasterize_dense1x2(&rendered, 8, 16, 10, 245);
        assert_eq!(decode_packet_from_luma(&bow), Some(pkt.clone()));

        // White-on-black (dark-mode terminal without color support)
        let wob = rasterize_dense1x2(&rendered, 8, 16, 245, 10);
        assert_eq!(decode_packet_from_luma(&wob), Some(pkt));
    }
}
