//! Camera capture + QR decoding, running on a background thread.
//!
//! Call [`spawn`] to start the thread; it sends decoded [`Packet`]s over the
//! returned [`std::sync::mpsc::Receiver`].  Invalid frames and malformed JSON
//! are silently dropped — only well-formed packets reach the caller.

use crate::protocol::Packet;
use anyhow::Result;
use image::DynamicImage;
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

    // AbsoluteHighestResolution: most webcams silently drop to 640x480 when
    // asked for the highest *framerate*. rqrr needs sharp edges to find the
    // three finder squares — resolution matters far more than FPS here since
    // we sleep(33ms) in the loop anyway.
    let index  = CameraIndex::Index(camera_index);
    let format = RequestedFormat::new::<RgbFormat>(RequestedFormatType::AbsoluteHighestResolution);

    thread::spawn(move || {
        #[cfg(target_os = "macos")]
        nokhwa::nokhwa_initialize(|_granted| {});

        // ── Camera init ───────────────────────────────────────────────────────
        let mut camera = match Camera::new(index, format) {
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

            // Downscale to a reasonable max width so rqrr's internal threshold
            // (width / 8) uses a local neighbourhood and can resolve QR modules.
            // Without this, high-res webcam frames (~2K) make threshold_s huge
            // (~240), effectively making the binarization global and unreliable.
            const MAX_WIDTH: u32 = 480;
            let gray = if gray.width() > MAX_WIDTH {
                let scale = MAX_WIDTH as f64 / gray.width() as f64;
                let h = (gray.height() as f64 * scale) as u32;
                image::imageops::resize(&gray, MAX_WIDTH, h, image::imageops::FilterType::Triangle)
            } else {
                gray
            };

            // Try raw grayscale first (rqrr's own binarization handles most
            // well-lit QR codes), then inverted for dark-mode terminals.
            'outer: for (base, invert) in [(&gray, false), (&gray, true)] {
                let img = if invert {
                    let mut inv = (*base).clone();
                    image::imageops::invert(&mut inv);
                    inv
                } else {
                    (*base).clone()
                };

                let mut prepared = rqrr::PreparedImage::prepare(img);
                for grid in prepared.detect_grids() {
                    if let Ok((_meta, content)) = grid.decode() {
                        if let Ok(pkt) = Packet::from_json(&content) {
                            if pkt_tx.send(pkt).is_err() {
                                return; // main thread dropped rx — exit cleanly
                            }
                            break 'outer;
                        }
                    }
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
