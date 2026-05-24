//! Camera capture + QR decoding, running on a background thread.
//!
//! Call [`spawn`] to start the thread; it sends decoded [`Packet`]s over the
//! returned [`std::sync::mpsc::Receiver`].  Invalid frames and malformed JSON
//! are silently dropped — only well-formed packets reach the caller.

use crate::protocol::Packet;
use anyhow::Result;
use image::DynamicImage;
use nokhwa::{
    pixel_format::RgbFormat,
    utils::{ApiBackend, CameraIndex, RequestedFormat, RequestedFormatType},
    Camera,
};
use std::{sync::mpsc, thread, time::Duration};

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
pub fn spawn(camera_index: u32) -> Result<mpsc::Receiver<Packet>> {
    let (pkt_tx, pkt_rx) = mpsc::channel::<Packet>();

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
            let frame = match camera.frame() {
                Ok(f)  => f,
                Err(_) => { thread::sleep(Duration::from_millis(50)); continue; }
            };

            let rgb = match frame.decode_image::<RgbFormat>() {
                Ok(img) => img,
                Err(_)  => continue,
            };

            // Convert to greyscale, then apply adaptive threshold to
            // compensate for uneven screen glow and webcam exposure variance.
            // This is the same trick phone cameras do in hardware — it gives
            // rqrr clean black/white edges regardless of ambient lighting.
            // Block radius 51 = ~25px neighbourhood; odd number required.
            let gray = DynamicImage::ImageRgb8(rgb).into_luma8();
            let gray = imageproc::contrast::adaptive_threshold(&gray, 51);

            let mut prepared = rqrr::PreparedImage::prepare(gray);
            let grids = prepared.detect_grids();

            for grid in grids {
                if let Ok((_meta, content)) = grid.decode() {
                    if let Ok(pkt) = Packet::from_json(&content) {
                        if pkt_tx.send(pkt).is_err() {
                            return; // main thread dropped rx — exit cleanly
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
        Ok(Ok(())) => Ok(pkt_rx),
        Ok(Err(msg)) => anyhow::bail!("Camera init failed: {}", msg),
        Err(_) => anyhow::bail!("Camera thread panicked during init"),
    }
}
