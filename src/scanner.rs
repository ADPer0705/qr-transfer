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
/// Opens camera `index`, then continuously:
///   1. Grabs a frame.
///   2. Converts to greyscale.
///   3. Runs rqrr grid detection + decode.
///   4. Parses the decoded string as a [`Packet`].
///   5. Sends valid packets down the channel.
///
/// The thread runs until the `Sender` half of the channel is dropped (i.e.
/// the main thread exits).
pub fn spawn(camera_index: u32) -> Result<mpsc::Receiver<Packet>> {
    let (tx, rx) = mpsc::channel::<Packet>();

    // We can define the config on the main thread, because config data
    // is just numbers and easily implements `Send`.
    let index  = CameraIndex::Index(camera_index);
    let format = RequestedFormat::new::<RgbFormat>(RequestedFormatType::AbsoluteHighestFrameRate);

    // ── Scanning loop ─────────────────────────────────────────────────────────
    thread::spawn(move || {
        // 1. Initialize the camera INSIDE the thread it will run on
        #[cfg(target_os = "macos")]
        nokhwa::nokhwa_initialize(|_granted| {});

        let mut camera = match Camera::new(index, format) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("  [Camera Thread] Failed to init camera: {}", e);
                return;
            }
        };

        if let Err(e) = camera.open_stream() {
            eprintln!("  [Camera Thread] Failed to open stream: {}", e);
            return;
        }

        loop {
            // Grab frame
            let frame = match camera.frame() {
                Ok(f)  => f,
                Err(_) => { thread::sleep(Duration::from_millis(50)); continue; }
            };

            // Decode to RGB → convert to greyscale
            let rgb = match frame.decode_image::<RgbFormat>() {
                Ok(img) => img,
                Err(_)  => continue,
            };
            let gray = DynamicImage::ImageRgb8(rgb).into_luma8();

            // Run QR detector
            let mut prepared = rqrr::PreparedImage::prepare(gray);
            let grids = prepared.detect_grids();

            for grid in grids {
                if let Ok((_meta, content)) = grid.decode() {
                    if let Ok(pkt) = Packet::from_json(&content) {
                        // If the receiver has dropped, exit the thread.
                        if tx.send(pkt).is_err() {
                            return;
                        }
                    }
                }
            }

            // ~30 fps cap — avoids pinning the CPU
            thread::sleep(Duration::from_millis(33));
        }
    });

    Ok(rx)
}
