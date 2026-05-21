//! Terminal display helpers.
//!
//! Two modes:
//!   `qr(data, status_lines)` — clears screen, renders QR + status text.
//!   `info(lines)`            — clears screen, prints plain text only (for
//!                               the receiver while it is scanning with no
//!                               QR to show).

use crossterm::{
    cursor,
    execute,
    style::Stylize,
    terminal,
};
use qrcode::{render::unicode, EcLevel, QrCode};
use std::io::{self, Write};

const BANNER: &str = r"
  ██████  ██████    ████████ ██████   █████  ███    ██ ███████
 ██    ██ ██   ██      ██    ██   ██ ██   ██ ████   ██ ██
 ██    ██ ██████       ██    ██████  ███████ ██ ██  ██ ███████
 ██ ▄▄ ██ ██   ██      ██    ██   ██ ██   ██ ██  ██ ██      ██
  ██████  ██   ██      ██    ██   ██ ██   ██ ██   ████ ███████
     ▀▀                                    QR-code file transfer
";

/// Clear the terminal and render a QR code, followed by status lines.
pub fn qr(data: &str, status: &[&str]) -> anyhow::Result<()> {
    let code = QrCode::with_error_correction_level(data.as_bytes(), EcLevel::L)?;
    let rendered = code
        .render::<unicode::Dense1x2>()
        .dark_color(unicode::Dense1x2::Dark)
        .light_color(unicode::Dense1x2::Light)
        .quiet_zone(true)
        .module_dimensions(1, 1)
        .build();

    let mut out = io::stdout();
    execute!(out, terminal::Clear(terminal::ClearType::All), cursor::MoveTo(0, 0))?;
    writeln!(out, "{}", rendered)?;
    for line in status {
        writeln!(out, "  {}", line)?;
    }
    out.flush()?;
    Ok(())
}

/// Clear the terminal and print informational text (no QR code).
pub fn info(lines: &[&str]) -> anyhow::Result<()> {
    let mut out = io::stdout();
    execute!(out, terminal::Clear(terminal::ClearType::All), cursor::MoveTo(0, 0))?;
    writeln!(out, "{}", BANNER.dark_grey())?;
    for line in lines {
        writeln!(out, "  {}", line)?;
    }
    out.flush()?;
    Ok(())
}

/// ASCII progress bar, e.g. `[████░░░░░░░░░░░░] 25.0%`
pub fn progress(done: u32, total: u32) -> String {
    if total == 0 {
        return "[──────────────────────────────────────────] 100%".to_string();
    }
    let pct = done as f32 / total as f32;
    let width = 42usize;
    let filled = (pct * width as f32) as usize;
    let bar = "█".repeat(filled) + &"░".repeat(width - filled);
    format!("[{}] {:.1}%", bar, pct * 100.0)
}
