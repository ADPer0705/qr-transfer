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
    queue,
    style::{Color, ResetColor, SetBackgroundColor, SetForegroundColor, Stylize},
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

/// Abstraction over terminal rendering so state machines can be tested headlessly.
pub trait DisplaySink {
    fn show_qr(&mut self, data: &str, status: &[&str]) -> anyhow::Result<()>;
    fn show_info(&mut self, lines: &[&str]) -> anyhow::Result<()>;
}

/// Default terminal display backed by stdout + crossterm.
pub struct TerminalDisplay;

impl DisplaySink for TerminalDisplay {
    fn show_qr(&mut self, data: &str, status: &[&str]) -> anyhow::Result<()> {
        qr(data, status)
    }

    fn show_info(&mut self, lines: &[&str]) -> anyhow::Result<()> {
        info(lines)
    }
}

/// Encode `data` into a Unicode half-block (`Dense1x2`) QR string at EC level M.
pub fn render_qr_string(data: &str) -> anyhow::Result<String> {
    // EcLevel::M — 15% data recovery capacity.
    // Keeps the QR grid compact enough for standard laptop terminals while
    // remaining resilient to webcam glare and terminal line-height micro-gaps.
    let code = QrCode::with_error_correction_level(data.as_bytes(), EcLevel::M)?;
    Ok(code
        .render::<unicode::Dense1x2>()
        .dark_color(unicode::Dense1x2::Dark)
        .light_color(unicode::Dense1x2::Light)
        .quiet_zone(true)
        .module_dimensions(1, 1)
        .build())
}

/// Clear the terminal and render a QR code, followed by status lines.
pub fn qr(data: &str, status: &[&str]) -> anyhow::Result<()> {
    let rendered = render_qr_string(data)?;
    let qr_lines: Vec<&str> = rendered.lines().collect();
    let qr_height = qr_lines.len();
    let qr_width = qr_lines.first().map(|l| l.chars().count()).unwrap_or(0);

    let mut out = io::stdout();
    execute!(out, terminal::Clear(terminal::ClearType::All), cursor::MoveTo(0, 0))?;
    for line in &qr_lines {
        queue!(
            out,
            SetForegroundColor(Color::Black),
            SetBackgroundColor(Color::White)
        )?;
        write!(out, "{}", line)?;
        queue!(out, ResetColor)?;
        writeln!(out)?;
    }
    for line in status {
        writeln!(out, "  {}", line)?;
    }
    if let Ok((term_cols, term_rows)) = terminal::size() {
        let needed_rows = qr_height + status.len() + 2;
        if qr_width > term_cols as usize || needed_rows > term_rows as usize {
            writeln!(
                out,
                "  ⚠ Terminal too small ({}x{}), need at least {}x{} — zoom out or enlarge window!",
                term_cols, term_rows, qr_width, needed_rows
            )?;
        }
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
