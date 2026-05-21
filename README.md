# qr-transfer (`qrt`)

> File transfer between two laptops using **QR codes as the physical channel**,
> implementing **stop-and-wait ARQ** (the same reliability mechanism as the
> oldest packet networks).

```
L1 (sender)                          L2 (receiver)
┌──────────────┐                    ┌──────────────┐
│  screen ████ │ ◄── camera sees ── │ 📷           │
│  📷          │ ── camera sees ──► │ ████ screen  │
└──────────────┘                    └──────────────┘
```

The two laptops sit **facing each other**.  Each one's camera reads the other's
screen.  No network, no USB, no Bluetooth — just photons.

---

## Protocol overview

```
Sender                          Receiver
──────────────────────────────────────────
[INIT QR]   ──────────────►  save metadata
            ◄──────────────  [INIT_ACK QR]
[DATA(0)]   ──────────────►  store chunk 0
            ◄──────────────  [ACK(0)]
[DATA(1)]   ──────────────►  store chunk 1
            ◄──────────────  [ACK(1)]
   …                           …
[DATA(n)]   ──────────────►  store chunk n
                              assemble + SHA-256 verify
                              write file
            ◄──────────────  [FIN]
done ✓
```

This is **stop-and-wait ARQ**:
- The sender blocks after every QR and waits for an ACK before advancing.
- If no ACK arrives within `RETRANSMIT_MS` (default 5 s), the same QR is
  shown again (retransmit).
- The receiver handles duplicates gracefully (re-sends last ACK).
- The receiver verifies the complete file with SHA-256 before sending FIN.

The protocol also models real networking concepts:
- **Sequence numbers** — each chunk has a `seq` field.
- **NAK** — receiver can explicitly request a retransmit.
- **Windowing** — currently window = 1 (stop-and-wait); extend to sliding-window
  by showing multiple QRs before waiting for ACKs.

---

## QR capacity maths

| Parameter              | Value                              |
|------------------------|------------------------------------|
| `CHUNK_SIZE`           | 512 raw bytes                      |
| base64 overhead        | × 4/3 → 684 chars                  |
| JSON envelope          | ~20 chars → **≈ 704 bytes total**  |
| QR version / EC level  | v23 / EC-L  (max 741 bytes)        |
| QR module grid         | 109 × 109 modules                  |
| Terminal width needed  | ~111 columns                       |

Increasing `CHUNK_SIZE` (in `src/protocol.rs`) will reduce the number of round
trips for large files but requires a wider terminal and better camera focus.
QR v40 EC-L tops out at 2 953 bytes per code.

### Rough throughput estimate

Assuming ~2 s per round trip (show QR + scan + show ACK + scan):

| Chunk size | Throughput       |
|------------|------------------|
| 512 B      | ~256 B/s         |
| 1 024 B    | ~512 B/s         |
| 2 048 B    | ~1 KB/s          |

This is intentionally slow — it is art, not SCP.

---

## Build

```bash
# Prerequisites: Rust stable toolchain, a webcam
cargo build --release
```

On Linux you may need V4L2 dev headers:
```bash
sudo apt install libv4l-dev
```

On macOS the first run will trigger a camera-permission dialog — allow it.

---

## Usage

```bash
# List available cameras
./target/release/qrt cameras

# Sender (laptop 1)
./target/release/qrt send path/to/file.zip --camera 0

# Receiver (laptop 2)
./target/release/qrt recv --output ~/received/ --camera 0
```

Start the receiver first (it will show a "Waiting for INIT" screen), then
start the sender.  Angle both laptops so each camera can clearly see the other
screen.  A ~30–50 cm distance with a clean background works well.

---

## Project layout

```
src/
├── main.rs        CLI entry point (clap)
├── protocol.rs    Packet enum, constants, JSON serde
├── chunker.rs     FileChunker (sender) + FileAssembler (receiver)
├── display.rs     Terminal rendering: QR codes + status text (crossterm)
├── scanner.rs     Background camera thread (nokhwa + rqrr)
├── sender.rs      Sender state machine
└── receiver.rs    Receiver state machine
```

---

## Extending ideas

- **Sliding window** — show N chunks without waiting for each ACK; collect
  ACKs asynchronously.  This is the leap from stop-and-wait to Go-Back-N / SR.
- **Compression** — `zstd`-compress chunks before base64; massive wins for
  text-heavy files.
- **Bidirectional** — both sides send and receive simultaneously (full-duplex)
  using two separate QR streams on split-screen terminals.
- **Error simulation** — randomly drop ACKs (`--drop-rate 0.1`) to watch the
  ARQ retransmit mechanism in action; great for networking demos.
- **Steganography mode** — instead of a raw QR, encode the data into a live
  video feed using invisible watermarks.  Completely impractical.  Absolutely
  worth building.
