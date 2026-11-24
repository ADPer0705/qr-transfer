mod chunker;
mod display;
mod protocol;
mod receiver;
mod scanner;
mod sender;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name    = "qrt",
    about   = "QR Transfer — send files between laptops using QR codes\n\
               Implements stop-and-wait ARQ over a visual channel.",
    version,
    propagate_version = true,
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Send a file to another machine
    Send {
        /// File to send
        file: PathBuf,

        /// Camera index to use for reading ACKs from the receiver
        #[arg(short, long, default_value = "0")]
        camera: u32,

        /// Raw bytes per chunk (1..=2048; smaller = larger QR modules, easier to scan)
        #[arg(
            short = 's',
            long,
            default_value_t = protocol::CHUNK_SIZE as u16,
            value_parser = clap::value_parser!(u16).range(1..=2048)
        )]
        chunk_size: u16,
    },

    /// Receive a file from another machine
    Recv {
        /// Directory to save the received file (created if it does not exist)
        #[arg(short, long, default_value = ".")]
        output: PathBuf,

        /// Camera index to use for reading DATA packets from the sender
        #[arg(short, long, default_value = "0")]
        camera: u32,
    },

    /// List all available camera devices and their indices
    Cameras,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Send { file, camera, chunk_size } => {
            sender::run(file, camera, chunk_size as usize)?
        }
        Commands::Recv { output, camera } => receiver::run(output, camera)?,
        Commands::Cameras => scanner::list_cameras()?,
    }

    Ok(())
}
