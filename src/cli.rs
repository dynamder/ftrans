use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::net::RelayChoice;

#[derive(Parser)]
#[command(name = "ftrans", about = "Fast LAN large-file transfer")]
pub struct Cli {
    #[arg(
        long,
        global = true,
        default_value = "none",
        value_name = "MODE",
        help = "How to reach the peer: none = LAN only, no relay and no external \
                server (default); n0 = n0 public relay (needs internet); or a \
                relay URL, e.g. http://10.0.0.5:3340 for your own iroh-relay"
    )]
    pub relay: RelayChoice,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    #[command(about = "Send files (run on old machine)")]
    Send {
        #[arg(short, long, help = "File or directory to send (repeatable)")]
        path: Vec<PathBuf>,
        #[arg(
            long,
            help = "Temp store dir (default: same drive as the data; the store holds file hashes, not the data itself)"
        )]
        store_dir: Option<PathBuf>,
    },
    #[command(about = "Receive files (run on new machine)")]
    Receive {
        #[arg(help = "Session code from the sender (e.g. 4K7M2P), or a full ticket")]
        ticket: Option<String>,
        #[arg(
            long,
            value_name = "IP",
            help = "Probe this address directly instead of broadcasting (for networks that filter broadcast traffic)"
        )]
        addr: Vec<std::net::IpAddr>,
        #[arg(short, long, default_value = ".", help = "Output directory")]
        output: PathBuf,
        #[arg(
            long,
            help = "Temp store dir (default: same drive as --output)"
        )]
        store_dir: Option<PathBuf>,
        #[arg(
            long,
            help = "Skip verifying exported files against the sender's checksums"
        )]
        no_verify: bool,
        #[arg(
            long,
            default_value_t = 8,
            help = "Number of parallel file download streams"
        )]
        parallel: usize,
        #[arg(
            long,
            help = "Retry only the files listed in <output>/ftrans-failed.txt; without that file, check local files against the sender and re-transfer only the failing ones"
        )]
        retry_failed: bool,
        #[arg(
            long,
            help = "Export files directly into --output, omitting the source folder name layer"
        )]
        strip_root: bool,
        #[arg(
            long,
            default_value_t = 2,
            help = "How many times to automatically re-transfer files that fail hash verification"
        )]
        max_retries: usize,
        #[arg(
            long,
            help = "Print the list of files that are being retried"
        )]
        print_retried: bool,
    },
}
