use clap::{Args, Parser, Subcommand};
use std::net::IpAddr;

use crate::common::build_role;

mod common;
mod recv;
mod send;
mod stats;

#[derive(Parser)]
#[command(name = "feroce-cli")]
#[command(about = "FEROCE connection manager cli")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Args)]
struct CmOpts {
    /// IP address to bind the CM to
    #[arg(long, default_value = "0.0.0.0")]
    bind_addr: IpAddr,
    /// Local CM port
    #[arg(long, default_value = "0x4321", value_parser = parse_hex)]
    cm_port: u16,
    /// Remote CM address (Required for the active side)
    #[arg(long)]
    remote_addr: Option<IpAddr>,
    /// Remote CM port (Required for the active side)
    #[arg(long, value_parser = parse_hex)]
    remote_port: Option<u16>,
    /// Number of data streams (=QPs)
    #[arg(long, default_value = "1")]
    num_streams: u32,
}

#[derive(Args)]
struct RdmaOpts {
    /// RDMA device name
    #[arg(long)]
    rdma_device: String,
    /// Device GID index
    #[arg(long)]
    gid_index: i32,
    /// Device port number
    #[arg(long, default_value = "1")]
    port_num: u8,
    // /// Max Work Requests processed in a single batc
    // #[arg(long, default_value = "8")]
    // max_wr: u32,
    // /// Size of the RDMA buffer
    #[arg(long, default_value = "8192")]
    buf_size: usize,
    /// Number of RDMA buffers
    #[arg(long, default_value = "64")]
    num_buf: usize,
}

#[derive(Args)]
struct SenderOpts {
    /// Number of messages sent per stream
    #[arg(long, default_value = "100")]
    num_msgs: u64,
}

#[derive(Subcommand)]
#[command(version, about, long_about = None)]
enum Commands {
    /// FEROCE receiver
    Recv {
        #[command(flatten)]
        cm_opts: CmOpts,
        #[command(flatten)]
        rdma_opts: RdmaOpts,
    },
    /// FEROCE sender
    Send {
        #[command(flatten)]
        cm_opts: CmOpts,
        #[command(flatten)]
        rdma_opts: RdmaOpts,
        #[command(flatten)]
        sender_opts: SenderOpts,
    },
}

fn parse_hex(s: &str) -> Result<u16, String> {
    if let Some(hex) = s.strip_prefix("0x") {
        u16::from_str_radix(hex, 16).map_err(|e| e.to_string())
    } else {
        s.parse::<u16>().map_err(|e| e.to_string())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Recv { cm_opts, rdma_opts } => {
            let cm_role = build_role(&cm_opts, false)?;
            recv::run(&cm_opts, &cm_role, &rdma_opts)?;
            Ok(())
        }
        Commands::Send {
            cm_opts,
            rdma_opts,
            sender_opts,
        } => {
            let cm_role = build_role(&cm_opts, true)?;
            send::run(&cm_opts, &cm_role, &rdma_opts, &sender_opts)?;
            Ok(())
        }
    }
}
