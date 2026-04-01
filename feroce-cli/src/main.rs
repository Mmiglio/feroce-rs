use clap::{Args, Parser, Subcommand};
use feroce::rdma::buffer_pool::CpuAllocator;
use std::net::IpAddr;

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
    /// Role of the CM: active side initiates the connection process, passive waits
    #[arg(long)]
    active: bool,
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
    /// Size of the RDMA buffer
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

#[derive(Args)]
struct ReceiverOpts {
    /// Receive on GPU memory
    #[arg(long)]
    gpu: bool,
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
        #[command(flatten)]
        receiver_opts: ReceiverOpts,
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
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Recv {
            cm_opts,
            rdma_opts,
            receiver_opts,
        } => {
            // pick the selected allocator
            if receiver_opts.gpu {
                #[cfg(feature = "gpu")]
                {
                    use feroce::rdma::gpu::GpuAllocator;

                    let allocator = GpuAllocator::new(0)?;
                    recv::run(&cm_opts, &rdma_opts, allocator)?;
                }
                #[cfg(not(feature = "gpu"))]
                {
                    return Err("--gpu requires building with --features gpu".into());
                }
            } else {
                recv::run(&cm_opts, &rdma_opts, CpuAllocator)?;
            }
            Ok(())
        }
        Commands::Send {
            cm_opts,
            rdma_opts,
            sender_opts,
        } => {
            send::run(&cm_opts, &rdma_opts, &sender_opts)?;
            Ok(())
        }
    }
}
