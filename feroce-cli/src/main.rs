use clap::{Parser, Subcommand};
use std::net::IpAddr;

mod recv;
mod send;

#[derive(Parser)]
#[command(name = "feroce-cli")]
#[command(about = "FEROCE connection manager cli")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
#[command(version, about, long_about = None)]
enum Commands {
    /// FEROCE receiver
    Recv {
        /// IP address to bind the CM to
        #[arg(long, default_value = "0.0.0.0")]
        bind_addr: IpAddr,
        /// CM port
        #[arg(long, default_value_t = 0x4321)]
        cm_port: u16,
        /// RDMA device name
        #[arg(long)]
        rdma_device: String,
        #[arg(long)]
        gid_index: i32,
    },
    /// FEROCE sender
    Send {
        /// IP address to bind the CM to
        #[arg(long, default_value = "127.0.0.1")]
        bind_addr: IpAddr,
        /// CM port
        #[arg(long, default_value_t = 0x4321)]
        cm_port: u16,
        /// Remote CM address
        #[arg(long)]
        remote_addr: IpAddr,
        /// Remote CM port
        #[arg(long)]
        remote_port: u16,
        /// RDMA device name
        #[arg(long)]
        rdma_device: String,
        #[arg(long)]
        gid_index: i32,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Recv {
            bind_addr,
            cm_port,
            rdma_device,
            gid_index,
        } => {
            if let Err(e) = recv::run(bind_addr, cm_port, rdma_device, gid_index) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Send {
            bind_addr,
            cm_port,
            remote_addr,
            remote_port,
            rdma_device,
            gid_index,
        } => {
            if let Err(e) = send::run(
                bind_addr,
                cm_port,
                remote_addr,
                remote_port,
                rdma_device,
                gid_index,
            ) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}
