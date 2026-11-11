use std::{
    io,
    net::SocketAddr,
    path::Path,
    sync::{Arc, atomic::AtomicBool},
    thread::JoinHandle,
};

use feroce::rdma::buffer_pool::BufferAllocator;
use feroce::{ConnectedRdmaEndpoint, RdmaConfig};
use feroce_runtime::{Monitor, OnStreamClosed, Peers, SessionConfig, StreamMetrics, StreamSpec};
use log::warn;

#[cfg(feature = "tui")]
use std::sync::Mutex;

#[cfg(feature = "tui")]
use crate::tui::Tui;
use crate::{
    CmOpts, LogBuffer, RdmaOpts,
    stats::{StatsCsvWriter, print_sample, print_summary},
};

fn peers(cm_opts: &CmOpts) -> Result<Peers, Box<dyn std::error::Error>> {
    if !cm_opts.active {
        return Ok(Peers::Passive(cm_opts.num_streams));
    }

    let remote = SocketAddr::new(
        cm_opts.remote_addr.ok_or("--remote-addr required")?,
        cm_opts.remote_port.ok_or("--remote-port required")?,
    );
    let qpns = &cm_opts.remote_qpn;
    if !qpns.is_empty() && qpns.len() != cm_opts.num_streams as usize {
        return Err(format!(
            "{} remote QPNs given for {} streams",
            qpns.len(),
            cm_opts.num_streams
        )
        .into());
    }

    Ok(Peers::Active(
        (0..cm_opts.num_streams)
            .map(|i| StreamSpec {
                name: format!("stream-{i}"),
                remote,
                remote_qpn: qpns.get(i as usize).copied().unwrap_or(0),
            })
            .collect(),
    ))
}

fn session_config(
    cm_opts: &CmOpts,
    rdma_opts: &RdmaOpts,
) -> Result<SessionConfig, Box<dyn std::error::Error>> {
    Ok(SessionConfig {
        bind_addr: cm_opts.bind_addr,
        cm_port: cm_opts.cm_port,
        peers: peers(cm_opts)?,
        rdma_device: rdma_opts.rdma_device.clone(),
        port_num: rdma_opts.port_num,
        gid_index: rdma_opts.gid_index as u8,
        rdma: RdmaConfig {
            num_buf: rdma_opts.num_buf,
            buf_size: rdma_opts.buf_size,
        },
    })
}

pub fn make_monitor(
    stats_out: Option<&Path>,
    #[cfg(feature = "tui")] tui_handle: Option<Arc<Mutex<Tui>>>,
) -> io::Result<Monitor> {
    let mut csv_writer = match stats_out {
        Some(path) => Some(StatsCsvWriter::create(path)?),
        None => None,
    };

    Ok(Box::new(move |samples, elapsed| {
        if let Some(csv) = csv_writer.as_mut()
            && let Err(e) = csv.write_tick(samples, elapsed)
        {
            warn!("--stats-out write failed: {e}");
        }

        #[cfg(feature = "tui")]
        if let Some(ref tui) = tui_handle {
            return tui.lock().unwrap().draw(samples).unwrap_or(false);
        }

        for s in samples {
            print_sample(s);
        }
        false
    }))
}

pub fn run_session<A, F>(
    cm_opts: &CmOpts,
    rdma_opts: &RdmaOpts,
    allocator: A,
    spawn_poller: F,
    stats_out: Option<&Path>,
    #[cfg_attr(not(feature = "tui"), allow(unused_variables))] log_buffer: LogBuffer,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: BufferAllocator,
    F: FnMut(ConnectedRdmaEndpoint<A>, u32, StreamMetrics) -> JoinHandle<()>,
{
    // TUI handle kept alive to call restore() after the test is done
    #[cfg(feature = "tui")]
    let tui_handle = if let Some(ref buffer) = log_buffer {
        Some(Arc::new(Mutex::new(Tui::new(Arc::clone(buffer))?)))
    } else {
        None
    };

    let monitor = make_monitor(
        stats_out,
        #[cfg(feature = "tui")]
        tui_handle.clone(),
    )?;

    let on_stream_closed: OnStreamClosed = Box::new(|metrics| {
        println!("[Done] Summary:");
        print_summary(metrics);
    });

    let shutdown = Arc::new(AtomicBool::new(false));
    for sig in [
        signal_hook::consts::SIGINT,
        signal_hook::consts::SIGTERM,
        signal_hook::consts::SIGHUP,
    ] {
        signal_hook::flag::register(sig, Arc::clone(&shutdown))?;
    }

    feroce_runtime::run_session(
        session_config(cm_opts, rdma_opts)?,
        allocator,
        spawn_poller,
        monitor,
        on_stream_closed,
        shutdown,
    )
}
