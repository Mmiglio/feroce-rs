use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use feroce::{
    connection::{CmEvent, ConnectionError, ConnectionManager},
    rdma::device::{Device, QueuePair},
    runtime::{RdmaConfig, RdmaEndpoint, connect_endpoint, setup_endpoint},
};
use log::{error, info};

use crate::{CmOpts, RdmaOpts, stats::StreamStats};

// the CM can either be active (initiate the connection) or passive (wait for incoming connections)
pub enum CmRole {
    Active {
        remote_addr: SocketAddr,
        num_streams: u32,
    },
    Passive,
}

pub fn build_cm_role(cm_opts: &CmOpts, active: bool) -> Result<CmRole, String> {
    if active {
        let addr = cm_opts
            .remote_addr
            .ok_or("--remote-addr required for active role")?;
        let port = cm_opts
            .remote_port
            .ok_or("--remote-port required for active role")?;

        Ok(CmRole::Active {
            remote_addr: SocketAddr::new(addr, port),
            num_streams: cm_opts.num_streams,
        })
    } else {
        Ok(CmRole::Passive)
    }
}

impl From<&RdmaOpts> for RdmaConfig {
    fn from(opts: &RdmaOpts) -> Self {
        RdmaConfig {
            port_num: opts.port_num,
            gid_index: opts.gid_index,
            num_buf: opts.num_buf,
            buf_size: opts.buf_size,
        }
    }
}

struct QpContext {
    qp: Arc<QueuePair>,
    poller_handle: JoinHandle<()>,
    stats: Arc<StreamStats>,
}

pub fn run_cm_active<F>(
    cm_opts: &CmOpts,
    rdma_opts: &RdmaOpts,
    remote_addr: SocketAddr,
    num_streams: u32,
    mut spawn_poller: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnMut(RdmaEndpoint, u32) -> (JoinHandle<()>, Arc<StreamStats>),
{
    let device = Device::open(&rdma_opts.rdma_device)?;
    let active_path_mtu = device
        .query_rocev2_mtu(rdma_opts.port_num, rdma_opts.gid_index)?
        .ok_or(format!(
            "{} GID index {} is not active RoCE v2",
            rdma_opts.rdma_device, rdma_opts.gid_index
        ))?;

    let mut qps: HashMap<u32, QpContext> = HashMap::new();
    let mut cm = ConnectionManager::new(cm_opts.bind_addr, cm_opts.cm_port)?;

    let rdma_cfg = RdmaConfig::from(rdma_opts);

    for stream_id in 0..num_streams {
        let rdma_endpoint = setup_endpoint(&device, &rdma_cfg)?;
        // keep a reference, rdma_endpoint will be moved to the poller thread
        let qp = Arc::clone(&rdma_endpoint.qp);

        // connect the CM to gather remote info
        let remote_info = cm.connect(
            remote_addr,
            &rdma_endpoint.local_info,
            Duration::from_secs(2),
            2,
        )?;

        connect_endpoint(&rdma_endpoint, &remote_info, &rdma_cfg, active_path_mtu)?;

        // spawn the threads
        let (poller_handle, stats) = spawn_poller(rdma_endpoint, stream_id);

        qps.insert(
            qp.qp_num(),
            QpContext {
                qp,
                poller_handle,
                stats,
            },
        );
    }

    // start signal: dummy for now, it can be external in the future
    for qp_num in qps.keys() {
        cm.start_qp(*qp_num)?;
    }

    // monitoring loop
    let shutdown = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&shutdown))?;

    let monitoring_interval = Duration::from_secs(1);
    loop {
        std::thread::sleep(monitoring_interval);

        // check exit conditions
        if shutdown.load(Ordering::Relaxed) {
            info!("Ctrl+C received, shutting down...");
            break;
        }
        if qps.values().all(|ctx| ctx.poller_handle.is_finished()) {
            break;
        }

        for qp_ctx in qps.values() {
            qp_ctx.stats.print_interval_metrics(monitoring_interval);
        }
    }

    // Signal stop or all threads finished
    for (qpn, qp_ctx) in qps.drain() {
        qp_ctx.qp.modify_to_error()?;
        let _ = qp_ctx.poller_handle.join();

        println!("[Done] Summary: ");

        qp_ctx.stats.print_summary();

        cm.close_qp(qpn, Duration::from_secs(2), 2)?;
    }

    Ok(())
}

pub fn run_cm_passive<F>(
    cm_opts: &CmOpts,
    rdma_opts: &RdmaOpts,
    mut spawn_poller: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnMut(RdmaEndpoint, u32) -> (JoinHandle<()>, Arc<StreamStats>),
{
    let device = Device::open(&rdma_opts.rdma_device)?;
    let active_path_mtu = device
        .query_rocev2_mtu(rdma_opts.port_num, rdma_opts.gid_index)?
        .ok_or(format!(
            "{} GID index {} is not active RoCE v2",
            rdma_opts.rdma_device, rdma_opts.gid_index
        ))?;

    let mut qps: HashMap<u32, QpContext> = HashMap::new();
    let mut cm = ConnectionManager::new(cm_opts.bind_addr, cm_opts.cm_port)?;

    let rdma_cfg = RdmaConfig::from(rdma_opts);

    let mut stream_id = 0;

    // set cm read timeout to avoid blocking the loop
    cm.set_read_timeout(Duration::from_millis(100))?;

    // handle sigint/sigterm
    let shutdown = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&shutdown))?;

    // monitor
    let mut last_print = Instant::now();
    let monitoring_interval = Duration::from_secs(1);

    // CM Loop
    loop {
        if let Some(event) = cm.try_process_next()? {
            // we have an actual CM event, match it
            match event {
                CmEvent::NewConnection {
                    peer_ip,
                    remote_qpn,
                    remote_info,
                } => {
                    let rdma_endpoint = setup_endpoint(&device, &rdma_cfg)?;
                    let qp = Arc::clone(&rdma_endpoint.qp);

                    // transition QP to RTS
                    connect_endpoint(&rdma_endpoint, &remote_info, &rdma_cfg, active_path_mtu)?;

                    cm.set_local_info(peer_ip, remote_qpn, &rdma_endpoint.local_info)?;

                    // spawn the threads
                    let (poller_handle, stats) = spawn_poller(rdma_endpoint, stream_id);
                    // maybe get stream id from somewhere? e.g. from the active side.
                    stream_id += 1;

                    qps.insert(
                        qp.qp_num(),
                        QpContext {
                            qp,
                            poller_handle,
                            stats,
                        },
                    );
                }
                CmEvent::CloseQp {
                    peer_ip: _,
                    local_qpn,
                    remote_qpn: _,
                } => {
                    // get the qp from the list
                    let Some(qp_ctx) = qps.remove(&local_qpn) else {
                        return Err(Box::new(ConnectionError::Protocol(
                            "Error! the QP is not present in the list".to_string(),
                        )));
                    };

                    qp_ctx.qp.modify_to_error()?;

                    // FIX THIS, PROPAGATE THE ERR
                    if let Err(e) = qp_ctx.poller_handle.join() {
                        error!("Poller thread panicked: {:?}", e);
                    }

                    cm.ack_close_qp(local_qpn)?;

                    println!("[Done] Summary:");
                    qp_ctx.stats.print_summary();
                }
            }
        }

        // monitoring, signal interrupts etc
        if shutdown.load(Ordering::Relaxed) {
            info!("Ctrl+C received, shutting down...");
            break;
        }

        if last_print.elapsed() >= monitoring_interval {
            for qp_ctx in qps.values() {
                qp_ctx.stats.print_interval_metrics(last_print.elapsed());
            }
            last_print = Instant::now();
        }
    }

    // cleanup
    for (_qpn, qp_ctx) in qps.drain() {
        qp_ctx.qp.modify_to_error()?;
        let _ = qp_ctx.poller_handle.join();
        println!("[Done] Summary:");
        qp_ctx.stats.print_summary();
        // this is the passive side, we can't send somethign to the active side.
    }

    Ok(())
}
