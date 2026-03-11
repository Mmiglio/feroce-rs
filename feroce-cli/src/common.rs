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
    protocol::QpConnectionInfo,
    rdma::{
        self,
        buffer_pool::BufferPool,
        device::{CompletionChannel, Device, QueuePair},
        ibv_mtu,
    },
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

// Create and init a QP
pub struct PreparedQP {
    pub qp: Arc<QueuePair>,
    pub comp_channel: CompletionChannel,
    pub buffer_pool: BufferPool,
    pub local_info: QpConnectionInfo,
}

pub fn setup_qp(
    device: &Device,
    rdma_opts: &RdmaOpts,
) -> Result<PreparedQP, Box<dyn std::error::Error>> {
    let comp_channel = CompletionChannel::create(device)?;

    // create local QP
    let pd = Arc::new(device.alloc_pd()?);
    let cq = Arc::new(device.create_cq_with_channel(rdma_opts.num_buf as i32, &comp_channel)?);

    let qp = Arc::new(QueuePair::create_qp(
        Arc::clone(&pd),
        Arc::clone(&cq),
        rdma_opts.num_buf as u32,
        1,
        rdma::ibv_qp_type::IBV_QPT_RC,
    )?);

    let loc_gid = device.query_gid(rdma_opts.port_num, rdma_opts.gid_index)?;
    let buffer_pool = BufferPool::new(rdma_opts.num_buf, rdma_opts.buf_size, &pd)?;

    // register local infos
    let local_info = QpConnectionInfo {
        qp_num: qp.qp_num(),
        psn: 0,
        rkey: buffer_pool.rkey(),
        addr: buffer_pool.addr(),
        gid: loc_gid.raw,
    };
    qp.modify_to_init(rdma_opts.port_num)?;

    Ok(PreparedQP {
        qp,
        comp_channel,
        buffer_pool,
        local_info,
    })
}

pub fn connect_qp(
    prepared_qp: &PreparedQP,
    remote_info: &QpConnectionInfo,
    rdma_opts: &RdmaOpts,
    active_path_mtu: ibv_mtu,
) -> Result<(), Box<dyn std::error::Error>> {
    prepared_qp.qp.modify_to_rtr(
        remote_info,
        rdma_opts.gid_index as u8,
        rdma_opts.port_num,
        active_path_mtu,
    )?;
    prepared_qp.qp.modify_to_rts(remote_info.psn)?;
    Ok(())
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
    F: FnMut(PreparedQP, u32) -> (JoinHandle<()>, Arc<StreamStats>),
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

    for stream_id in 0..num_streams {
        let prepared_qp = setup_qp(&device, rdma_opts)?;
        // keep a reference, prepared_qp will be moved to the poller thread
        let qp = Arc::clone(&prepared_qp.qp);

        // connect the CM to gather remote info
        let remote_info = cm.connect(
            remote_addr,
            &prepared_qp.local_info,
            Duration::from_secs(2),
            2,
        )?;

        connect_qp(&prepared_qp, &remote_info, rdma_opts, active_path_mtu)?;

        // spawn the threads
        let (poller_handle, stats) = spawn_poller(prepared_qp, stream_id);

        qps.insert(
            qp.qp_num(),
            QpContext {
                qp,
                poller_handle,
                stats,
            },
        );
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
    F: FnMut(PreparedQP, u32) -> (JoinHandle<()>, Arc<StreamStats>),
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
                    let prepared_qp = setup_qp(&device, rdma_opts)?;
                    let qp = Arc::clone(&prepared_qp.qp);

                    // transition QP to RTS
                    connect_qp(&prepared_qp, &remote_info, rdma_opts, active_path_mtu)?;

                    cm.set_local_info(peer_ip, remote_qpn, &prepared_qp.local_info)?;

                    // spawn the threads
                    let (poller_handle, stats) = spawn_poller(prepared_qp, stream_id);
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
