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

use feroce::rdma::{self, buffer_pool::BufferAllocator};
use feroce::{
    CmEvent, ConnectionManager, Device, FeroceError, QueuePair, RdmaConfig, RdmaEndpoint,
    connect_endpoint, setup_endpoint,
};
use log::{error, info, warn};

use crate::{CmOpts, RdmaOpts, stats::StreamStats};

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

/// Callback invoked from the event loop once per monitoring tick.
/// Returning `true` asks the event loop to shut down (e.g. pressed 'q' or ctrl+c).
pub type Monitor = Box<dyn FnMut(&[Arc<StreamStats>], Duration) -> bool>;

pub struct SessionRunner<F, A, M>
where
    A: BufferAllocator,
    F: FnMut(RdmaEndpoint<A>, u32, u32) -> (JoinHandle<()>, Arc<StreamStats>),
    M: FnMut(&[Arc<StreamStats>], Duration) -> bool,
{
    rdma_cfg: RdmaConfig,
    device: rdma::device::Device,
    path_mtu: rdma::ibv_mtu,
    allocator: A,

    cm: ConnectionManager,
    qps: HashMap<u32, QpContext>,

    spawn_poller: F,
    shutdown: Arc<AtomicBool>,

    // temporary, ideally we should get it from somewhere else
    next_stream_id: u32,

    monitor: M,
}

impl<F, A, M> SessionRunner<F, A, M>
where
    A: BufferAllocator,
    F: FnMut(RdmaEndpoint<A>, u32, u32) -> (JoinHandle<()>, Arc<StreamStats>),
    M: FnMut(&[Arc<StreamStats>], Duration) -> bool,
{
    pub fn new(
        cm_opts: &CmOpts,
        rdma_opts: &RdmaOpts,
        allocator: A,
        spawn_poller: F,
        monitor: M,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let device = Device::open(&rdma_opts.rdma_device)?;
        let active_path_mtu = device
            .query_rocev2_mtu(rdma_opts.port_num, rdma_opts.gid_index)?
            .ok_or(format!(
                "{} GID index {} is not active RoCE v2",
                rdma_opts.rdma_device, rdma_opts.gid_index
            ))?;

        let cm = ConnectionManager::new(cm_opts.bind_addr, cm_opts.cm_port)?;

        let shutdown = Arc::new(AtomicBool::new(false));
        for sig in [
            signal_hook::consts::SIGINT,
            signal_hook::consts::SIGTERM,
            signal_hook::consts::SIGHUP,
        ] {
            signal_hook::flag::register(sig, Arc::clone(&shutdown))?;
        }

        Ok(SessionRunner {
            rdma_cfg: RdmaConfig::from(rdma_opts),
            device,
            path_mtu: active_path_mtu,
            allocator,
            cm,
            qps: HashMap::new(),
            spawn_poller,
            shutdown,
            next_stream_id: 0,
            monitor,
        })
    }

    // active side, connection setup
    pub fn connect_and_run(
        &mut self,
        remote_addr: SocketAddr,
        num_streams: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for _ in 0..num_streams {
            let rdma_endpoint = setup_endpoint(&self.device, &self.rdma_cfg, &self.allocator)?;
            // keep a reference, rdma_endpoint will be moved to the poller thread
            let qp = Arc::clone(&rdma_endpoint.qp);

            // connection request to remote side
            let remote_info = self.cm.connect(
                remote_addr,
                &rdma_endpoint.local_info,
                Duration::from_secs(2),
                2,
            )?;

            connect_endpoint(&rdma_endpoint, &remote_info, &self.rdma_cfg, self.path_mtu)?;

            // spawn the poller thread using the closure
            let (poller_handle, stats) =
                (self.spawn_poller)(rdma_endpoint, self.next_stream_id, remote_info.qp_num);
            self.next_stream_id += 1;

            self.qps.insert(
                qp.qp_num(),
                QpContext {
                    qp,
                    poller_handle,
                    stats,
                },
            );
        }

        // start signal: dummy for now, it can be external in the future
        for qp_num in self.qps.keys() {
            self.cm.start_qp(*qp_num)?;
        }

        self.event_loop()
    }

    // passive side, wait for incoming connections
    pub fn listen_and_run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.event_loop()
    }

    fn event_loop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // set cm read timeout to avoid blocking the loop
        self.cm.set_read_timeout(Duration::from_millis(100))?;

        // monitor
        let mut last_print = Instant::now();
        let monitoring_interval = Duration::from_secs(1);

        loop {
            if let Some(event) = self.cm.try_process_next()? {
                // we have an actual CM event (open/close QP), handle it
                self.handle_cm_event(event)?;
            }

            // close the streams that are finished (mainly for the sender)
            self.close_finished_streams()?;

            // monitoring, signal interrupts etc
            if self.shutdown.load(Ordering::Relaxed) {
                info!("Shutdown signal received, shutting down...");
                break;
            }

            // all QPS have been closed, terminate the event loop
            if self.next_stream_id > 0 && self.qps.is_empty() {
                break;
            }

            if last_print.elapsed() >= monitoring_interval {
                let stats: Vec<Arc<StreamStats>> = self
                    .qps
                    .values()
                    .map(|ctx| Arc::clone(&ctx.stats))
                    .collect();
                let quit = (self.monitor)(&stats, last_print.elapsed());
                last_print = Instant::now();
                if quit {
                    info!("Quit requested, shutting down...");
                    break;
                }
            }
        }

        // event loop has been interrupted, clean up remaining resources
        // best-effort: a failing close_qp must not prevent the rest from being cleaned up
        let qpns: Vec<u32> = self.qps.keys().copied().collect();
        for qpn in qpns {
            let qp_ctx = self.qps.remove(&qpn).unwrap();
            if let Err(e) = qp_ctx.qp.modify_to_error() {
                warn!("Failed to transition QP {qpn} to error: {e}");
            }
            let _ = qp_ctx.poller_handle.join();
            println!("[Done] Summary:");
            qp_ctx.stats.print_summary();

            if let Err(e) = self.cm.close_qp(qpn, Duration::from_secs(2), 2) {
                warn!("Failed to close QP {qpn}: {e}");
            }
        }

        Ok(())
    }

    fn handle_cm_event(&mut self, event: CmEvent) -> Result<(), Box<dyn std::error::Error>> {
        match event {
            CmEvent::NewConnection {
                peer_ip,
                remote_qpn,
                remote_info,
            } => {
                let rdma_endpoint = setup_endpoint(&self.device, &self.rdma_cfg, &self.allocator)?;
                let qp = Arc::clone(&rdma_endpoint.qp);

                // transition QP to RTS
                connect_endpoint(&rdma_endpoint, &remote_info, &self.rdma_cfg, self.path_mtu)?;

                self.cm
                    .set_local_info(peer_ip, remote_qpn, &rdma_endpoint.local_info)?;

                // spawn the poller thread
                let (poller_handle, stats) =
                    (self.spawn_poller)(rdma_endpoint, self.next_stream_id, remote_qpn);
                // TODO: maybe get stream id from somewhere? e.g. from the active side.
                self.next_stream_id += 1;

                self.qps.insert(
                    qp.qp_num(),
                    QpContext {
                        qp,
                        poller_handle,
                        stats,
                    },
                );

                Ok(())
            }
            CmEvent::CloseQp {
                peer_ip: _,
                local_qpn,
                remote_qpn: _,
            } => {
                // get the qp from the list
                let Some(qp_ctx) = self.qps.remove(&local_qpn) else {
                    return Err(Box::new(FeroceError::Protocol(
                        "Error! the QP is not present in the list".to_string(),
                    )));
                };

                qp_ctx.qp.modify_to_error()?;

                if let Err(e) = qp_ctx.poller_handle.join() {
                    error!("Poller thread panicked: {:?}", e);
                }

                self.cm.ack_close_qp(local_qpn)?;

                println!("[Done] Summary:");
                qp_ctx.stats.print_summary();

                Ok(())
            }
        }
    }

    fn close_finished_streams(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let finished: Vec<u32> = self
            .qps
            .iter()
            .filter(|(_, ctx)| ctx.poller_handle.is_finished())
            .map(|(qpn, _)| *qpn)
            .collect();

        // best-effort: a failing close on one stream must not abort the others
        for qpn in finished {
            let ctx = self.qps.remove(&qpn).unwrap();
            if let Err(e) = ctx.qp.modify_to_error() {
                warn!("Failed to transition QP {qpn} to error: {e}");
            }
            let _ = ctx.poller_handle.join();
            println!("[Done] Summary:");
            ctx.stats.print_summary();
            if let Err(e) = self.cm.close_qp(qpn, Duration::from_secs(2), 2) {
                warn!("Failed to close QP {qpn}: {e}");
            }
        }

        Ok(())
    }
}

impl<F, A, M> Drop for SessionRunner<F, A, M>
where
    A: BufferAllocator,
    F: FnMut(RdmaEndpoint<A>, u32, u32) -> (JoinHandle<()>, Arc<StreamStats>),
    M: FnMut(&[Arc<StreamStats>], Duration) -> bool,
{
    fn drop(&mut self) {
        if self.qps.is_empty() {
            return;
        }
        info!("Cleaning up {} open QP(s) on shutdown", self.qps.len());
        for (qpn, qp_ctx) in self.qps.drain() {
            if let Err(e) = qp_ctx.qp.modify_to_error() {
                warn!("Failed to transition QP {qpn} to error: {e}");
            }
            let _ = qp_ctx.poller_handle.join();
            if let Err(e) = self.cm.close_qp(qpn, Duration::from_millis(500), 1) {
                warn!("Failed to close QP {qpn}: {e}");
            }
        }
    }
}
