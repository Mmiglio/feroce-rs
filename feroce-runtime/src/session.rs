use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use feroce::rdma::{self, buffer_pool::BufferAllocator, device::LocalLink};
use feroce::{
    CmEvent, ConnectedRdmaEndpoint, ConnectionManager, Device, FeroceError, QueuePair, RdmaConfig,
    connect_endpoint, setup_endpoint,
};
use log::{error, info, warn};

use crate::metrics::{GroupSample, StreamMetrics};

pub struct SessionConfig {
    pub bind_addr: IpAddr,
    pub cm_port: u16,
    /// Required on the active side, ignored on the passive one.
    pub remote: Option<SocketAddr>,
    pub num_streams: u32,
    /// Active side only: remote QPN to request per stream, 0 = any. Empty = any for all.
    pub remote_qpns: Vec<u32>,
    pub active: bool,

    pub rdma_device: String,
    pub port_num: u8,
    pub gid_index: u8,
    pub rdma: RdmaConfig,
}

struct QpContext {
    qp: Arc<QueuePair>,
    poller_handle: JoinHandle<()>,
    metrics: StreamMetrics,
}

// Called once per monitoring tick with every live stream already sampled.
// Returning `true` asks the event loop to shut down (e.g. 'q' or ctrl+c).
pub type Monitor = Box<dyn FnMut(&[GroupSample], Duration) -> bool>;

// Called once per stream after its poller is joined.
pub type OnStreamClosed = Box<dyn FnMut(&StreamMetrics)>;

pub fn run_session<A, F>(
    cfg: SessionConfig,
    allocator: A,
    spawn_poller: F,
    monitor: Monitor,
    on_stream_closed: OnStreamClosed,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: BufferAllocator,
    F: FnMut(ConnectedRdmaEndpoint<A>, u32, StreamMetrics) -> JoinHandle<()>,
{
    let active = cfg.active;
    let num_streams = cfg.num_streams;
    let remote = cfg.remote;

    if !cfg.remote_qpns.is_empty() && cfg.remote_qpns.len() != num_streams as usize {
        return Err(format!(
            "{} remote QPNs given for {} streams",
            cfg.remote_qpns.len(),
            num_streams
        )
        .into());
    }

    let mut runner = SessionRunner::new(
        cfg,
        allocator,
        spawn_poller,
        monitor,
        on_stream_closed,
        shutdown,
    )?;

    if active {
        let remote_addr = remote.ok_or("active side requires a remote CM address")?;
        runner.connect_and_run(remote_addr, num_streams)?;
    } else {
        runner.listen_and_run()?;
    }

    Ok(())
}

pub struct SessionRunner<F, A>
where
    A: BufferAllocator,
    F: FnMut(ConnectedRdmaEndpoint<A>, u32, StreamMetrics) -> JoinHandle<()>,
{
    cfg: SessionConfig,
    device: Arc<rdma::device::Device>,
    link: LocalLink,
    allocator: A,

    cm: ConnectionManager,
    qps: HashMap<u32, QpContext>,

    spawn_poller: F,
    shutdown: Arc<AtomicBool>,

    // temporary, ideally we should get it from somewhere else
    next_stream_id: u32,

    monitor: Monitor,
    on_stream_closed: OnStreamClosed,
}

impl<F, A> SessionRunner<F, A>
where
    A: BufferAllocator,
    F: FnMut(ConnectedRdmaEndpoint<A>, u32, StreamMetrics) -> JoinHandle<()>,
{
    pub fn new(
        cfg: SessionConfig,
        allocator: A,
        spawn_poller: F,
        monitor: Monitor,
        on_stream_closed: OnStreamClosed,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let device = Arc::new(Device::open(&cfg.rdma_device)?);
        let link = device
            .probe_link(cfg.port_num, cfg.gid_index)?
            .ok_or(format!(
                "{} GID index {} is not an active RoCE v2 endpoint",
                cfg.rdma_device, cfg.gid_index
            ))?;

        let cm = ConnectionManager::new(cfg.bind_addr, cfg.cm_port)?;

        Ok(SessionRunner {
            cfg,
            device,
            link,
            allocator,
            cm,
            qps: HashMap::new(),
            spawn_poller,
            shutdown,
            next_stream_id: 0,
            monitor,
            on_stream_closed,
        })
    }

    // active side, connection setup
    pub fn connect_and_run(
        &mut self,
        remote_addr: SocketAddr,
        num_streams: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for i in 0..num_streams as usize {
            let prepared =
                setup_endpoint(&self.device, self.link, &self.cfg.rdma, &self.allocator)?;

            // connection request to remote side (uses our local_info, no QP transition yet)
            let remote_qpn = self.cfg.remote_qpns.get(i).copied().unwrap_or(0);
            let remote_info = self.cm.connect(
                remote_addr,
                &prepared.local_info,
                remote_qpn,
                Duration::from_secs(2),
                2,
            )?;

            // RTR + RTS with the remote info
            let connected = connect_endpoint(prepared, &remote_info)?;
            // keep a reference, connected will be moved to the poller thread
            let qp = Arc::clone(&connected.qp);

            // spawn the poller thread using the closure
            let metrics = StreamMetrics::new(self.next_stream_id, qp.qp_num(), remote_info.qp_num);
            let poller_handle =
                (self.spawn_poller)(connected, self.next_stream_id, metrics.clone());
            self.next_stream_id += 1;

            self.qps.insert(
                qp.qp_num(),
                QpContext {
                    qp,
                    poller_handle,
                    metrics,
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
                let interval = last_print.elapsed();
                let samples: Vec<GroupSample> = self
                    .qps
                    .values()
                    .map(|ctx| ctx.metrics.group.sample(interval))
                    .collect();
                let quit = (self.monitor)(&samples, interval);
                last_print = Instant::now();
                if quit {
                    info!("Quit requested, shutting down...");
                    break;
                }
            }
        }

        // event loop has been interrupted, clean up remaining resources
        for (qpn, ctx) in std::mem::take(&mut self.qps) {
            self.close_stream(qpn, ctx);
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
                let prepared =
                    setup_endpoint(&self.device, self.link, &self.cfg.rdma, &self.allocator)?;

                // transition QP to RTS before replying to the peer. once
                // set_local_info sends the ack the active side may start
                // posting WRs against our QP.
                let connected = connect_endpoint(prepared, &remote_info)?;

                self.cm
                    .set_local_info(peer_ip, remote_qpn, &connected.local_info)?;

                let qp = Arc::clone(&connected.qp);

                // spawn the poller thread
                let metrics = StreamMetrics::new(self.next_stream_id, qp.qp_num(), remote_qpn);
                let poller_handle =
                    (self.spawn_poller)(connected, self.next_stream_id, metrics.clone());
                // TODO: maybe get stream id from somewhere? e.g. from the active side.
                self.next_stream_id += 1;

                self.qps.insert(
                    qp.qp_num(),
                    QpContext {
                        qp,
                        poller_handle,
                        metrics,
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

                (self.on_stream_closed)(&qp_ctx.metrics);

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

        for qpn in finished {
            let ctx = self.qps.remove(&qpn).unwrap();
            self.close_stream(qpn, ctx);
        }

        Ok(())
    }

    // best-effort: one failing stream must not stop the others being cleaned up
    fn close_stream(&mut self, qpn: u32, ctx: QpContext) {
        if let Err(e) = ctx.qp.modify_to_error() {
            warn!("Failed to transition QP {qpn} to error: {e}");
        }
        let _ = ctx.poller_handle.join();
        (self.on_stream_closed)(&ctx.metrics);
        if let Err(e) = self.cm.close_qp(qpn, Duration::from_secs(2), 2) {
            warn!("Failed to close QP {qpn}: {e}");
        }
    }
}

impl<F, A> Drop for SessionRunner<F, A>
where
    A: BufferAllocator,
    F: FnMut(ConnectedRdmaEndpoint<A>, u32, StreamMetrics) -> JoinHandle<()>,
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
