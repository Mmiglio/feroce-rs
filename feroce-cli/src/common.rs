use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    path::Path,
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

#[cfg(feature = "tui")]
use std::sync::Mutex;

#[cfg(feature = "tui")]
use crate::tui::Tui;
use crate::{
    CmOpts, LogBuffer, RdmaOpts,
    stats::{StatsCsvWriter, StreamStats},
};

impl From<&RdmaOpts> for RdmaConfig {
    fn from(opts: &RdmaOpts) -> Self {
        RdmaConfig {
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

// Callback invoked from the event loop once per monitoring tick.
// Returning `true` asks the event loop to shut down (e.g. pressed 'q' or ctrl+c).
pub type Monitor = Box<dyn FnMut(&[Arc<StreamStats>], Duration) -> bool>;

// Build the monitor closure: sample each stream once per tick
// then send the snapshots out to CSV, TUI, and/or stdout.
pub fn make_monitor(
    stats_out: Option<&Path>,
    #[cfg(feature = "tui")] tui_handle: Option<Arc<Mutex<Tui>>>,
) -> io::Result<Monitor> {
    let mut csv_writer = match stats_out {
        Some(path) => Some(StatsCsvWriter::create(path)?),
        None => None,
    };

    Ok(Box::new(move |stats, elapsed| {
        let samples: Vec<_> = stats.iter().map(|s| s.sample(elapsed)).collect();

        if let Some(csv) = csv_writer.as_mut()
            && let Err(e) = csv.write_tick(&samples, elapsed)
        {
            warn!("--stats-out write failed: {e}");
        }

        #[cfg(feature = "tui")]
        if let Some(ref tui) = tui_handle {
            return tui.lock().unwrap().draw(&samples).unwrap_or(false);
        }

        for s in &samples {
            s.print();
        }
        false
    }))
}

// Wire a poller to a CM session
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
    F: FnMut(ConnectedRdmaEndpoint<A>, u32, u32) -> (JoinHandle<()>, Arc<StreamStats>),
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

    let mut runner = SessionRunner::new(cm_opts, rdma_opts, allocator, spawn_poller, monitor)?;

    if cm_opts.active {
        let remote_addr = SocketAddr::new(
            cm_opts.remote_addr.ok_or("--remote-addr required")?,
            cm_opts.remote_port.ok_or("--remote-port required")?,
        );
        runner.connect_and_run(remote_addr, cm_opts.num_streams)?;
    } else {
        runner.listen_and_run()?;
    }

    Ok(())
}

pub struct SessionRunner<F, A>
where
    A: BufferAllocator,
    F: FnMut(ConnectedRdmaEndpoint<A>, u32, u32) -> (JoinHandle<()>, Arc<StreamStats>),
{
    rdma_cfg: RdmaConfig,
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
}

impl<F, A> SessionRunner<F, A>
where
    A: BufferAllocator,
    F: FnMut(ConnectedRdmaEndpoint<A>, u32, u32) -> (JoinHandle<()>, Arc<StreamStats>),
{
    pub fn new(
        cm_opts: &CmOpts,
        rdma_opts: &RdmaOpts,
        allocator: A,
        spawn_poller: F,
        monitor: Monitor,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let device = Arc::new(Device::open(&rdma_opts.rdma_device)?);
        let link = device
            .probe_link(rdma_opts.port_num, rdma_opts.gid_index as u8)?
            .ok_or(format!(
                "{} GID index {} is not an active RoCE v2 endpoint",
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
            link,
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
            let prepared =
                setup_endpoint(&self.device, self.link, &self.rdma_cfg, &self.allocator)?;

            // connection request to remote side (uses our local_info, no QP transition yet)
            let remote_info =
                self.cm
                    .connect(remote_addr, &prepared.local_info, Duration::from_secs(2), 2)?;

            // RTR + RTS with the remote info
            let connected = connect_endpoint(prepared, &remote_info)?;
            // keep a reference, connected will be moved to the poller thread
            let qp = Arc::clone(&connected.qp);

            // spawn the poller thread using the closure
            let (poller_handle, stats) =
                (self.spawn_poller)(connected, self.next_stream_id, remote_info.qp_num);
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
                    setup_endpoint(&self.device, self.link, &self.rdma_cfg, &self.allocator)?;

                // transition QP to RTS before replying to the peer. once
                // set_local_info sends the ack the active side may start
                // posting WRs against our QP.
                let connected = connect_endpoint(prepared, &remote_info)?;

                self.cm
                    .set_local_info(peer_ip, remote_qpn, &connected.local_info)?;

                let qp = Arc::clone(&connected.qp);

                // spawn the poller thread
                let (poller_handle, stats) =
                    (self.spawn_poller)(connected, self.next_stream_id, remote_qpn);
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
        println!("[Done] Summary:");
        ctx.stats.print_summary();
        if let Err(e) = self.cm.close_qp(qpn, Duration::from_secs(2), 2) {
            warn!("Failed to close QP {qpn}: {e}");
        }
    }
}

impl<F, A> Drop for SessionRunner<F, A>
where
    A: BufferAllocator,
    F: FnMut(ConnectedRdmaEndpoint<A>, u32, u32) -> (JoinHandle<()>, Arc<StreamStats>),
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
