use feroce::rdma;
use feroce::rdma::buffer_pool::BufferAllocator;
use feroce::{BufferPool, CompletionChannel, QueuePair, RdmaEndpoint};
use log::{debug, error};
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};

#[cfg(feature = "tui")]
use crate::tui;
use crate::{
    CmOpts, RdmaOpts,
    common::{Monitor, SessionRunner},
    stats::StreamStats,
};

pub fn run<A: BufferAllocator>(
    cm_opts: &CmOpts,
    rdma_opts: &RdmaOpts,
    allocator: A,
    #[cfg_attr(not(feature = "tui"), allow(unused_variables))] log_buffer: Option<
        Arc<Mutex<VecDeque<String>>>,
    >,
) -> Result<(), Box<dyn std::error::Error>> {
    // spawn poller closure
    let spawn_poller = |rdma_endpoint: RdmaEndpoint<A>, stream_id: u32, remote_qpn: u32| {
        let stats = Arc::new(StreamStats::new(
            stream_id,
            rdma_endpoint.qp.qp_num(),
            remote_qpn,
        ));

        let handle = std::thread::spawn({
            let qp = Arc::clone(&rdma_endpoint.qp);
            let stats = Arc::clone(&stats);
            move || {
                if let Err(e) = poller_thread(
                    qp,
                    rdma_endpoint.buffer_pool,
                    rdma_endpoint.comp_channel,
                    stats,
                ) {
                    error!("poller thread error: {}", e);
                }
            }
        });

        (handle, stats)
    };

    // TUI handle kept alive to call restore() after the test is done
    #[cfg(feature = "tui")]
    let tui_handle = if let Some(ref buffer) = log_buffer {
        Some(Arc::new(Mutex::new(tui::Tui::new(Arc::clone(buffer))?)))
    } else {
        None
    };

    // monitoring closure
    let monitor: Monitor = {
        #[cfg(feature = "tui")]
        if let Some(ref tui) = tui_handle {
            let tui_clone = Arc::clone(tui);
            Box::new(move |stats: &[Arc<StreamStats>], elapsed: Duration| {
                tui_clone.lock().unwrap().draw(stats, elapsed).unwrap_or(false)
            })
        } else {
            Box::new(|stats: &[Arc<StreamStats>], elapsed: Duration| {
                for s in stats {
                    s.print_interval_metrics(elapsed);
                }
                false
            })
        }

        #[cfg(not(feature = "tui"))]
        Box::new(|stats: &[Arc<StreamStats>], elapsed: Duration| {
            for s in stats {
                s.print_interval_metrics(elapsed);
            }
            false
        })
    };

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

fn poller_thread<A: BufferAllocator>(
    qp: Arc<QueuePair>,
    buffer_pool: BufferPool<A>,
    channel: CompletionChannel,
    stats: Arc<StreamStats>,
) -> Result<(), Box<dyn std::error::Error>> {
    // metrics
    let mut total_bytes;
    let mut total_msgs;

    // start by pre-building and pre-posting recv request
    let mut sge_list = Vec::<Vec<rdma::ibv_sge>>::new();
    let mut recv_wr_list = Vec::<rdma::ibv_recv_wr>::new();

    // build wr_list and prepiost all buffers
    for idx in 0..buffer_pool.num_buf() {
        let buf_handle = buffer_pool.get_handle(idx);
        // 1 sge for now
        sge_list.push(vec![rdma::ibv_sge {
            addr: buf_handle.addr as u64,
            length: buf_handle.len as u32,
            lkey: buf_handle.lkey,
        }]);

        recv_wr_list.push(QueuePair::build_recv_wr(idx as u64, &mut sge_list[idx]));

        qp.post_recv(&mut recv_wr_list[idx])?;
    }

    // create all work completions
    let mut wc_list = vec![
        rdma::ibv_wc {
            ..Default::default()
        };
        buffer_pool.num_buf()
    ];

    // request notification from completion channel for every event
    qp.cq().req_notify_cq(false)?;

    let mut poller_done = false;
    while !poller_done {
        // wait for completion event, blocking with timout of 10 ms
        let got_event = channel.try_get_cq_event(10)?;

        // rearm the notification
        if got_event {
            qp.cq().req_notify_cq(false)?;
        }

        // poll the CQ (we do it regardless the presence of an event, to avoid race conditions)
        let num_wce = qp.cq().poll(&mut wc_list)?;

        if got_event {
            qp.cq().ack_cq_events(1);
        }

        // finally, process completions
        total_bytes = 0;
        total_msgs = 0;
        for (ce_idx, wce) in wc_list.iter().enumerate().take(num_wce) {
            if wce.status != rdma::ibv_wc_status::IBV_WC_SUCCESS {
                if wce.status == rdma::ibv_wc_status::IBV_WC_WR_FLUSH_ERR {
                    debug!("Got IBV_WC_WR_FLUSH_ERR, QP is most likely being closed");
                } else {
                    error!("WC index {} error status: {}", ce_idx, wce.status as i32)
                }
                poller_done = true;
                break;
            }

            // update metrics
            total_bytes += wce.byte_len as u64;
            total_msgs += 1;

            // immediately repost buffer
            qp.post_recv(&mut recv_wr_list[wce.wr_id as usize])?;
        }

        stats
            .messages
            .fetch_add(total_msgs, std::sync::atomic::Ordering::Relaxed);
        stats
            .bytes
            .fetch_add(total_bytes, std::sync::atomic::Ordering::Relaxed);
    }

    Ok(())
}
