use std::collections::VecDeque;
use std::sync::Mutex;
use std::thread::sleep;
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};

use feroce::rdma;
use feroce::rdma::buffer_pool::{BufferAllocator, CpuAllocator};
use feroce::{BufferHandle, BufferPool, CompletionChannel, QueuePair, RdmaEndpoint};
use log::{debug, error};

#[cfg(feature = "tui")]
use crate::tui;
use crate::{
    CmOpts, RdmaOpts, SenderOpts,
    common::{Monitor, SessionRunner},
    stats::StreamStats,
};

pub fn run(
    cm_opts: &CmOpts,
    rdma_opts: &RdmaOpts,
    send_opts: &SenderOpts,
    #[cfg_attr(not(feature = "tui"), allow(unused_variables))] log_buffer: Option<
        Arc<Mutex<VecDeque<String>>>,
    >,
) -> Result<(), Box<dyn std::error::Error>> {
    // spawn poller closure
    let spawn_poller = |rdma_endpoint: RdmaEndpoint<CpuAllocator>,
                        stream_id: u32,
                        remote_qpn: u32| {
        let stats = Arc::new(StreamStats::new(
            stream_id,
            rdma_endpoint.qp.qp_num(),
            remote_qpn,
        ));

        let handle = std::thread::spawn({
            let num_msgs = send_opts.num_msgs;
            let qp = Arc::clone(&rdma_endpoint.qp);
            let stats = Arc::clone(&stats);
            move || {
                if let Err(e) = poller_thread(
                    qp,
                    rdma_endpoint.buffer_pool,
                    rdma_endpoint.comp_channel,
                    stats,
                    num_msgs,
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
                tui_clone.lock().unwrap().draw(stats, elapsed).unwrap()
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

    let mut runner = SessionRunner::new(cm_opts, rdma_opts, CpuAllocator, spawn_poller, monitor)?;

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
    num_msgs: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Remove this
    // Needed to give time to the receiver (in passive mode) post WR and be ready to receive.
    // This would require to split session runner in receiver and sender, which is currently
    // not needed as the sender is for testing purposes only.
    sleep(Duration::from_secs(1));

    // metrics
    let mut total_bytes;
    let mut total_msgs;

    // counter used as a pattern for the msg payload
    let mut send_posted = 0_u64;
    let mut send_completed = 0_u64;

    // start by pre-building and pre-posting send request
    let mut sge_list = Vec::<Vec<rdma::ibv_sge>>::new();
    let mut send_wr_list = Vec::<rdma::ibv_send_wr>::new();

    for idx in 0..buffer_pool.num_buf() {
        let buf_handle = buffer_pool.get_handle(idx);

        // 1 sge for now
        sge_list.push(vec![rdma::ibv_sge {
            addr: buf_handle.addr as u64,
            length: buf_handle.len as u32,
            lkey: buf_handle.lkey,
        }]);

        send_wr_list.push(QueuePair::build_send_wr(
            idx as u64,
            &mut sge_list[idx],
            rdma::device::SendOp::Send,
            true,
        ));
    }

    // create all work completions
    let mut wc_list = vec![
        rdma::ibv_wc {
            ..Default::default()
        };
        buffer_pool.num_buf()
    ];

    // keep track of free buffs (simulate channel)
    let mut free_bufs: Vec<usize> = (0..buffer_pool.num_buf()).collect();

    // initial post
    while !free_bufs.is_empty() && send_posted < num_msgs {
        let idx = free_bufs.pop().unwrap();
        let mut buf_handle = buffer_pool.get_handle(idx);
        fill_buffer(send_posted, &mut buf_handle);
        qp.post_send(&mut send_wr_list[idx])?;
        send_posted += 1;
    }

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

        // process work completion, if any
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

            send_completed += 1;
            // update metrics
            total_bytes += sge_list[wce.wr_id as usize][0].length as u64;
            total_msgs += 1;

            // return free buff idx to the pool
            free_bufs.push(wce.wr_id as usize);

            if send_completed == num_msgs {
                // we sent all posted messages and the worker is done
                poller_done = true;
                break;
            }
        }

        stats
            .messages
            .fetch_add(total_msgs, std::sync::atomic::Ordering::Relaxed);
        stats
            .bytes
            .fetch_add(total_bytes, std::sync::atomic::Ordering::Relaxed);

        // Repost the free buffers and check if the worker is still connected.
        while !free_bufs.is_empty() && send_posted < num_msgs {
            let idx = free_bufs.pop().unwrap();
            let mut buf_handle = buffer_pool.get_handle(idx);
            fill_buffer(send_posted, &mut buf_handle);
            qp.post_send(&mut send_wr_list[idx])?;
            send_posted += 1;
        }
    }

    Ok(())
}

fn fill_buffer(counter: u64, buf_handle: &mut BufferHandle) {
    let slice = unsafe { std::slice::from_raw_parts_mut(buf_handle.addr, buf_handle.len) };
    slice[0..8].copy_from_slice(&counter.to_be_bytes());
}
