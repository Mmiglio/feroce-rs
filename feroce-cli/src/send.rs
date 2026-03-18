use std::sync::Arc;

use feroce::rdma::{
    self,
    buffer_pool::{BufferHandle, BufferPool},
    device::{CompletionChannel, QueuePair},
};
use log::{debug, error};

use crate::{
    CmOpts, RdmaOpts, SenderOpts,
    common::{CmRole, PreparedQP, run_cm_active, run_cm_passive},
    stats::StreamStats,
};

pub fn run(
    cm_opts: &CmOpts,
    cm_role: &CmRole,
    rdma_opts: &RdmaOpts,
    send_opts: &SenderOpts,
) -> Result<(), Box<dyn std::error::Error>> {
    // spawn poller closure
    let spawn_poller = |prepared_qp: PreparedQP, stream_id: u32| {
        let stats = Arc::new(StreamStats::new(stream_id, prepared_qp.qp.qp_num()));

        let handle = std::thread::spawn({
            let num_msgs = send_opts.num_msgs;
            let qp = Arc::clone(&prepared_qp.qp);
            let stats = Arc::clone(&stats);
            move || {
                if let Err(e) = poller_thread(
                    qp,
                    prepared_qp.buffer_pool,
                    prepared_qp.comp_channel,
                    stats,
                    num_msgs,
                ) {
                    error!("poller thread error: {}", e);
                }
            }
        });

        (handle, stats)
    };

    match cm_role {
        CmRole::Active {
            remote_addr,
            num_streams,
        } => {
            run_cm_active(cm_opts, rdma_opts, *remote_addr, *num_streams, spawn_poller)?;
        }
        CmRole::Passive => {
            run_cm_passive(cm_opts, rdma_opts, spawn_poller)?;
        }
    }

    Ok(())
}

fn poller_thread(
    qp: Arc<QueuePair>,
    buffer_pool: BufferPool,
    channel: CompletionChannel,
    stats: Arc<StreamStats>,
    num_msgs: u64,
) -> Result<(), Box<dyn std::error::Error>> {
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

    'poller_loop: loop {
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
                break 'poller_loop;
            }

            send_completed += 1;
            // update metrics
            total_bytes += sge_list[wce.wr_id as usize][0].length as u64;
            total_msgs += 1;

            // return free buff idx to the pool
            free_bufs.push(wce.wr_id as usize);

            if send_completed == num_msgs {
                // we sent all posted messages and the worker is done
                break 'poller_loop;
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

    // flush remaining stats
    stats
        .messages
        .fetch_add(total_msgs, std::sync::atomic::Ordering::Relaxed);
    stats
        .bytes
        .fetch_add(total_bytes, std::sync::atomic::Ordering::Relaxed);

    Ok(())
}

fn fill_buffer(counter: u64, buf_handle: &mut BufferHandle) {
    let slice = unsafe { std::slice::from_raw_parts_mut(buf_handle.addr, buf_handle.len) };
    slice[0..8].copy_from_slice(&counter.to_be_bytes());
}
