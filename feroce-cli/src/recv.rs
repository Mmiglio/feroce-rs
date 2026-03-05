use feroce::rdma::{
    self,
    buffer_pool::BufferPool,
    device::{CompletionChannel, QueuePair},
};
use std::{collections::VecDeque, sync::Arc, thread::JoinHandle};

use crate::{
    CmOpts, RdmaOpts,
    common::{CmRole, PreparedQP, run_cm_active, run_cm_passive},
    stats::StreamStats,
};

#[allow(dead_code, unused)]
struct QpContext {
    qp: Arc<QueuePair>,
    poller_handle: JoinHandle<()>,
}

pub fn run(
    cm_opts: &CmOpts,
    cm_role: &CmRole,
    rdma_opts: &RdmaOpts,
) -> Result<(), Box<dyn std::error::Error>> {
    // spawn poller closure
    let spawn_poller = |prepared_qp: PreparedQP, stream_id: u32| {
        let stats = Arc::new(StreamStats::new(stream_id));

        let handle = std::thread::spawn({
            let qp = Arc::clone(&prepared_qp.qp);
            let stats = Arc::clone(&stats);
            move || {
                if let Err(e) =
                    poller_thread(qp, prepared_qp.buffer_pool, prepared_qp.comp_channel, stats)
                {
                    eprintln!("poller thread error: {}", e);
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
) -> Result<(), Box<dyn std::error::Error>> {
    // metrics
    let mut total_bytes;
    let mut total_msgs;

    // start by pre-building and pre-posting recv request
    let mut sge_list = Vec::<Vec<rdma::ibv_sge>>::new();
    let mut recv_wr_list = Vec::<rdma::ibv_recv_wr>::new();

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

    let mut free_idx_channel = VecDeque::<usize>::new();
    'poller_loop: loop {
        // wait for completion event
        channel.get_cq_event()?;

        // re-arm the notification
        qp.cq().req_notify_cq(false)?;

        let num_wce = qp.cq().poll(&mut wc_list)?;
        // ack the event
        qp.cq().ack_cq_events(1);

        // finally, process completions
        //println!("Got {} wc events", num_wce);
        total_bytes = 0;
        total_msgs = 0;
        for (ce_idx, wce) in wc_list.iter().enumerate().take(num_wce) {
            if wce.status != rdma::ibv_wc_status::IBV_WC_SUCCESS {
                if wce.status == rdma::ibv_wc_status::IBV_WC_WR_FLUSH_ERR {
                    println!("Got IBV_WC_WR_FLUSH_ERR, QP is most likely being closed");
                } else {
                    println!("WC index {} error status: {}", ce_idx, wce.status as i32)
                }
                break 'poller_loop;
            }

            // update metrics
            total_bytes += wce.byte_len as u64;
            total_msgs += 1;

            // release buffef for now (this will be passed to the processing thread)
            free_idx_channel.push_back(wce.wr_id as usize);
        }

        stats
            .messages
            .fetch_add(total_msgs, std::sync::atomic::Ordering::Relaxed);
        stats
            .bytes
            .fetch_add(total_bytes, std::sync::atomic::Ordering::Relaxed);

        // repost recv requests: this will come from the thread channel
        while let Some(idx) = free_idx_channel.pop_front() {
            qp.post_recv(&mut recv_wr_list[idx])?;
        }
    }

    stats
        .messages
        .fetch_add(total_msgs, std::sync::atomic::Ordering::Relaxed);
    stats
        .bytes
        .fetch_add(total_bytes, std::sync::atomic::Ordering::Relaxed);

    Ok(())
}
