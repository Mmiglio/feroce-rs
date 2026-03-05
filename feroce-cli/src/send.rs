use std::{cmp::min, collections::VecDeque, sync::Arc};

use feroce::rdma::{
    self,
    buffer_pool::{BufferHandle, BufferPool},
    device::{CompletionChannel, QueuePair},
};

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
        let stats = Arc::new(StreamStats::new(stream_id));

        let handle = std::thread::spawn({
            let qp = Arc::clone(&prepared_qp.qp);
            let stats = Arc::clone(&stats);
            let num_msgs = send_opts.num_msgs;
            move || {
                if let Err(e) = poller_thread(
                    qp,
                    prepared_qp.buffer_pool,
                    prepared_qp.comp_channel,
                    num_msgs,
                    stats,
                ) {
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
    num_msgs: u64,
    stats: Arc<StreamStats>,
) -> Result<(), Box<dyn std::error::Error>> {
    const BATCH_SIZE: usize = 32;
    // metrics
    let mut total_bytes;
    let mut total_msgs;

    // counter used as a pattern for the msg payload
    let mut send_posted = 0_u64;
    let mut send_completed = 0_u64;

    // start by pre-building and pre-posting send request
    let mut sge_list = Vec::<Vec<rdma::ibv_sge>>::new();
    let mut send_wr_list = Vec::<rdma::ibv_send_wr>::new();

    let mut pending_batches: VecDeque<Vec<usize>> = VecDeque::new();

    let num_initial_posts = min(buffer_pool.num_buf(), num_msgs as usize);

    for idx in 0..num_initial_posts {
        let mut buf_handle = buffer_pool.get_handle(idx);

        fill_buffer(send_posted, &mut buf_handle);
        send_posted += 1;

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
            false,
        ));
    }

    // create batches
    for chunk_start in (0..num_initial_posts).step_by(BATCH_SIZE) {
        let chunk_end = min(chunk_start + BATCH_SIZE, num_initial_posts);

        // chain the batch WR
        for idx in chunk_start..chunk_end - 1 {
            send_wr_list[idx].next = &mut send_wr_list[idx + 1];
            send_wr_list[idx].send_flags = 0;
        }

        let last = chunk_end - 1;
        send_wr_list[last].next = std::ptr::null_mut();
        send_wr_list[last].send_flags |= rdma::IBV_SEND_SIGNALED;

        qp.post_send(&mut send_wr_list[chunk_start])?;

        // record which indices are in this batch
        let batch_indices: Vec<usize> = (chunk_start..chunk_end).collect();
        pending_batches.push_back(batch_indices);
    }

    // create all work completions
    let mut wc_list = vec![
        rdma::ibv_wc {
            ..Default::default()
        };
        num_initial_posts//buffer_pool.num_buf()
    ];

    // request notification from completion channel for every event
    qp.cq().req_notify_cq(false)?;

    let mut free_idx_channel = VecDeque::<usize>::new();
    let mut batch = Vec::<usize>::new();

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

            // get the completed batch
            let completed_batch = pending_batches.pop_front().unwrap();
            let batch_len = completed_batch.len() as u64;

            send_completed += batch_len;
            // update metrics
            //total_bytes += sge_list[wce.wr_id as usize][0].length as u64;
            for idx in completed_batch {
                total_bytes += sge_list[idx][0].length as u64;
                free_idx_channel.push_back(idx);
            }
            total_msgs += batch_len;

            if send_completed >= num_msgs {
                // we sent all messages, done
                break 'poller_loop;
            }

            //free_idx_channel.push_back(wc_list[ce_idx].wr_id as usize);
        }

        stats
            .messages
            .fetch_add(total_msgs, std::sync::atomic::Ordering::Relaxed);
        stats
            .bytes
            .fetch_add(total_bytes, std::sync::atomic::Ordering::Relaxed);

        while !free_idx_channel.is_empty() {
            batch.clear();
            while let Some(idx) = free_idx_channel.pop_front() {
                batch.push(idx);
                let mut buf_handle = buffer_pool.get_handle(idx);
                fill_buffer(send_posted, &mut buf_handle);
                send_posted += 1;
                if send_posted == num_msgs || batch.len() == BATCH_SIZE {
                    break;
                }
            }

            // chain and set the last to signaled
            for idx in 0..batch.len() - 1 {
                let curr = batch[idx];
                let next = batch[idx + 1];
                send_wr_list[curr].next = &mut send_wr_list[next];
                send_wr_list[curr].send_flags = 0;
            }

            let last_idx = *batch.last().unwrap();
            send_wr_list[last_idx].next = std::ptr::null_mut();
            send_wr_list[last_idx].send_flags |= rdma::IBV_SEND_SIGNALED;

            qp.post_send(&mut send_wr_list[batch[0]])?;
            pending_batches.push_back(batch.clone());
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
