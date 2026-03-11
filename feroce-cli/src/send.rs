use std::sync::{Arc, mpsc};

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

        let (free_tx, free_rx) = mpsc::channel::<BufferHandle>();
        let (work_tx, work_rx) = mpsc::channel::<BufferHandle>();

        let _worker_handle = std::thread::spawn({
            let num_msgs = send_opts.num_msgs;
            move || {
                let mut counter = 0;
                loop {
                    let Ok(mut buf_handle) = free_rx.recv() else {
                        // closed channel
                        break;
                    };
                    // fill the buffer and sent it
                    fill_buffer(counter, &mut buf_handle);
                    buf_handle.written_bytes = buf_handle.len;
                    counter += 1;
                    if work_tx.send(buf_handle).is_err() {
                        // channel droped by the sender
                        break;
                    }
                    // check if we produced all the messages
                    if counter == num_msgs {
                        debug!("Worker done, produced {num_msgs} messages");
                        break;
                    }
                }
            }
        });

        let handle = std::thread::spawn({
            let qp = Arc::clone(&prepared_qp.qp);
            let stats = Arc::clone(&stats);
            move || {
                if let Err(e) = poller_thread(
                    qp,
                    prepared_qp.buffer_pool,
                    prepared_qp.comp_channel,
                    stats,
                    free_tx,
                    work_rx,
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
    free_tx: mpsc::Sender<BufferHandle>,
    work_rx: mpsc::Receiver<BufferHandle>,
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

    // detect when the worker is done
    let mut worker_done = false;

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

        if free_tx.send(buf_handle).is_err() {
            error!("failed to send free buf handle to worker");
            panic!();
        }
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

    'poller_loop: loop {
        // wait for completion event / timeout
        if !channel.try_get_cq_event(10)? {
            // timed out, no new completion. Repost the free buffers and check if the worker is still connected.
            drain_work_rx(
                &work_rx,
                &qp,
                &mut send_wr_list,
                &mut send_posted,
                &mut worker_done,
            )?;

            // go back to waiting
            continue;
        }

        // re-arm the notification
        qp.cq().req_notify_cq(false)?;

        let num_wce = qp.cq().poll(&mut wc_list)?;
        // ack the event
        qp.cq().ack_cq_events(1);

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
                break 'poller_loop;
            }

            send_completed += 1;
            // update metrics
            total_bytes += sge_list[wce.wr_id as usize][0].length as u64;
            total_msgs += 1;

            if worker_done && send_completed == send_posted {
                // we sent all posted messages and the worker is done
                break 'poller_loop;
            }

            let buf_handle = buffer_pool.get_handle(wce.wr_id as usize);
            if free_tx.send(buf_handle).is_err() {
                // worker done, but we still need to drain completions
                continue;
            }
        }

        stats
            .messages
            .fetch_add(total_msgs, std::sync::atomic::Ordering::Relaxed);
        stats
            .bytes
            .fetch_add(total_bytes, std::sync::atomic::Ordering::Relaxed);

        // Repost the free buffers and check if the worker is still connected.
        drain_work_rx(
            &work_rx,
            &qp,
            &mut send_wr_list,
            &mut send_posted,
            &mut worker_done,
        )?;
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

fn drain_work_rx(
    work_rx: &mpsc::Receiver<BufferHandle>,
    qp: &QueuePair,
    send_wr_list: &mut [rdma::ibv_send_wr],
    send_posted: &mut u64,
    worker_done: &mut bool,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        match work_rx.try_recv() {
            Ok(buf_handle) => {
                qp.post_send(&mut send_wr_list[buf_handle.index])?;
                *send_posted += 1;
            }
            Err(mpsc::TryRecvError::Empty) => break,
            Err(mpsc::TryRecvError::Disconnected) => {
                *worker_done = true;
                break;
            }
        }
    }
    Ok(())
}
