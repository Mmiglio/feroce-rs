use std::{
    cmp::min,
    collections::{HashMap, VecDeque},
    net::{IpAddr, SocketAddr},
    sync::Arc,
    thread::{JoinHandle, sleep},
    time::{Duration, Instant},
};

use feroce::{
    connection::ConnectionManager,
    protocol::QpConnectionInfo,
    rdma::{
        self,
        buffer_pool::{BufferHandle, BufferPool},
        device::{CompletionChannel, Device, QueuePair},
    },
};

use crate::stats::StreamStats;

#[allow(dead_code, unused)]
struct QpContext {
    qp: Arc<QueuePair>,
    poller_handle: JoinHandle<()>,
}

struct ConnectedQp {
    qp: Arc<QueuePair>,
    comp_channel: CompletionChannel,
    buffer_pool: BufferPool,
    stream_id: u32,
}

pub fn run(
    bind_addr: IpAddr,
    cm_port: u16,
    remote_addr: IpAddr,
    remote_port: u16,
    rdma_device: String,
    gid_index: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let port_num = 1; // move to configs...
    let max_wr_num = 128 as u32;
    let num_buf = max_wr_num as usize;
    let buf_size = 8192 * 8;
    let num_streams = 4;
    let num_msgs = 10000 as u64;

    let mut streams_stat = Vec::<Arc<StreamStats>>::new();

    let device = Device::open(&rdma_device)?;
    let active_path_mtu = device
        .query_rocev2_mtu(port_num, gid_index)?
        .ok_or(format!(
            "{} GID index {} is not active RoCE v2",
            rdma_device, gid_index
        ))?;

    let mut qps: HashMap<u32, QpContext> = HashMap::new();
    let mut cm = ConnectionManager::new(bind_addr, cm_port)?;

    let mut connected_qps = Vec::<ConnectedQp>::new();
    // to be moved into functions
    for stream_id in 0..num_streams {
        let comp_channel = CompletionChannel::create(&device)?;

        // create local QP
        let pd = Arc::new(device.alloc_pd()?);
        let cq = Arc::new(device.create_cq_with_channel(max_wr_num as i32, &comp_channel)?);

        let qp = Arc::new(QueuePair::create_qp(
            Arc::clone(&pd),
            Arc::clone(&cq),
            max_wr_num,
            1,
            rdma::ibv_qp_type::IBV_QPT_RC,
        )?);

        let loc_gid = device.query_gid(port_num, gid_index)?;
        let buffer_pool = BufferPool::new(num_buf, buf_size, &pd)?;

        // register local infos
        let local_info = QpConnectionInfo {
            qp_num: qp.qp_num(),
            psn: 0,
            rkey: buffer_pool.rkey(),
            addr: buffer_pool.addr(),
            gid: loc_gid.raw,
        };
        qp.modify_to_init(port_num)?;

        println!("created local qp {}", qp.qp_num());

        // connect the CM to gather remote info
        let remote_info = cm.connect(
            SocketAddr::new(remote_addr, remote_port),
            &local_info,
            Duration::from_secs(2),
            2,
        )?;

        qp.modify_to_rtr(&remote_info, gid_index as u8, port_num, active_path_mtu)?;
        qp.modify_to_rts(remote_info.psn)?;

        connected_qps.push(ConnectedQp {
            qp: Arc::clone(&qp),
            comp_channel,
            buffer_pool,
            stream_id,
        });

        println!(
            "connected qp: local {} - remote {}",
            local_info.qp_num, remote_info.qp_num
        );
    }

    // start senders
    let timer_start = Instant::now();
    for qp_ctx in connected_qps {
        let stat = Arc::new(StreamStats::new(qp_ctx.stream_id));
        let poller_handle = std::thread::spawn({
            let qp = Arc::clone(&qp_ctx.qp);
            let stat = Arc::clone(&stat);
            move || {
                if let Err(e) =
                    poller_thread(qp, qp_ctx.buffer_pool, qp_ctx.comp_channel, num_msgs, stat)
                {
                    eprintln!("poller thread error: {}", e);
                };
            }
        });

        streams_stat.push(stat);

        qps.insert(
            qp_ctx.qp.qp_num(),
            QpContext {
                qp: Arc::clone(&qp_ctx.qp),
                poller_handle: poller_handle,
            },
        );
    }

    //close the qps
    for (qpn, qp_ctx) in qps.drain() {
        let _ = qp_ctx.poller_handle.join();
        qp_ctx.qp.modify_to_error()?;
        cm.close_qp(qpn, Duration::from_secs(2), 2)?;
        println!("closed local qp {}", qpn);
    }

    let timer_stop = Instant::now();
    let run_duration = timer_stop.duration_since(timer_start);

    for stat in streams_stat {
        stat.print_metrics(run_duration, 0, 0);
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
    // metrics
    let mut total_bytes = 0 as u64;
    let mut total_msgs = 0 as u64;

    // counter used as a pattern for the msg payload
    let mut send_posted = 0 as u64;
    let mut send_completed = 0 as u64;

    // start by pre-building and pre-posting send request
    let mut sge_list = Vec::<Vec<rdma::ibv_sge>>::new();

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

        qp.post_send(
            idx as u64,
            &mut sge_list[idx],
            rdma::device::SendOp::Send,
            true,
        )?;
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
        for ce_idx in 0..num_wce {
            if wc_list[ce_idx].status != rdma::ibv_wc_status::IBV_WC_SUCCESS {
                if wc_list[ce_idx].status == rdma::ibv_wc_status::IBV_WC_WR_FLUSH_ERR {
                    println!("Got IBV_WC_WR_FLUSH_ERR, QP is most likely being closed");
                } else {
                    println!(
                        "WC index {} error status: {}",
                        ce_idx, wc_list[ce_idx].status as i32
                    )
                }
                break 'poller_loop;
            }

            send_completed += 1;

            if send_completed == num_msgs {
                // we sent all messages, done
                break 'poller_loop;
            }

            free_idx_channel.push_back(wc_list[ce_idx].wr_id as usize);

            // update metrics
            total_bytes += sge_list[wc_list[ce_idx].wr_id as usize][0].length as u64;
            total_msgs += 1;
        }

        stats
            .messages
            .fetch_add(total_msgs, std::sync::atomic::Ordering::Relaxed);
        stats
            .bytes
            .fetch_add(total_bytes, std::sync::atomic::Ordering::Relaxed);

        while let Some(idx) = free_idx_channel.pop_front() {
            if send_posted == num_msgs {
                break;
            }
            // we need to post more
            let mut buf_handle = buffer_pool.get_handle(idx);
            fill_buffer(send_posted, &mut buf_handle);
            qp.post_send(
                idx as u64,
                &mut sge_list[idx],
                rdma::device::SendOp::Send,
                true,
            )?;
            send_posted += 1;
        }
    }

    Ok(())
}

fn fill_buffer(counter: u64, buf_handle: &mut BufferHandle) {
    let slice = unsafe { std::slice::from_raw_parts_mut(buf_handle.addr, buf_handle.len) };
    slice[0..8].copy_from_slice(&counter.to_be_bytes());
}
