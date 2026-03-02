use feroce::{
    connection::{CmEvent, ConnectionError, ConnectionManager},
    protocol::QpConnectionInfo,
    rdma::{
        self,
        buffer_pool::BufferPool,
        device::{CompletionChannel, Device, QueuePair},
    },
};
use std::{
    collections::{HashMap, VecDeque},
    net::IpAddr,
    sync::Arc,
    thread::JoinHandle,
};

#[allow(dead_code, unused)]
struct QpContext {
    qp: Arc<QueuePair>,
    poller_handle: JoinHandle<()>,
}

pub fn run(
    bind_addr: IpAddr,
    cm_port: u16,
    rdma_device: String,
    gid_index: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let port_num = 1; // move to configs...
    let max_wr_num = 128 as u32;
    let num_buf = max_wr_num as usize;
    let buf_size = 8192 * 8;

    let device = Device::open(&rdma_device)?;
    let active_path_mtu = device
        .query_rocev2_mtu(port_num, gid_index)?
        .ok_or(format!(
            "{} GID index {} is not active RoCE v2",
            rdma_device, gid_index
        ))?;

    let mut qps: HashMap<u32, QpContext> = HashMap::new();
    let mut cm = ConnectionManager::new(bind_addr, cm_port)?;

    loop {
        let cm_event = cm.process_next()?;

        match cm_event {
            CmEvent::NewConnection {
                peer_ip,
                remote_qpn,
                remote_info,
            } => {
                let comp_channel = CompletionChannel::create(&device)?;

                let pd = Arc::new(device.alloc_pd()?);
                let cq = Arc::new(device.create_cq_with_channel(max_wr_num as i32, &comp_channel)?);

                let qp = Arc::new(QueuePair::create_qp(
                    Arc::clone(&pd),
                    Arc::clone(&cq),
                    max_wr_num,
                    1,
                    rdma::ibv_qp_type::IBV_QPT_RC,
                )?);

                // create buffer pool (memory region is handled inside)
                let buf_pool = BufferPool::new(num_buf, buf_size, &pd)?;

                let loc_gid = device.query_gid(port_num, gid_index)?;
                // register local infos
                let local_info = QpConnectionInfo {
                    qp_num: qp.qp_num(),
                    psn: 0,
                    rkey: buf_pool.rkey(),
                    addr: buf_pool.addr(),
                    gid: loc_gid.raw,
                };

                // transition QPs
                qp.modify_to_init(port_num)?;
                qp.modify_to_rtr(&remote_info, gid_index as u8, port_num, active_path_mtu)?;
                qp.modify_to_rts(remote_info.psn)?;

                let poller_thread_handle = std::thread::spawn({
                    let qp = Arc::clone(&qp);
                    move || {
                        if let Err(e) = poller_thread(qp, buf_pool, comp_channel) {
                            eprintln!("poller thread error: {}", e);
                        };
                    }
                });

                qps.insert(
                    qp.qp_num(),
                    QpContext {
                        qp: Arc::clone(&qp),
                        poller_handle: poller_thread_handle,
                    },
                );

                cm.set_local_info(peer_ip, remote_qpn, &local_info)?;
                println!(
                    "connected qp: local {} - remote {}",
                    local_info.qp_num, remote_qpn
                );
            }
            CmEvent::CloseQp {
                peer_ip: _,
                local_qpn,
                remote_qpn,
            } => {
                // get the qp from the list
                let Some(qp_ctx) = qps.remove(&local_qpn) else {
                    return Err(Box::new(ConnectionError::Protocol(
                        "Error! the QP is not present in the list".to_string(),
                    )));
                };

                qp_ctx.qp.modify_to_error()?;

                // FIX THIS, PROPAGATE THE ERR
                let _ = qp_ctx.poller_handle.join();

                cm.ack_close_qp(local_qpn)?;

                println!("closed qp: local {} - remote {}", local_qpn, remote_qpn);
            }
        }
    }
}

fn poller_thread(
    qp: Arc<QueuePair>,
    buffer_pool: BufferPool,
    channel: CompletionChannel,
) -> Result<(), Box<dyn std::error::Error>> {
    // start by pre-building and pre-posting recv request
    let mut sge_list = Vec::<Vec<rdma::ibv_sge>>::new();

    for idx in 0..buffer_pool.num_buf() {
        let buf_handle = buffer_pool.get_handle(idx);
        // 1 sge for now
        sge_list.push(vec![rdma::ibv_sge {
            addr: buf_handle.addr as u64,
            length: buf_handle.len as u32,
            lkey: buf_handle.lkey,
        }]);

        qp.post_recv(idx as u64, &mut sge_list[idx])?;
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

            // release buffef for now (this will be passed to the processing thread)
            free_idx_channel.push_back(wc_list[ce_idx].wr_id as usize);
        }

        // repost recv requests: this will come from the thread channel
        while let Some(idx) = free_idx_channel.pop_front() {
            qp.post_recv(idx as u64, &mut sge_list[idx])?;
        }
    }

    Ok(())
}
