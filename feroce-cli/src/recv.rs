use feroce::{
    connection::{CmEvent, ConnectionError, ConnectionManager},
    protocol::QpConnectionInfo,
    rdma::{
        self,
        buffer_pool::BufferPool,
        device::{CompletionChannel, Device, QueuePair},
    },
};
use std::{collections::HashMap, net::IpAddr, sync::Arc, thread::JoinHandle};

#[allow(dead_code, unused)]
struct QpContext {
    qp: QueuePair,
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
    let buf_size = 8192;

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

                let qp = QueuePair::create_qp(
                    Arc::clone(&pd),
                    Arc::clone(&cq),
                    max_wr_num,
                    1,
                    rdma::ibv_qp_type::IBV_QPT_RC,
                )?;

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

                qps.insert(qp.qp_num(), QpContext { qp });

                cm.set_local_info(peer_ip, remote_qpn, &local_info)?;
                println!(
                    "Connected new qp! local {} - remote {}",
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
                cm.ack_close_qp(local_qpn)?;

                println!("Closed qp! local {} - remote {}", local_qpn, remote_qpn);
            }
        }
    }
}

fn poller_thread(qp: Arc<QueuePair>, buffer_pool: BufferPool, channel: CompletionChannel) {}
