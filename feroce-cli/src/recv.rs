use feroce::{
    connection::{CmEvent, ConnectionError, ConnectionManager},
    protocol::QpConnectionInfo,
    rdma::{
        self,
        device::{Device, MemoryRegion, QueuePair},
    },
};
use std::{collections::HashMap, net::IpAddr, sync::Arc};

#[allow(dead_code, unused)]
struct QpContext {
    qp: QueuePair,
    mr: MemoryRegion,
    buf: Vec<u8>,
}

pub fn run(
    bind_addr: IpAddr,
    cm_port: u16,
    rdma_device: String,
    gid_index: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let port_num = 1; // move to configs...
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
                let pd = Arc::new(device.alloc_pd()?);
                let cq = Arc::new(device.create_cq(16)?);

                let qp = QueuePair::create_qp(
                    Arc::clone(&pd),
                    Arc::clone(&cq),
                    4,
                    1,
                    rdma::ibv_qp_type::IBV_QPT_RC,
                )?;

                let mut buf = vec![0u8; 64];
                let mr = MemoryRegion::register(
                    qp.pd(),
                    &mut buf,
                    rdma::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                        | rdma::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE,
                )?;

                let loc_gid = device.query_gid(port_num, gid_index)?;
                // register local infos
                let local_info = QpConnectionInfo {
                    qp_num: qp.qp_num(),
                    psn: 0,
                    rkey: mr.rkey(),
                    addr: mr.addr(),
                    gid: loc_gid.raw,
                };

                // transition QPs
                qp.modify_to_init(port_num)?;
                qp.modify_to_rtr(&remote_info, gid_index as u8, port_num, active_path_mtu)?;
                qp.modify_to_rts(remote_info.psn)?;

                qps.insert(qp.qp_num(), QpContext { qp, mr, buf });

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
