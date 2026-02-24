use feroce::{
    connection::{CmEvent, ConnectionManager},
    protocol::QpConnectionInfo,
    rdma::{
        self,
        device::{Device, MemoryRegion, QueuePair},
    },
};
use std::{net::IpAddr, thread, time::Duration};

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

    let mut cm = ConnectionManager::new(bind_addr, cm_port)?;

    // process a single event connection event
    let cm_event = cm.process_next()?;

    match cm_event {
        CmEvent::NewConnection {
            peer_ip,
            remote_qpn,
            remote_info,
        } => {
            let pd = device.alloc_pd()?;
            let cq = device.create_cq(16)?;

            let qp = QueuePair::create_qp(pd, cq, 4, 1, rdma::ibv_qp_type::IBV_QPT_RC)?;

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
                gid: loc_gid.raw, //gid_from_ipv4(ipv4_to_u32(bind_addr).ok_or("IPv6 not supported")?),
            };

            // transition QPs
            qp.modify_to_init(port_num)?;
            qp.modify_to_rtr(&remote_info, gid_index as u8, port_num, active_path_mtu)?;
            qp.modify_to_rts(remote_info.psn)?;

            // post receive wr before sending ack to sender
            let mut recv_sg_list = Vec::<rdma::ibv_sge>::new();
            recv_sg_list.push(rdma::ibv_sge {
                addr: mr.addr(),
                length: buf.len() as u32,
                lkey: mr.lkey(),
            });
            qp.post_recv(1, &mut recv_sg_list)?;

            // set local info and send ACK
            cm.set_local_info(peer_ip, remote_qpn, &local_info)?;

            // println!("Local QP: {}", local_info);
            // println!("Remote QP: {}", remote_info);

            // start polling the CQ
            let mut recv_wc = Vec::<rdma::ibv_wc>::new();
            recv_wc.push(rdma::ibv_wc {
                ..Default::default()
            });
            loop {
                let n = qp.cq().poll(&mut recv_wc).unwrap();
                if n > 0 {
                    break;
                }
                thread::sleep(Duration::from_millis(1)); // avoid burning CPU 
            }

            if recv_wc[0].status != rdma::ibv_wc_status::IBV_WC_SUCCESS {
                return Err(format!("recv failed with status {:?}", recv_wc[0].status).into());
            }
            println!("Received message!\nSize: {}", recv_wc[0].byte_len);
            println!("\tFirst 8 bytes: {:02x?}", &buf[..8]);
        }
        CmEvent::CloseQp { .. } => todo!(),
    }

    Ok(())
}
