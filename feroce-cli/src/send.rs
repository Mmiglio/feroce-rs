use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use feroce::{
    connection::ConnectionManager,
    protocol::QpConnectionInfo,
    rdma::{
        self,
        device::{Device, MemoryRegion, QueuePair, SendOp},
    },
};

pub fn run(
    bind_addr: IpAddr,
    cm_port: u16,
    remote_addr: IpAddr,
    remote_port: u16,
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

    // create local QP
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
    qp.modify_to_init(port_num)?;

    // connect the CM to gather remote info
    let remote_info = cm.connect(
        SocketAddr::new(remote_addr, remote_port),
        &local_info,
        Duration::from_secs(2),
        2,
    )?;

    qp.modify_to_rtr(&remote_info, gid_index as u8, port_num, active_path_mtu)?;
    qp.modify_to_rts(remote_info.psn)?;

    // post the send WR
    // post send wr
    buf[0] = 0xff;
    buf[1] = 0xde;
    buf[2] = 0xad;

    let mut send_sg_list = Vec::<rdma::ibv_sge>::new();
    send_sg_list.push(rdma::ibv_sge {
        addr: buf.as_mut_ptr() as u64, // mr.addr(),
        length: buf.len() as u32,
        lkey: mr.lkey(),
    });

    qp.post_send(2, &mut send_sg_list, SendOp::Send, true)?;

    // poll the completion queue and hope for the best
    let mut send_wc = Vec::<rdma::ibv_wc>::new();
    send_wc.push(rdma::ibv_wc {
        ..Default::default()
    });
    loop {
        let n = qp.cq().poll(&mut send_wc).unwrap();
        if n > 0 {
            break;
        }
    }

    if send_wc[0].status != rdma::ibv_wc_status::IBV_WC_SUCCESS {
        return Err(format!("recv failed with status {:?}", send_wc[0].status).into());
    }
    println!("Sent message");

    // println!("Local QP: {}", local_info);
    // println!("Remote QP: {}", remote_info);
    Ok(())
}
