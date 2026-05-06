use std::sync::Arc;

use crate::{
    protocol::QpConnectionInfo,
    rdma::{self, ffi},
};

use super::device::*;

pub struct LoopbackEndpoint {
    pub qp: QueuePair,
    pub channel: Arc<CompletionChannel>,
    pub mr: MemoryRegion,
    pub buf: Vec<u8>,
}

pub struct LoopbackPair {
    pub send: LoopbackEndpoint,
    pub recv: LoopbackEndpoint,
}

fn build_endpoint(device: &Arc<Device>, buf_size: usize, label: &str) -> LoopbackEndpoint {
    let channel = Arc::new(
        CompletionChannel::create(device)
            .unwrap_or_else(|_| panic!("failed to create {} completion channel", label)),
    );
    let pd = Arc::new(device.alloc_pd().unwrap_or_else(|_| panic!("pd_{}", label)));
    let cq = Arc::new(
        device
            .create_cq_with_channel(16, &channel)
            .unwrap_or_else(|_| panic!("cq_{}", label)),
    );
    let mut buf = vec![0u8; buf_size];
    let mr = MemoryRegion::register(
        &pd,
        &mut buf,
        ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE,
    )
    .unwrap_or_else(|_| panic!("mr_{}", label));
    let qp = QueuePair::create_qp(
        Arc::clone(&pd),
        Arc::clone(&cq),
        8,
        1,
        rdma::ibv_qp_type::IBV_QPT_RC,
    )
    .unwrap_or_else(|_| panic!("qp_{}", label));

    LoopbackEndpoint {
        qp,
        channel,
        mr,
        buf,
    }
}

pub fn create_loopback_qp(
    device: &Arc<Device>,
    port: u8,
    gid_index: i32,
    path_mtu: ffi::ibv_mtu,
    buf_size: usize,
) -> LoopbackPair {
    let recv = build_endpoint(device, buf_size, "recv");
    let send = build_endpoint(device, buf_size, "send");

    let loc_gid = device.query_gid(port, gid_index).expect("gid");

    recv.qp.modify_to_init(port).expect("recv init");
    recv.qp
        .modify_to_rtr(
            &QpConnectionInfo {
                qp_num: send.qp.qp_num(),
                psn: 0,
                rkey: recv.mr.rkey(),
                addr: recv.mr.addr(),
                gid: loc_gid.raw,
            },
            gid_index as u8,
            port,
            path_mtu,
        )
        .expect("recv rtr");
    recv.qp.modify_to_rts(0).expect("recv rts");

    send.qp.modify_to_init(port).expect("send init");
    send.qp
        .modify_to_rtr(
            &QpConnectionInfo {
                qp_num: recv.qp.qp_num(),
                psn: 0,
                rkey: send.mr.rkey(),
                addr: send.mr.addr(),
                gid: loc_gid.raw,
            },
            gid_index as u8,
            port,
            path_mtu,
        )
        .expect("send rtr");
    send.qp.modify_to_rts(0).expect("send rts");

    LoopbackPair { send, recv }
}

pub fn poll_cq_with_timeout(
    cq: &CompletionQueue,
    wc: &mut [ffi::ibv_wc],
    timeout: std::time::Duration,
) -> usize {
    let start = std::time::Instant::now();
    let mut n = 0;
    while start.elapsed() < timeout {
        n = cq.poll(wc).unwrap();
        if n > 0 {
            break;
        }
    }
    n
}

pub fn make_sge_list(n: usize, buf: &mut Vec<u8>, lkey: u32) -> Vec<ffi::ibv_sge> {
    let sg_list = vec![
        ffi::ibv_sge {
            addr: buf.as_mut_ptr() as u64,
            length: buf.len() as u32,
            lkey,
        };
        n
    ];

    sg_list
}
