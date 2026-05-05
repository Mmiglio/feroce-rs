use std::sync::Arc;

use crate::{
    protocol::QpConnectionInfo,
    rdma::{self, ffi},
};

use super::device::*;

pub fn create_loopback_qp(
    device: &Arc<Device>,
    port: u8,
    gid_index: i32,
    path_mtu: ffi::ibv_mtu,
    buf_size: usize,
) -> (
    QueuePair,
    Arc<CompletionChannel>,
    MemoryRegion,
    Vec<u8>,
    QueuePair,
    Arc<CompletionChannel>,
    MemoryRegion,
    Vec<u8>,
) {
    // register receiver
    let channel_recv = Arc::new(
        CompletionChannel::create(device).expect("failed to create recv completion channel"),
    );
    let pd_recv = Arc::new(device.alloc_pd().expect("pd_recv"));
    let cq_recv = Arc::new(
        device
            .create_cq_with_channel(16, &channel_recv)
            .expect("cq_recv"),
    );
    let mut buf_recv = vec![0u8; buf_size];
    let mr_recv = MemoryRegion::register(
        &pd_recv,
        &mut buf_recv,
        ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE,
    )
    .expect("mr rcv");
    let qp_recv = QueuePair::create_qp(
        Arc::clone(&pd_recv),
        Arc::clone(&cq_recv),
        8,
        1,
        rdma::ibv_qp_type::IBV_QPT_RC,
    )
    .expect("qp receiver");

    // register sender
    let channel_send = Arc::new(
        CompletionChannel::create(device).expect("failed to create send completion channel"),
    );
    let pd_send = Arc::new(device.alloc_pd().expect("pd_send"));
    let cq_send = Arc::new(
        device
            .create_cq_with_channel(16, &channel_send)
            .expect("cq_send"),
    );
    let mut buf_send = vec![0u8; buf_size];
    let mr_send = MemoryRegion::register(
        &pd_send,
        &mut buf_send,
        ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE,
    )
    .expect("mr rcv");
    let qp_send = QueuePair::create_qp(
        Arc::clone(&pd_send),
        Arc::clone(&cq_send),
        8,
        1,
        rdma::ibv_qp_type::IBV_QPT_RC,
    )
    .expect("qp sender");

    // state transition of qps
    let loc_gid = device.query_gid(port, gid_index).expect("gid");

    qp_recv.modify_to_init(port).expect("recv init");
    qp_recv
        .modify_to_rtr(
            &QpConnectionInfo {
                qp_num: qp_send.qp_num(), // point at the sender
                psn: 0,
                rkey: mr_recv.rkey(),
                addr: mr_recv.addr(),
                gid: loc_gid.raw,
            },
            gid_index as u8,
            port,
            path_mtu,
        )
        .expect("recv rtr");
    qp_recv.modify_to_rts(0).expect("recv rts");

    qp_send.modify_to_init(port).expect("send init");
    qp_send
        .modify_to_rtr(
            &QpConnectionInfo {
                qp_num: qp_recv.qp_num(), // point at the receiver
                psn: 0,
                rkey: mr_send.rkey(),
                addr: mr_send.addr(),
                gid: loc_gid.raw,
            },
            gid_index as u8,
            port,
            path_mtu,
        )
        .expect("send rtr");
    qp_send.modify_to_rts(0).expect("send rts");

    (
        qp_send,
        channel_send,
        mr_send,
        buf_send,
        qp_recv,
        channel_recv,
        mr_recv,
        buf_recv,
    )
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
