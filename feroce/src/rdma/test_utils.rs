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

struct PreparedLoopback {
    qp: PreparedQueuePair,
    channel: Arc<CompletionChannel>,
    mr: MemoryRegion,
    buf: Vec<u8>,
}

fn build_endpoint(
    device: &Arc<Device>,
    link: LocalLink,
    buf_size: usize,
    label: &str,
) -> PreparedLoopback {
    let channel = Arc::new(
        CompletionChannel::create(device).unwrap_or_else(|e| panic!("channel_{}: {}", label, e)),
    );
    let pd = Arc::new(
        device
            .alloc_pd()
            .unwrap_or_else(|e| panic!("pd_{}: {}", label, e)),
    );
    let cq = Arc::new(
        device
            .create_cq_with_channel(16, &channel)
            .unwrap_or_else(|e| panic!("cq_{}: {}", label, e)),
    );
    let mut buf = vec![0u8; buf_size];
    let mr = MemoryRegion::register(
        &pd,
        &mut buf,
        ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE,
    )
    .unwrap_or_else(|e| panic!("mr_{}: {}", label, e));

    let qp = pd
        .create_qp(&cq, &cq, rdma::ibv_qp_type::IBV_QPT_RC, link)
        .set_max_wr(8)
        .build()
        .unwrap_or_else(|e| panic!("qp_{}: {}", label, e));

    PreparedLoopback {
        qp,
        channel,
        mr,
        buf,
    }
}

pub fn create_loopback_qp(device: &Arc<Device>, link: LocalLink, buf_size: usize) -> LoopbackPair {
    let recv_p = build_endpoint(device, link, buf_size, "recv");
    let send_p = build_endpoint(device, link, buf_size, "send");

    let loc_gid = device
        .query_gid(link.port_num, link.gid_index as i32)
        .expect("gid");

    let recv_qpn = recv_p.qp.qp_num();
    let send_qpn = send_p.qp.qp_num();

    let recv_qp = recv_p
        .qp
        .handshake(&QpConnectionInfo {
            qp_num: send_qpn,
            psn: 0,
            rkey: recv_p.mr.rkey(),
            addr: recv_p.mr.addr(),
            gid: loc_gid.raw,
        })
        .expect("recv handshake");

    let send_qp = send_p
        .qp
        .handshake(&QpConnectionInfo {
            qp_num: recv_qpn,
            psn: 0,
            rkey: send_p.mr.rkey(),
            addr: send_p.mr.addr(),
            gid: loc_gid.raw,
        })
        .expect("send handshake");

    LoopbackPair {
        recv: LoopbackEndpoint {
            qp: recv_qp,
            channel: recv_p.channel,
            mr: recv_p.mr,
            buf: recv_p.buf,
        },
        send: LoopbackEndpoint {
            qp: send_qp,
            channel: send_p.channel,
            mr: send_p.mr,
            buf: send_p.buf,
        },
    }
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
