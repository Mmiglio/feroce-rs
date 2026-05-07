use std::sync::Arc;

use crate::{
    FeroceError,
    protocol::QpConnectionInfo,
    rdma::{
        self,
        buffer_pool::{BufferAllocator, BufferPool},
        device::{CompletionChannel, Device, LocalLink, PreparedQueuePair, QueuePair},
    },
};

pub struct RdmaConfig {
    pub num_buf: usize,
    pub buf_size: usize,
}

// QP state generic endpoint: Q is either PreparedQueuePair (post-INIT,
// pre-handshake) or Arc<QueuePair> (post-handshake, ready for traffic).
pub struct RdmaEndpoint<A: BufferAllocator, Q> {
    pub qp: Q,
    pub comp_channel: Arc<CompletionChannel>,
    pub buffer_pool: BufferPool<A>,
    pub local_info: QpConnectionInfo,
}

pub type PreparedRdmaEndpoint<A> = RdmaEndpoint<A, PreparedQueuePair>;
pub type ConnectedRdmaEndpoint<A> = RdmaEndpoint<A, Arc<QueuePair>>;

// Create and init resources for a qp/data stream
pub fn setup_endpoint<A: BufferAllocator>(
    device: &Arc<Device>,
    link: LocalLink,
    rdma_cfg: &RdmaConfig,
    allocator: &A,
) -> Result<PreparedRdmaEndpoint<A>, FeroceError> {
    let comp_channel = Arc::new(CompletionChannel::create(device)?);

    // create local QP. single CQ shared between send and recv (for now)
    let pd = Arc::new(device.alloc_pd()?);
    let cq = Arc::new(device.create_cq_with_channel(rdma_cfg.num_buf as i32, &comp_channel)?);

    let prepared = pd
        .create_qp(&cq, &cq, rdma::ibv_qp_type::IBV_QPT_RC, link)
        .set_max_wr(rdma_cfg.num_buf as u32)
        .set_max_sge(1)
        .build()?;

    let loc_gid = device.query_gid(link.port_num, link.gid_index as i32)?;
    let buffer_pool = BufferPool::new(rdma_cfg.num_buf, rdma_cfg.buf_size, &pd, allocator)?;

    let local_info = QpConnectionInfo {
        qp_num: prepared.qp_num(),
        psn: 0,
        rkey: buffer_pool.rkey(),
        addr: buffer_pool.addr(),
        gid: loc_gid.raw,
    };

    Ok(RdmaEndpoint {
        qp: prepared,
        comp_channel,
        buffer_pool,
        local_info,
    })
}

pub fn connect_endpoint<A: BufferAllocator>(
    endpoint: PreparedRdmaEndpoint<A>,
    remote_info: &QpConnectionInfo,
) -> Result<ConnectedRdmaEndpoint<A>, FeroceError> {
    let qp = endpoint.qp.handshake(remote_info)?;
    Ok(RdmaEndpoint {
        qp: Arc::new(qp),
        comp_channel: endpoint.comp_channel,
        buffer_pool: endpoint.buffer_pool,
        local_info: endpoint.local_info,
    })
}
