use std::sync::Arc;

use crate::{
    protocol::QpConnectionInfo,
    rdma::{
        self,
        buffer_pool::{BufferAllocator, BufferPool},
        device::{CompletionChannel, Device, QueuePair},
    },
};

pub struct RdmaConfig {
    pub port_num: u8,
    pub gid_index: i32,
    pub num_buf: usize,
    pub buf_size: usize,
}

pub struct RdmaEndpoint<A: BufferAllocator> {
    pub qp: Arc<QueuePair>,
    pub comp_channel: CompletionChannel,
    pub buffer_pool: BufferPool<A>,
    pub local_info: QpConnectionInfo,
}

// Create and init resources for a qp/data stream
pub fn setup_endpoint<A: BufferAllocator>(
    device: &Device,
    rdma_cfg: &RdmaConfig,
    allocator: &A,
) -> Result<RdmaEndpoint<A>, Box<dyn std::error::Error>> {
    let comp_channel = CompletionChannel::create(device)?;

    // create local QP
    let pd = Arc::new(device.alloc_pd()?);
    let cq = Arc::new(device.create_cq_with_channel(rdma_cfg.num_buf as i32, &comp_channel)?);

    let qp = Arc::new(QueuePair::create_qp(
        Arc::clone(&pd),
        Arc::clone(&cq),
        rdma_cfg.num_buf as u32,
        1,
        rdma::ibv_qp_type::IBV_QPT_RC,
    )?);

    let loc_gid = device.query_gid(rdma_cfg.port_num, rdma_cfg.gid_index)?;
    let buffer_pool = BufferPool::new(rdma_cfg.num_buf, rdma_cfg.buf_size, &pd, allocator)?;

    // register local infos
    let local_info = QpConnectionInfo {
        qp_num: qp.qp_num(),
        psn: 0,
        rkey: buffer_pool.rkey(),
        addr: buffer_pool.addr(),
        gid: loc_gid.raw,
    };

    qp.modify_to_init(rdma_cfg.port_num)?;

    Ok(RdmaEndpoint {
        qp,
        comp_channel,
        buffer_pool,
        local_info,
    })
}

pub fn connect_endpoint<A: BufferAllocator>(
    endpoint: &RdmaEndpoint<A>,
    remote_info: &QpConnectionInfo,
    rdma_cfg: &RdmaConfig,
    active_path_mtu: rdma::ibv_mtu,
) -> Result<(), Box<dyn std::error::Error>> {
    endpoint.qp.modify_to_rtr(
        remote_info,
        rdma_cfg.gid_index as u8,
        rdma_cfg.port_num,
        active_path_mtu,
    )?;
    endpoint.qp.modify_to_rts(remote_info.psn)?;
    Ok(())
}
