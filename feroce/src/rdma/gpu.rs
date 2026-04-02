use log::warn;

use crate::rdma::{
    self,
    buffer_pool::BufferAllocator,
    device::{MemoryRegion, ProtectionDomain},
};

// Cuda types
type CUresult = i32;
type CUdeviceptr = u64;
type CUdevice = i32;
type CUcontext = *mut std::ffi::c_void;

// Cuda constants
const CUDA_SUCCESS: CUresult = 0;
const CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD: i32 = 0x1;

unsafe extern "C" {
    fn cuInit(flags: u32) -> CUresult;
    //fn cuCtxSetCurrent(ctx: CUcontext) -> CUresult;
    fn cuDeviceGet(device: *mut CUdevice, ordinal: i32) -> CUresult;
    fn cuCtxCreate_v2(pctx: *mut CUcontext, flags: u32, device: CUdevice) -> CUresult;
    fn cuMemAlloc_v2(dptr: *mut CUdeviceptr, bytesize: usize) -> CUresult;
    fn cuMemFree_v2(dptr: CUdeviceptr) -> CUresult;
    fn cuCtxDestroy_v2(ctx: CUcontext) -> CUresult;
    fn cuMemGetHandleForAddressRange(
        handle: *mut std::ffi::c_void,
        dptr: CUdeviceptr,
        size: usize,
        handle_type: i32, // CU_MEM_HANDLE_TYPE_DMABUF_FD = 0x1
        flags: u64,
    ) -> CUresult;
    fn cuMemcpyDtoH_v2(
        dst_host: *mut std::ffi::c_void,
        src_device: CUdeviceptr,
        byte_count: usize,
    ) -> CUresult;
}

fn check_cuda(result: CUresult, msg: &str) -> Result<(), String> {
    if result != CUDA_SUCCESS {
        Err(format!("{}: CUDA error {}", msg, result))
    } else {
        Ok(())
    }
}

pub fn init_cuda_thread(device_ordinal: i32) -> Result<(), String> {
    check_cuda(unsafe { cuInit(0) }, "cuInit failed")?;
    let mut dev = 0;
    check_cuda(
        unsafe { cuDeviceGet(&mut dev, device_ordinal) },
        "cuDeviceGet failed",
    )?;
    let mut ctx = std::ptr::null_mut();
    check_cuda(
        unsafe { cuCtxCreate_v2(&mut ctx, 0, dev) },
        "cuCtxCreate failed",
    )?;
    Ok(())
}

pub fn copy_device_to_host(dst: &mut [u8], device_addr: u64) -> Result<(), String> {
    check_cuda(
        unsafe {
            cuMemcpyDtoH_v2(
                dst.as_mut_ptr() as *mut std::ffi::c_void,
                device_addr,
                dst.len(),
            )
        },
        "Failed to copy device to host",
    )
}

pub struct CudaContext {
    ctx: CUcontext,
}

unsafe impl Send for CudaContext {}

impl CudaContext {
    pub fn new(device_number: i32) -> Result<Self, String> {
        let mut res = unsafe { cuInit(0) };
        check_cuda(res, "Initialize the CUDA driver API")?;

        let mut dev = 0;
        res = unsafe { cuDeviceGet(&mut dev, device_number) };
        check_cuda(res, "Failed to get CUDA device")?;

        let mut ctx = std::ptr::null_mut();
        res = unsafe { cuCtxCreate_v2(&mut ctx, 0, dev) };
        check_cuda(res, "Failed to create ctx")?;

        Ok(CudaContext { ctx })
    }
}

impl Drop for CudaContext {
    fn drop(&mut self) {
        unsafe {
            cuCtxDestroy_v2(self.ctx);
        }
    }
}

pub struct GpuBuffer {
    dptr: CUdeviceptr,
    size: usize,
    dmabuf_fd: i32,
}

impl GpuBuffer {
    pub fn alloc(size: usize) -> Result<Self, String> {
        // allign to page size
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size <= 0 {
            return Err("Failed to get page size".into());
        }
        let page_size = page_size as usize;
        let alligned_size = size.next_multiple_of(page_size);
        if size != alligned_size {
            warn!(
                "Requested buffer size {} is not alligned with page size {}, allocating {} bytes",
                size, page_size, alligned_size
            );
        }

        let mut dptr = 0;
        let mut res = unsafe { cuMemAlloc_v2(&mut dptr, alligned_size) };
        check_cuda(res, "Failed to allocate memory")?;

        let mut dmabuf_fd = 0;
        res = unsafe {
            cuMemGetHandleForAddressRange(
                &mut dmabuf_fd as *mut i32 as *mut std::ffi::c_void,
                dptr,
                alligned_size,
                CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD,
                0,
            )
        };
        check_cuda(res, "Failed to get DMA BUF fd")?;

        Ok(GpuBuffer {
            dptr,
            size: alligned_size,
            dmabuf_fd,
        })
    }
}

impl Drop for GpuBuffer {
    fn drop(&mut self) {
        unsafe { cuMemFree_v2(self.dptr) };
        unsafe { libc::close(self.dmabuf_fd) };
    }
}

pub struct GpuAllocator {
    _ctx: CudaContext,
}

impl GpuAllocator {
    pub fn new(device_number: i32) -> Result<Self, String> {
        let ctx = CudaContext::new(device_number)?;
        Ok(GpuAllocator { _ctx: ctx })
    }
}

impl BufferAllocator for GpuAllocator {
    type Storage = GpuBuffer;

    fn alloc_and_register(
        &self,
        pd: &ProtectionDomain,
        size: usize,
    ) -> Result<(Self::Storage, MemoryRegion, u64), String> {
        let buff = GpuBuffer::alloc(size)?;
        let base_addr = buff.dptr;

        let access_flags = rdma::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | rdma::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | rdma::ibv_access_flags::IBV_ACCESS_RELAXED_ORDERING;
        let mr = MemoryRegion::register_dmabuf(
            pd,
            0,
            buff.size,
            buff.dptr,
            buff.dmabuf_fd,
            access_flags,
        )?;

        Ok((buff, mr, base_addr))
    }
}

#[cfg(test)]
#[cfg(feature = "gpu")]
mod test {
    use std::sync::Arc;

    use super::*;
    use crate::{
        CompletionChannel, QueuePair,
        protocol::QpConnectionInfo,
        rdma::{device::find_roce_device, ffi, test_utils::*},
    };

    #[test]
    fn alloc_gpu_buffer() {
        let _ctx = CudaContext::new(0).unwrap();

        let buff = GpuBuffer::alloc(1024).unwrap();

        drop(buff);
    }

    #[test]
    fn gpu_allocator_with_rdma() {
        let allocator = GpuAllocator::new(0).unwrap();

        let devices = rdma::device::DeviceList::new().expect("no RDMA devices");
        let name = devices.device_name(0).expect("no devices");
        let device = rdma::device::Device::open(name).expect("failed to open");
        let pd = device.alloc_pd().expect("failed to allocate PD");

        let (buffer, mr, _base_addr) = allocator.alloc_and_register(&pd, 4096).unwrap();
        assert!(mr.lkey() != 0);
        println!(
            "dptr: {:#x}, mr.addr: {:#x}, mr.lkey: {}",
            buffer.dptr,
            mr.addr(),
            mr.lkey()
        );
        drop(buffer);
    }

    #[test]
    fn gpu_direct_loopback() {
        let (_name, device, port, gid_index, mtu) =
            find_roce_device().expect("no RoCE device found");

        // Sender side uses a CPU buffer
        let pd_send = Arc::new(device.alloc_pd().expect("pd_send"));
        let channel_send = CompletionChannel::create(&device).expect("channel_send");
        let cq_send = Arc::new(
            device
                .create_cq_with_channel(16, &channel_send)
                .expect("cq_send"),
        );
        let mut buf_send = vec![0xABu8; 4096];
        let mr_send = MemoryRegion::register(
            &pd_send,
            &mut buf_send,
            rdma::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE,
        )
        .expect("mr_send");
        let qp_send = QueuePair::create_qp(
            Arc::clone(&pd_send),
            Arc::clone(&cq_send),
            8,
            1,
            rdma::ibv_qp_type::IBV_QPT_RC,
        )
        .expect("qp_send");

        // Receiver side with GPU buffer
        let allocator = GpuAllocator::new(0).expect("GpuAllocator");
        let pd_recv = Arc::new(device.alloc_pd().expect("pd_recv"));
        let channel_recv = CompletionChannel::create(&device).expect("channel_recv");
        let cq_recv = Arc::new(
            device
                .create_cq_with_channel(16, &channel_recv)
                .expect("cq_recv"),
        );
        let (gpu_buf, mr_recv, base_addr) = allocator
            .alloc_and_register(&pd_recv, 4096)
            .expect("gpu alloc");
        let qp_recv = QueuePair::create_qp(
            Arc::clone(&pd_recv),
            Arc::clone(&cq_recv),
            8,
            1,
            rdma::ibv_qp_type::IBV_QPT_RC,
        )
        .expect("qp_recv");

        // Connect QPs in loopback
        let loc_gid = device.query_gid(port, gid_index).expect("gid");

        qp_recv.modify_to_init(port).expect("recv init");
        qp_recv
            .modify_to_rtr(
                &QpConnectionInfo {
                    qp_num: qp_send.qp_num(),
                    psn: 0,
                    rkey: mr_recv.rkey(),
                    addr: base_addr,
                    gid: loc_gid.raw,
                },
                gid_index as u8,
                port,
                mtu,
            )
            .expect("recv rtr");
        qp_recv.modify_to_rts(0).expect("recv rts");

        qp_send.modify_to_init(port).expect("send init");
        qp_send
            .modify_to_rtr(
                &QpConnectionInfo {
                    qp_num: qp_recv.qp_num(),
                    psn: 0,
                    rkey: mr_send.rkey(),
                    addr: mr_send.addr(),
                    gid: loc_gid.raw,
                },
                gid_index as u8,
                port,
                mtu,
            )
            .expect("send rtr");
        qp_send.modify_to_rts(0).expect("send rts");

        // Post recv on GPU buffer
        let mut sge_recv = ffi::ibv_sge {
            addr: base_addr,
            length: 4096,
            lkey: mr_recv.lkey(),
        };
        let mut recv_wr = ffi::ibv_recv_wr {
            sg_list: &mut sge_recv,
            num_sge: 1,
            ..Default::default()
        };
        qp_recv.post_recv(&mut recv_wr).expect("post_recv");

        // Post send from CPU buffer
        let mut sge_send = vec![
            ffi::ibv_sge {
                addr: buf_send.as_ptr() as u64,
                length: 4096,
                lkey: mr_send.lkey(),
            };
            1
        ];

        let mut send_wr =
            QueuePair::build_send_wr(0, &mut sge_send, rdma::device::SendOp::Send, true);
        qp_send.post_send(&mut send_wr).expect("post_send");

        // Poll the CQs
        let mut wc = [ffi::ibv_wc::default(); 1];
        let timeout = std::time::Duration::from_secs(5);

        let n = poll_cq_with_timeout(&cq_send, &mut wc, timeout);
        assert_eq!(n, 1, "send completion not received");
        assert_eq!(wc[0].status, ffi::ibv_wc_status::IBV_WC_SUCCESS);

        let n = poll_cq_with_timeout(&cq_recv, &mut wc, timeout);
        assert_eq!(n, 1, "recv completion not received");
        assert_eq!(wc[0].status, ffi::ibv_wc_status::IBV_WC_SUCCESS);

        // Copy data back to host
        let mut result = vec![0u8; 4096];
        copy_device_to_host(&mut result, gpu_buf.dptr).expect("DtoH copy");
        assert_eq!(result, vec![0xABu8; 4096], "GPU buffer content mismatch");
    }
}
