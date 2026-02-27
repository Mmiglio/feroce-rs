use std::collections::VecDeque;

use crate::rdma::{self, device::MemoryRegion, device::ProtectionDomain};

pub struct BufferHandle {
    pub index: usize,
    pub addr: *mut u8,
    pub len: usize,
    pub lkey: u32,
}

// Needed to move handle between channels.
// It is actually safe because the pointer targets pinned registered memory (mr)
// and only one thread holds the handle at a time
unsafe impl Send for BufferHandle {}

pub struct BufferPool {
    num_buf: usize,
    buf_size: usize,

    mr: MemoryRegion,
    data: Vec<u8>,
    free_bufs: VecDeque<usize>,
}

unsafe impl Send for BufferPool {}

impl BufferPool {
    pub fn new(num_buf: usize, buf_size: usize, pd: &ProtectionDomain) -> Result<Self, String> {
        let mut data = vec![0u8; num_buf * buf_size];

        // register RDMA MR for the base buffer
        let access_flags = rdma::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | rdma::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE;
        let mr = MemoryRegion::register(pd, &mut data, access_flags)?;

        let free_bufs = VecDeque::from_iter(0..num_buf);

        Ok(BufferPool {
            num_buf,
            buf_size,
            mr,
            data,
            free_bufs,
        })
    }

    pub fn num_buf(&self) -> usize {
        self.num_buf
    }

    pub fn buf_size(&self) -> usize {
        self.buf_size
    }

    pub fn num_free_bufs(&self) -> usize {
        self.free_bufs.len()
    }

    pub fn lkey(&self) -> u32 {
        self.mr.lkey()
    }

    pub fn rkey(&self) -> u32 {
        self.mr.rkey()
    }

    pub fn addr(&self) -> u64 {
        self.mr.addr()
    }

    pub fn get_handle(&self, index: usize) -> BufferHandle {
        debug_assert!(
            index < self.num_buf,
            "buffer index out of range (idx={}, num_bufs={})",
            index,
            self.num_buf
        );
        let addr = unsafe { (self.data.as_ptr() as *mut u8).add(index * self.buf_size) };
        BufferHandle {
            index,
            addr,
            len: self.buf_size,
            lkey: self.mr.lkey(),
        }
    }

    pub fn get_buffer(&mut self) -> Option<BufferHandle> {
        self.free_bufs.pop_front().map(|idx| self.get_handle(idx))
    }

    pub fn release_buffer(&mut self, buf_handle: BufferHandle) {
        self.free_bufs.push_back(buf_handle.index);
    }
}

#[cfg(test)]
#[cfg(feature = "rdma-test")]
mod test {
    use super::*;
    use crate::rdma;

    #[test]
    fn basic_functionalities() {
        let devices = rdma::device::DeviceList::new().expect("no RDMA devices found");
        let name = devices.device_name(0).expect("no devices");
        let device = rdma::device::Device::open(name).expect("failed to open device");

        let pd = device.alloc_pd().expect("failed to allocate PD");

        let mut buf_pool = BufferPool::new(2, 128, &pd).expect("failed to create buffer pool");

        let Some(buf_handle) = buf_pool.get_buffer() else {
            panic!("expect at least one buffer")
        };
        assert_eq!(buf_handle.index, 0);
        assert_eq!(buf_pool.num_free_bufs(), 1);

        let Some(buf_handle_new) = buf_pool.get_buffer() else {
            panic!("expect at least one buffer")
        };

        assert_ne!(buf_handle.addr, buf_handle_new.addr);
        assert_eq!(buf_handle_new.addr as usize - buf_handle.addr as usize, 128);

        // no more buffers
        assert!(buf_pool.get_buffer().is_none());

        buf_pool.release_buffer(buf_handle);
        assert_eq!(buf_pool.num_free_bufs(), 1);
    }
}
