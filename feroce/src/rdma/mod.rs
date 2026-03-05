pub mod buffer_pool;
pub mod device;
mod ffi;

pub use ffi::ibv_access_flags;
pub use ffi::ibv_mtu;
pub use ffi::ibv_qp_type;
pub use ffi::ibv_recv_wr;
pub use ffi::ibv_send_wr;
pub use ffi::ibv_sge;
pub use ffi::ibv_wc;
pub use ffi::ibv_wc_status;

pub const IBV_SEND_SIGNALED: u32 = ffi::ibv_send_flags::IBV_SEND_SIGNALED.0;
