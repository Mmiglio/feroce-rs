#[repr(C)]
pub struct ibv_device {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct ibv_context {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct ibv_pd {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct ibv_cq {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct ibv_qp {
    _context: *mut ibv_context,
    _qp_context: *mut core::ffi::c_void,
    _pd: *mut ibv_pd,
    _send_cq: *mut ibv_cq,
    _recv_cq: *mut ibv_cq,
    _srq: *mut core::ffi::c_void,
    _handle: u32,
    pub qp_num: u32,
}

#[repr(C)]
#[derive(Default)]
pub struct ibv_qp_cap {
    pub max_send_wr: u32,
    pub max_recv_wr: u32,
    pub max_send_sge: u32,
    pub max_recv_sge: u32,
    pub max_inline_data: u32,
}

#[repr(i32)]
#[allow(non_camel_case_types)]
#[derive(Copy, Clone)]
pub enum ibv_qp_type {
    IBV_QPT_RC = 2,
    IBV_QPT_UC = 3,
    IBV_QPT_UD = 4,
    IBV_QPT_RAW_PACKET = 8,
    IBV_QPT_XRC_SEND = 9,
    IBV_QPT_XRC_RECV = 10,
    IBV_QPT_DRIVER = 0xff,
}

#[repr(C)]
pub struct ibv_qp_init_attr {
    pub qp_context: *mut core::ffi::c_void,
    pub send_cq: *mut ibv_cq,
    pub recv_cq: *mut ibv_cq,
    pub srq: *mut core::ffi::c_void, // We won't be using SRQ for now, set it to null
    pub cap: ibv_qp_cap,
    pub qp_type: ibv_qp_type,
    pub sq_sig_all: i32,
}

#[repr(i32)]
#[allow(non_camel_case_types, dead_code)]
#[derive(Default)]
pub enum ibv_qp_state {
    #[default]
    IBV_QPS_RESET,
    IBV_QPS_INIT,
    IBV_QPS_RTR,
    IBV_QPS_RTS,
    IBV_QPS_SQD,
    IBV_QPS_SQE,
    IBV_QPS_ERR,
    IBV_QPS_UNKNOWN,
}

#[allow(non_camel_case_types)]
pub struct ibv_qp_attr_mask;
#[allow(dead_code)]
impl ibv_qp_attr_mask {
    pub const IBV_QP_STATE: i32 = 1 << 0;
    pub const IBV_QP_CUR_STATE: i32 = 1 << 1;
    pub const IBV_QP_EN_SQD_ASYNC_NOTIFY: i32 = 1 << 2;
    pub const IBV_QP_ACCESS_FLAGS: i32 = 1 << 3;
    pub const IBV_QP_PKEY_INDEX: i32 = 1 << 4;
    pub const IBV_QP_PORT: i32 = 1 << 5;
    pub const IBV_QP_QKEY: i32 = 1 << 6;
    pub const IBV_QP_AV: i32 = 1 << 7;
    pub const IBV_QP_PATH_MTU: i32 = 1 << 8;
    pub const IBV_QP_TIMEOUT: i32 = 1 << 9;
    pub const IBV_QP_RETRY_CNT: i32 = 1 << 10;
    pub const IBV_QP_RNR_RETRY: i32 = 1 << 11;
    pub const IBV_QP_RQ_PSN: i32 = 1 << 12;
    pub const IBV_QP_MAX_QP_RD_ATOMIC: i32 = 1 << 13;
    pub const IBV_QP_ALT_PATH: i32 = 1 << 14;
    pub const IBV_QP_MIN_RNR_TIMER: i32 = 1 << 15;
    pub const IBV_QP_SQ_PSN: i32 = 1 << 16;
    pub const IBV_QP_MAX_DEST_RD_ATOMIC: i32 = 1 << 17;
    pub const IBV_QP_PATH_MIG_STATE: i32 = 1 << 18;
    pub const IBV_QP_CAP: i32 = 1 << 19;
    pub const IBV_QP_DEST_QPN: i32 = 1 << 20;
    pub const IBV_QP_RATE_LIMIT: i32 = 1 << 25;
}

#[repr(C)]
#[derive(Default)]
pub struct ibv_gid {
    pub raw: [u8; 16],
}

#[repr(i32)]
#[allow(non_camel_case_types)]
#[derive(Default, Copy, Clone)]
pub enum ibv_mtu {
    #[default]
    IBV_MTU_256 = 1,
    IBV_MTU_512 = 2,
    IBV_MTU_1024 = 3,
    IBV_MTU_2048 = 4,
    IBV_MTU_4096 = 5,
}
#[repr(i32)]
#[allow(non_camel_case_types, dead_code)]
#[derive(Default, Copy, Clone)]
pub enum ibv_mig_state {
    #[default]
    IBV_MIG_MIGRATED,
    IBV_MIG_REARM,
    IBV_MIG_ARMED,
}

#[repr(C)]
#[derive(Default)]
pub struct ibv_global_route {
    pub dgid: ibv_gid,
    pub flow_label: u32,
    pub sgid_index: u8,
    pub hop_limit: u8,
    pub traffic_class: u8,
}

#[repr(C)]
#[derive(Default)]
pub struct ibv_ah_attr {
    pub grh: ibv_global_route,
    pub dlid: u16,
    pub sl: u8,
    pub src_path_bits: u8,
    pub static_rate: u8,
    pub is_global: u8,
    pub port_num: u8,
}

#[repr(C)]
#[derive(Default)]
pub struct ibv_qp_attr {
    pub qp_state: ibv_qp_state,
    pub cur_qp_state: ibv_qp_state,
    pub path_mtu: ibv_mtu,
    pub path_mig_state: ibv_mig_state,
    pub qkey: u32,
    pub rq_psn: u32,
    pub sq_psn: u32,
    pub dest_qp_num: u32,
    pub qp_access_flags: u32,
    pub cap: ibv_qp_cap,
    pub ah_attr: ibv_ah_attr,
    pub alt_ah_attr: ibv_ah_attr,
    pub pkey_index: u16,
    pub alt_pkey_index: u16,
    pub en_sqd_async_notify: u8,
    pub sq_draining: u8,
    pub max_rd_atomic: u8,
    pub max_dest_rd_atomic: u8,
    pub min_rnr_timer: u8,
    pub port_num: u8,
    pub timeout: u8,
    pub retry_cnt: u8,
    pub rnr_retry: u8,
    pub alt_port_num: u8,
    pub alt_timeout: u8,
    pub rate_limit: u32,
}

#[repr(i32)]
#[allow(non_camel_case_types, dead_code)]
#[derive(Default, Copy, Clone)]
pub enum ibv_port_state {
    #[default]
    IBV_PORT_NOP = 0,
    IBV_PORT_DOWN = 1,
    IBV_PORT_INIT = 2,
    IBV_PORT_ARMED = 3,
    IBV_PORT_ACTIVE = 4,
    IBV_PORT_ACTIVE_DEFER = 5,
}

#[allow(non_camel_case_types)]
pub struct ibv_link_layer;
#[allow(dead_code)]
impl ibv_link_layer {
    pub const IBV_LINK_LAYER_UNSPECIFIED: u8 = 0;
    pub const IBV_LINK_LAYER_INFINIBAND: u8 = 1;
    pub const IBV_LINK_LAYER_ETHERNET: u8 = 2;
}

#[repr(C)]
#[derive(Default)]
pub struct ibv_port_attr {
    pub state: ibv_port_state,
    pub max_mtu: ibv_mtu,
    pub active_mtu: ibv_mtu,
    pub gid_tbl_len: i32,
    pub port_cap_flags: u32,
    pub max_msg_sz: u32,
    pub bad_pkey_cntr: u32,
    pub qkey_viol_cntr: u32,
    pub pkey_tbl_len: u16,
    pub lid: u16,
    pub sm_lid: u16,
    pub lmc: u8,
    pub max_vl_num: u8,
    pub sm_sl: u8,
    pub subnet_timeout: u8,
    pub init_type_reply: u8,
    pub active_width: u8,
    pub active_speed: u8,
    pub phys_state: u8,
    pub link_layer: u8,
    pub flags: u8,
    pub port_cap_flags2: u16,
    pub active_speed_ex: u32,
}

#[repr(u32)]
#[allow(non_camel_case_types, dead_code)]
#[derive(Copy, Clone)]
pub enum ibv_gid_type {
    IBV_GID_TYPE_IB,
    IBV_GID_TYPE_ROCE_V1,
    IBV_GID_TYPE_ROCE_V2,
}

#[repr(C)]
pub struct ibv_gid_entry {
    pub gid: ibv_gid,
    pub gid_index: u32,
    pub port_num: u32,
    pub gid_type: u32,
    pub ndev_ifindex: u32,
}

unsafe extern "C" {
    pub fn ibv_get_device_list(num_devices: *mut i32) -> *mut *mut ibv_device;
    pub fn ibv_free_device_list(list: *mut *mut ibv_device);
    pub fn ibv_get_device_name(device: *mut ibv_device) -> *const std::os::raw::c_char;
    pub fn ibv_open_device(device: *mut ibv_device) -> *mut ibv_context;
    pub fn ibv_close_device(context: *mut ibv_context) -> i32;
    pub fn ibv_alloc_pd(context: *mut ibv_context) -> *mut ibv_pd;
    pub fn ibv_dealloc_pd(pd: *mut ibv_pd) -> i32;
    pub fn ibv_create_cq(
        context: *mut ibv_context,
        cqe: i32,
        cq_context: *mut std::ffi::c_void,
        channel: *mut std::ffi::c_void,
        comp_vector: i32,
    ) -> *mut ibv_cq;
    pub fn ibv_destroy_cq(cq: *mut ibv_cq) -> i32;
    pub fn ibv_create_qp(pd: *mut ibv_pd, init_attr: *mut ibv_qp_init_attr) -> *mut ibv_qp;
    pub fn ibv_destroy_qp(qp: *mut ibv_qp) -> i32;
    pub fn ibv_modify_qp(qp: *mut ibv_qp, attr: *mut ibv_qp_attr, attr_mask: i32) -> i32;
    pub fn ibv_query_gid(
        context: *mut ibv_context,
        port_num: u8,
        index: i32,
        gid: *mut ibv_gid,
    ) -> i32;
    pub fn ibv_query_port(
        context: *mut ibv_context,
        port_num: u8,
        port_attr: *mut ibv_port_attr,
    ) -> i32;
    pub fn _ibv_query_gid_table(
        context: *mut ibv_context,
        entries: *mut ibv_gid_entry,
        max_entries: usize,
        flags: u32,
        entry_size: usize,
    ) -> isize;
}
