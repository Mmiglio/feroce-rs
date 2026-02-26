#![allow(
    non_camel_case_types,
    non_upper_case_globals,
    dead_code,
    non_snake_case
)]

#[repr(C, align(8))]
#[derive(Default, Copy, Clone, Debug)]
pub struct ibv_gid {
    pub raw: [u8; 16],
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ibv_wc {
    pub wr_id: u64,
    pub status: ibv_wc_status,
    pub opcode: ibv_wc_opcode,
    pub vendor_err: u32,
    pub byte_len: u32,
    pub imm_data: u32,
    pub qp_num: u32,
    pub src_qp: u32,
    pub wc_flags: ibv_wc_flags,
    pub pkey_index: u16,
    pub slid: u16,
    pub sl: u8,
    pub dlid_path_bits: u8,
}

impl Default for ibv_wc {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

include!("ffi_generated.rs");

pub unsafe fn ibv_query_port_compact(
    context: *mut ibv_context,
    port_num: u8,
    port_attr: *mut ibv_port_attr,
) -> i32 {
    unsafe { ibv_query_port(context, port_num, port_attr as *mut _compat_ibv_port_attr) }
}
