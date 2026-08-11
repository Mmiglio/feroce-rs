use log::{debug, error, info};

use super::ffi;
use crate::FeroceError;
use crate::protocol::QpConnectionInfo;
use std::sync::Arc;

fn rdma_err(call: &'static str) -> FeroceError {
    FeroceError::Rdma {
        call,
        errno: std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
    }
}

fn rdma_err_code(call: &'static str, errno: i32) -> FeroceError {
    FeroceError::Rdma { call, errno }
}

pub struct DeviceList {
    list: *mut *mut ffi::ibv_device,
    count: i32,
}

impl DeviceList {
    pub fn new() -> Result<Self, FeroceError> {
        let mut count: i32 = 0;
        let list = unsafe { ffi::ibv_get_device_list(&mut count) };

        if list.is_null() {
            Err(rdma_err("ibv_get_device_list"))
        } else {
            debug!("Found {} rdma devices", count);
            Ok(DeviceList { list, count })
        }
    }

    pub fn len(&self) -> usize {
        self.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn raw_device(&self, i: usize) -> Option<*mut ffi::ibv_device> {
        if i >= self.len() {
            return None;
        }
        unsafe { Some(*self.list.add(i)) }
    }

    // get the name for device at index i
    pub fn device_name(&self, i: usize) -> Option<&str> {
        if i >= self.len() {
            return None;
        }
        unsafe {
            let device = *self.list.add(i);
            let name_ptr = ffi::ibv_get_device_name(device);
            if name_ptr.is_null() {
                return None;
            }
            std::ffi::CStr::from_ptr(name_ptr).to_str().ok()
        }
    }
}

impl Drop for DeviceList {
    fn drop(&mut self) {
        unsafe {
            ffi::ibv_free_device_list(self.list);
        }
    }
}

pub struct Device {
    context: *mut ffi::ibv_context,
}

unsafe impl Send for Device {}
unsafe impl Sync for Device {}

impl Device {
    pub fn open(name: &str) -> Result<Self, FeroceError> {
        let devices = DeviceList::new()?;

        for i in 0..devices.len() {
            if devices.device_name(i) == Some(name) {
                let device_ptr = devices
                    .raw_device(i)
                    .ok_or(FeroceError::InvalidArg("unable to get device".to_string()))?;
                let context = unsafe { ffi::ibv_open_device(device_ptr) };
                if context.is_null() {
                    return Err(rdma_err("ibv_open_device"));
                } else {
                    debug!("Opened device {}", name);
                    return Ok(Device { context });
                }
            }
        }
        Err(FeroceError::InvalidArg(format!(
            "device '{}' not found",
            name
        )))
    }

    pub fn alloc_pd(self: &Arc<Self>) -> Result<ProtectionDomain, FeroceError> {
        let pd = unsafe { ffi::ibv_alloc_pd(self.context) };
        if pd.is_null() {
            Err(rdma_err("ibv_alloc_pd"))
        } else {
            Ok(ProtectionDomain {
                pd,
                _device: Arc::clone(self),
            })
        }
    }

    pub fn create_cq(self: &Arc<Self>, cqe: i32) -> Result<CompletionQueue, FeroceError> {
        let cq = unsafe {
            ffi::ibv_create_cq(
                self.context,
                cqe,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0,
            )
        };

        if cq.is_null() {
            Err(rdma_err("ibv_create_cq"))
        } else {
            Ok(CompletionQueue {
                cq,
                _device: Arc::clone(self),
                _channel: None,
            })
        }
    }

    pub fn create_cq_with_channel(
        self: &Arc<Self>,
        cqe: i32,
        comp_channel: &Arc<CompletionChannel>,
    ) -> Result<CompletionQueue, FeroceError> {
        let cq = unsafe {
            ffi::ibv_create_cq(
                self.context,
                cqe,
                std::ptr::null_mut(),
                comp_channel.channel,
                0,
            )
        };

        if cq.is_null() {
            Err(rdma_err("ibv_create_cq"))
        } else {
            Ok(CompletionQueue {
                cq,
                _device: Arc::clone(self),
                _channel: Some(Arc::clone(comp_channel)),
            })
        }
    }

    pub fn query_gid(&self, port_num: u8, gid_index: i32) -> Result<ffi::ibv_gid, FeroceError> {
        let mut gid = ffi::ibv_gid::default();
        let ret = unsafe { ffi::ibv_query_gid(self.context, port_num, gid_index, &mut gid) };

        if ret != 0 {
            Err(rdma_err_code("ibv_query_gid", ret))
        } else {
            Ok(gid)
        }
    }

    pub fn query_port(&self, port_num: u8) -> Result<ffi::ibv_port_attr, FeroceError> {
        let mut attr = ffi::ibv_port_attr::default();

        let ret = unsafe { ffi::ibv_query_port_compact(self.context, port_num, &mut attr) };

        if ret != 0 {
            Err(rdma_err_code("ibv_query_port_compact", ret))
        } else {
            Ok(attr)
        }
    }

    pub fn query_gid_table(&self) -> Result<Vec<ffi::ibv_gid_entry>, FeroceError> {
        let max_entries = 64;
        let mut entries = Vec::with_capacity(max_entries);
        let ret = unsafe {
            ffi::_ibv_query_gid_table(
                self.context,
                entries.as_mut_ptr(),
                max_entries,
                0,
                std::mem::size_of::<ffi::ibv_gid_entry>(),
            )
        };

        if ret < 0 {
            Err(rdma_err_code("ibv_query_gid_table", ret as i32))
        } else {
            unsafe {
                entries.set_len(ret as usize);
                Ok(entries)
            }
        }
    }

    // Validate that the given (port, gid_index) refers to an active RoCEv2
    // endpoint and return a localLink
    pub fn probe_link(
        &self,
        port_num: u8,
        gid_index: u8,
    ) -> Result<Option<LocalLink>, FeroceError> {
        let port_attr = self.query_port(port_num)?;
        let gid_entries = self.query_gid_table()?;

        for entry in &gid_entries {
            if entry.port_num == port_num as u32
                && entry.gid_index == gid_index as u32
                && entry.gid_type == ffi::ibv_gid_type::IBV_GID_TYPE_ROCE_V2 as u32
                && entry.gid.raw.iter().any(|&b| b != 0)
                && entry.gid.raw[10] == 0xff // look for ipv4-mapped GIDs
                && entry.gid.raw[11] == 0xff
                && port_attr.link_layer == ffi::IBV_LINK_LAYER_ETHERNET as u8
                && matches!(port_attr.state, ffi::ibv_port_state::IBV_PORT_ACTIVE)
            {
                return Ok(Some(LocalLink {
                    port_num,
                    gid_index,
                    mtu: port_attr.active_mtu,
                }));
            }
        }

        Ok(None)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LocalLink {
    pub port_num: u8,
    pub gid_index: u8,
    pub mtu: ffi::ibv_mtu,
}

impl Drop for Device {
    fn drop(&mut self) {
        let ret = unsafe { ffi::ibv_close_device(self.context) };
        if ret != 0 {
            error!("ibv_close_device failed: errno={}", ret);
        }
    }
}

pub struct CompletionChannel {
    channel: *mut ffi::ibv_comp_channel,
    _device: Arc<Device>,
}

unsafe impl Send for CompletionChannel {}
unsafe impl Sync for CompletionChannel {}

impl CompletionChannel {
    pub fn create(device: &Arc<Device>) -> Result<Self, FeroceError> {
        let ch = unsafe { ffi::ibv_create_comp_channel(device.context) };
        if ch.is_null() {
            Err(rdma_err("ibv_create_comp_channel"))
        } else {
            Ok(CompletionChannel {
                channel: ch,
                _device: Arc::clone(device),
            })
        }
    }

    // Block until a completion event is available on the channel.
    // Returns a CqEvent guard, dropping it acks the event
    pub fn get_cq_event(&self) -> Result<CqEvent, FeroceError> {
        let mut cq: *mut ffi::ibv_cq = std::ptr::null_mut();
        let mut cq_ctx: *mut std::ffi::c_void = std::ptr::null_mut();

        let ret = unsafe { ffi::ibv_get_cq_event(self.channel, &mut cq, &mut cq_ctx) };

        if ret != 0 {
            Err(rdma_err_code("ibv_get_cq_event", ret))
        } else {
            Ok(CqEvent { cq })
        }
    }

    // Wrapper around get_cq_event() with a timeout.
    // Checks if data is available on the comp channel fd using poll(),
    // then calls get_cq_event(), which is therefore non-blocking.
    pub fn try_get_cq_event(&self, timeout_ms: i32) -> Result<Option<CqEvent>, FeroceError> {
        let fd = unsafe { (*self.channel).fd };

        // we want to monitor only the comp channel fd
        let mut pfd = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };

        // check if there is an event (pollin) in the fd
        let ret = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
        match ret {
            -1 => {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    // interrupted by a signal
                    return Ok(None);
                }
                return Err(rdma_err("poll"));
            }
            0 => return Ok(None), // timeout
            _ => {}               // proceed, data are available
        }

        // now get_cq_event won't block since poll flagged that there are data
        Ok(Some(self.get_cq_event()?))
    }
}

// consumed completion-channel event that acks itself when dropped
pub struct CqEvent {
    cq: *mut ffi::ibv_cq,
}

impl Drop for CqEvent {
    fn drop(&mut self) {
        unsafe { ffi::ibv_ack_cq_events(self.cq, 1) };
    }
}

impl Drop for CompletionChannel {
    fn drop(&mut self) {
        let ret = unsafe { ffi::ibv_destroy_comp_channel(self.channel) };
        if ret != 0 {
            error!("ibv_destroy_comp_channel failed: errno={}", ret);
        }
    }
}

pub struct ProtectionDomain {
    pd: *mut ffi::ibv_pd,
    _device: Arc<Device>,
}

unsafe impl Send for ProtectionDomain {}
unsafe impl Sync for ProtectionDomain {}

impl ProtectionDomain {
    pub fn raw(&self) -> *mut ffi::ibv_pd {
        self.pd
    }

    // start building a QP on this protection domain, creates a QpBuilder
    pub fn create_qp(
        self: &Arc<Self>,
        send_cq: &Arc<CompletionQueue>,
        recv_cq: &Arc<CompletionQueue>,
        qp_type: ffi::ibv_qp_type,
        link: LocalLink,
    ) -> QpBuilder {
        QpBuilder::new(
            Arc::clone(self),
            Arc::clone(send_cq),
            Arc::clone(recv_cq),
            qp_type,
            link,
        )
    }
}

impl Drop for ProtectionDomain {
    fn drop(&mut self) {
        let ret = unsafe { ffi::ibv_dealloc_pd(self.pd) };
        if ret != 0 {
            error!("ibv_dealloc_pd failed: errno={}", ret);
        }
    }
}

pub struct CompletionQueue {
    cq: *mut ffi::ibv_cq,
    _device: Arc<Device>,
    _channel: Option<Arc<CompletionChannel>>,
}

unsafe impl Send for CompletionQueue {}
unsafe impl Sync for CompletionQueue {}

impl CompletionQueue {
    pub fn raw(&self) -> *mut ffi::ibv_cq {
        self.cq
    }

    pub fn poll(&self, completions: &mut [ffi::ibv_wc]) -> Result<usize, FeroceError> {
        let ctx = unsafe { (*self.cq).context };
        let poll_fn = unsafe { (*ctx).ops.poll_cq.unwrap() };
        let num_completions =
            unsafe { poll_fn(self.cq, completions.len() as i32, completions.as_mut_ptr()) };

        if num_completions < 0 {
            Err(rdma_err_code("ibv_poll_cq", num_completions))
        } else {
            Ok(num_completions as usize)
        }
    }

    pub fn req_notify_cq(&self, solicited: bool) -> Result<(), FeroceError> {
        let ctx = unsafe { (*self.cq).context };
        let req_notify_cq_fn = unsafe { (*ctx).ops.req_notify_cq.unwrap() };
        let ret = unsafe { req_notify_cq_fn(self.cq, solicited as i32) };

        if ret != 0 {
            Err(rdma_err_code("ibv_req_notify_cq", ret))
        } else {
            Ok(())
        }
    }
}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        let ret = unsafe { ffi::ibv_destroy_cq(self.cq) };
        if ret != 0 {
            error!("ibv_destroy_cq failed: errno={}", ret);
        }
    }
}

pub enum SendOp {
    Send,
    SendWithImm(u32),
    Write {
        remote_addr: u64,
        rkey: u32,
    },
    WriteWithImm {
        remote_addr: u64,
        rkey: u32,
        imm_data: u32,
    },
}

pub(crate) struct RtrParams {
    pub min_rnr_timer: u8,
    pub max_dest_rd_atomic: u8,
    pub hop_limit: u8,
    pub traffic_class: u8,
}

impl Default for RtrParams {
    fn default() -> Self {
        Self {
            min_rnr_timer: 8,
            max_dest_rd_atomic: 1,
            hop_limit: 1,
            traffic_class: 96,
        }
    }
}

pub(crate) struct RtsParams {
    pub timeout: u8,
    pub retry_cnt: u8,
    pub rnr_retry: u8,
    pub max_rd_atomic: u8,
}

impl Default for RtsParams {
    fn default() -> Self {
        Self {
            timeout: 14,
            retry_cnt: 7,
            rnr_retry: 7,
            max_rd_atomic: 1,
        }
    }
}

pub struct QueuePair {
    qp: *mut ffi::ibv_qp,
    _pd: Arc<ProtectionDomain>,
    _send_cq: Arc<CompletionQueue>,
    _recv_cq: Arc<CompletionQueue>,
}

unsafe impl Send for QueuePair {}
unsafe impl Sync for QueuePair {}

impl QueuePair {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_qp(
        pd: Arc<ProtectionDomain>,
        send_cq: Arc<CompletionQueue>,
        recv_cq: Arc<CompletionQueue>,
        max_send_wr: u32,
        max_recv_wr: u32,
        max_send_sge: u32,
        max_recv_sge: u32,
        max_inline_data: u32,
        qp_type: ffi::ibv_qp_type,
    ) -> Result<Self, FeroceError> {
        let mut init_attr = ffi::ibv_qp_init_attr {
            qp_context: std::ptr::null_mut(),
            send_cq: send_cq.raw(),
            recv_cq: recv_cq.raw(),
            srq: std::ptr::null_mut(),
            cap: ffi::ibv_qp_cap {
                max_send_wr,
                max_recv_wr,
                max_send_sge,
                max_recv_sge,
                max_inline_data,
            },
            qp_type,
            sq_sig_all: 0,
        };

        let qp = unsafe { ffi::ibv_create_qp(pd.raw(), &mut init_attr) };
        if qp.is_null() {
            Err(rdma_err("ibv_create_qp"))
        } else {
            debug!(
                "Created QP {}, max send WR={}, max recv WR={}, max send SGE={}, max recv SGE={}",
                unsafe { (*qp).qp_num },
                max_send_wr,
                max_recv_wr,
                max_send_sge,
                max_recv_sge
            );
            Ok(QueuePair {
                qp,
                _pd: pd,
                _send_cq: send_cq,
                _recv_cq: recv_cq,
            })
        }
    }

    pub fn qp_num(&self) -> u32 {
        unsafe { (*self.qp).qp_num }
    }

    pub(crate) fn modify_to_init(&self, port_num: u8) -> Result<(), FeroceError> {
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_INIT,
            port_num,
            qp_access_flags: (ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE)
                .0,
            ..Default::default()
        };
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE
            | ffi::ibv_qp_attr_mask::IBV_QP_PORT
            | ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;

        let ret = unsafe { ffi::ibv_modify_qp(self.qp, &mut attr, mask.0 as i32) };
        if ret != 0 {
            Err(rdma_err_code("ibv_modify_qp[INIT]", ret))
        } else {
            debug!("QP {} modified from RESET to INIT", self.qp_num());
            Ok(())
        }
    }

    pub(crate) fn modify_to_rtr(
        &self,
        remote_info: &QpConnectionInfo,
        gid_index: u8,
        port_num: u8,
        mtu: ffi::ibv_mtu,
        params: &RtrParams,
    ) -> Result<(), FeroceError> {
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RTR,
            path_mtu: mtu,
            dest_qp_num: remote_info.qp_num,
            rq_psn: remote_info.psn,
            max_dest_rd_atomic: params.max_dest_rd_atomic,
            min_rnr_timer: params.min_rnr_timer,
            ah_attr: ffi::ibv_ah_attr {
                is_global: 1,
                port_num,
                grh: ffi::ibv_global_route {
                    sgid_index: gid_index,
                    hop_limit: params.hop_limit,
                    dgid: ffi::ibv_gid {
                        raw: remote_info.gid,
                    },
                    traffic_class: params.traffic_class,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        };

        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE
            | ffi::ibv_qp_attr_mask::IBV_QP_AV
            | ffi::ibv_qp_attr_mask::IBV_QP_PATH_MTU
            | ffi::ibv_qp_attr_mask::IBV_QP_DEST_QPN
            | ffi::ibv_qp_attr_mask::IBV_QP_RQ_PSN
            | ffi::ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC
            | ffi::ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;

        let ret = unsafe { ffi::ibv_modify_qp(self.qp, &mut attr, mask.0 as i32) };

        if ret != 0 {
            Err(rdma_err_code("ibv_modify_qp[RTR]", ret))
        } else {
            debug!(
                "QP {} modified from INIT to RTR. Local GID Index={}. Remote QPN={}, PSN={}, GID={:?}",
                self.qp_num(),
                gid_index,
                remote_info.qp_num,
                remote_info.psn,
                remote_info.gid,
            );
            Ok(())
        }
    }

    pub(crate) fn modify_to_rts(&self, sq_psn: u32, params: &RtsParams) -> Result<(), FeroceError> {
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RTS,
            timeout: params.timeout,
            retry_cnt: params.retry_cnt,
            rnr_retry: params.rnr_retry,
            sq_psn,
            max_rd_atomic: params.max_rd_atomic,
            ..Default::default()
        };

        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE
            | ffi::ibv_qp_attr_mask::IBV_QP_TIMEOUT
            | ffi::ibv_qp_attr_mask::IBV_QP_RETRY_CNT
            | ffi::ibv_qp_attr_mask::IBV_QP_RNR_RETRY
            | ffi::ibv_qp_attr_mask::IBV_QP_SQ_PSN
            | ffi::ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;

        let ret = unsafe { ffi::ibv_modify_qp(self.qp, &mut attr, mask.0 as i32) };

        if ret != 0 {
            Err(rdma_err_code("ibv_modify_qp[RTS]", ret))
        } else {
            debug!(
                "QP {} modified from RTR to RTS. Set local psn to {}",
                self.qp_num(),
                sq_psn,
            );
            Ok(())
        }
    }

    pub fn modify_to_error(&self) -> Result<(), FeroceError> {
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_ERR,
            ..Default::default()
        };

        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE;
        let ret = unsafe { ffi::ibv_modify_qp(self.qp, &mut attr, mask.0 as i32) };

        if ret != 0 {
            Err(rdma_err_code("ibv_modify_qp[ERR]", ret))
        } else {
            debug!("QP {} modify to ERROR", self.qp_num());
            Ok(())
        }
    }

    pub fn post_recv(&self, wr: &mut ffi::ibv_recv_wr) -> Result<(), FeroceError> {
        let mut bad_wr: *mut ffi::ibv_recv_wr = std::ptr::null_mut();
        let ctx = unsafe { (*self.qp).context };
        let post_recv_fn = unsafe { (*ctx).ops.post_recv.unwrap() };
        let ret = unsafe { post_recv_fn(self.qp, wr, &mut bad_wr) };

        if ret != 0 {
            Err(rdma_err_code("ibv_post_recv", ret))
        } else {
            Ok(())
        }
    }
    // usefull for building wr and posting in one call
    pub fn post_send(&self, wr: &mut ffi::ibv_send_wr) -> Result<(), FeroceError> {
        let mut bad_wr: *mut ffi::ibv_send_wr = std::ptr::null_mut();
        let ctx = unsafe { (*self.qp).context };
        let post_send_fn = unsafe { (*ctx).ops.post_send.unwrap() };
        let ret = unsafe { post_send_fn(self.qp, wr, &mut bad_wr) };

        if ret != 0 {
            Err(rdma_err_code("ibv_post_send", ret))
        } else {
            Ok(())
        }
    }

    pub fn build_recv_wr(wr_id: u64, sge_list: &mut [ffi::ibv_sge]) -> ffi::ibv_recv_wr {
        ffi::ibv_recv_wr {
            wr_id,
            next: std::ptr::null_mut(),
            sg_list: sge_list.as_mut_ptr(),
            num_sge: sge_list.len() as i32,
        }
    }

    pub fn build_send_wr(
        wr_id: u64,
        sge_list: &mut [ffi::ibv_sge],
        op: SendOp,
        signaled: bool,
    ) -> ffi::ibv_send_wr {
        let mut flags = 0u32;
        if signaled {
            flags |= ffi::ibv_send_flags::IBV_SEND_SIGNALED.0;
        }

        let mut wr = ffi::ibv_send_wr {
            wr_id,
            next: std::ptr::null_mut(),
            sg_list: sge_list.as_mut_ptr(),
            num_sge: sge_list.len() as i32,
            send_flags: flags,
            ..Default::default()
        };

        match op {
            SendOp::Send => {
                wr.opcode = ffi::ibv_wr_opcode::IBV_WR_SEND;
            }
            SendOp::SendWithImm(imm) => {
                wr.opcode = ffi::ibv_wr_opcode::IBV_WR_SEND_WITH_IMM;
                wr.__bindgen_anon_1.imm_data = imm;
            }
            SendOp::Write { remote_addr, rkey } => {
                wr.opcode = ffi::ibv_wr_opcode::IBV_WR_RDMA_WRITE;
                wr.wr.rdma.remote_addr = remote_addr;
                wr.wr.rdma.rkey = rkey;
            }
            SendOp::WriteWithImm {
                remote_addr,
                rkey,
                imm_data,
            } => {
                wr.opcode = ffi::ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM;
                wr.wr.rdma.remote_addr = remote_addr;
                wr.wr.rdma.rkey = rkey;
                wr.__bindgen_anon_1.imm_data = imm_data;
            }
        }

        wr
    }

    pub fn send_cq(&self) -> &CompletionQueue {
        &self._send_cq
    }

    pub fn recv_cq(&self) -> &CompletionQueue {
        &self._recv_cq
    }

    pub fn pd(&self) -> &ProtectionDomain {
        &self._pd
    }
}

impl Drop for QueuePair {
    fn drop(&mut self) {
        let ret = unsafe { ffi::ibv_destroy_qp(self.qp) };
        if ret != 0 {
            error!("ibv_destroy_qp failed: errno={}", ret);
        }
    }
}

pub struct QpBuilder {
    pd: Arc<ProtectionDomain>,
    send_cq: Arc<CompletionQueue>,
    recv_cq: Arc<CompletionQueue>,
    qp_type: ffi::ibv_qp_type,
    link: LocalLink,

    // create_qp knobs
    max_send_wr: u32,
    max_recv_wr: u32,
    max_send_sge: u32,
    max_recv_sge: u32,
    max_inline_data: u32,

    // RTR knobs
    rtr: RtrParams,

    // RTS knobs
    rts: RtsParams,
    sq_psn: u32,
}

impl QpBuilder {
    fn new(
        pd: Arc<ProtectionDomain>,
        send_cq: Arc<CompletionQueue>,
        recv_cq: Arc<CompletionQueue>,
        qp_type: ffi::ibv_qp_type,
        link: LocalLink,
    ) -> Self {
        Self {
            pd,
            send_cq,
            recv_cq,
            qp_type,
            link,
            max_send_wr: 128,
            max_recv_wr: 128,
            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: 0,
            rtr: RtrParams::default(),
            rts: RtsParams::default(),
            sq_psn: 0,
        }
    }

    pub fn set_max_wr(mut self, n: u32) -> Self {
        self.max_send_wr = n;
        self.max_recv_wr = n;
        self
    }

    pub fn set_max_send_wr(mut self, n: u32) -> Self {
        self.max_send_wr = n;
        self
    }

    pub fn set_max_recv_wr(mut self, n: u32) -> Self {
        self.max_recv_wr = n;
        self
    }
    pub fn set_max_sge(mut self, n: u32) -> Self {
        self.max_send_sge = n;
        self.max_recv_sge = n;
        self
    }

    pub fn set_max_send_sge(mut self, n: u32) -> Self {
        self.max_send_sge = n;
        self
    }

    pub fn set_max_recv_sge(mut self, n: u32) -> Self {
        self.max_recv_sge = n;
        self
    }

    pub fn set_max_inline_data(mut self, n: u32) -> Self {
        self.max_inline_data = n;
        self
    }

    pub fn set_min_rnr_timer(mut self, n: u8) -> Self {
        self.rtr.min_rnr_timer = n;
        self
    }

    pub fn set_max_dest_rd_atomic(mut self, n: u8) -> Self {
        self.rtr.max_dest_rd_atomic = n;
        self
    }

    pub fn set_hop_limit(mut self, n: u8) -> Self {
        self.rtr.hop_limit = n;
        self
    }

    pub fn set_timeout(mut self, n: u8) -> Self {
        self.rts.timeout = n;
        self
    }

    pub fn set_retry_cnt(mut self, n: u8) -> Self {
        self.rts.retry_cnt = n;
        self
    }

    pub fn set_rnr_retry(mut self, n: u8) -> Self {
        self.rts.rnr_retry = n;
        self
    }

    pub fn set_max_rd_atomic(mut self, n: u8) -> Self {
        self.rts.max_rd_atomic = n;
        self
    }

    pub fn set_sq_psn(mut self, sq_psn: u32) -> Self {
        self.sq_psn = sq_psn;
        self
    }

    pub fn set_traffic_class(mut self, traffic_class: u8) -> Self {
        self.rtr.traffic_class = traffic_class;
        self
    }

    pub fn build(self) -> Result<PreparedQueuePair, FeroceError> {
        let qp = QueuePair::create_qp(
            self.pd,
            self.send_cq,
            self.recv_cq,
            self.max_send_wr,
            self.max_recv_wr,
            self.max_send_sge,
            self.max_recv_sge,
            self.max_inline_data,
            self.qp_type,
        )?;
        qp.modify_to_init(self.link.port_num)?;
        Ok(PreparedQueuePair {
            qp,
            link: self.link,
            rtr: self.rtr,
            rts: self.rts,
            sq_psn: self.sq_psn,
        })
    }
}

pub struct PreparedQueuePair {
    qp: QueuePair,
    link: LocalLink,
    rtr: RtrParams,
    rts: RtsParams,
    sq_psn: u32,
}

impl PreparedQueuePair {
    pub fn qp_num(&self) -> u32 {
        self.qp.qp_num()
    }

    pub fn send_cq(&self) -> &CompletionQueue {
        self.qp.send_cq()
    }

    pub fn recv_cq(&self) -> &CompletionQueue {
        self.qp.recv_cq()
    }

    pub fn pd(&self) -> &ProtectionDomain {
        self.qp.pd()
    }

    pub fn link(&self) -> LocalLink {
        self.link
    }

    // Transition the QP through RTR and RTS with the remote info,
    // returning the connected QueuePair
    pub fn handshake(self, remote_info: &QpConnectionInfo) -> Result<QueuePair, FeroceError> {
        self.qp.modify_to_rtr(
            remote_info,
            self.link.gid_index,
            self.link.port_num,
            self.link.mtu,
            &self.rtr,
        )?;
        self.qp.modify_to_rts(self.sq_psn, &self.rts)?;
        Ok(self.qp)
    }
}

// NIC-registered memory region that owns the buffer it registered.
// S is the backing storage (`Vec<u8>` for the CPU, a GPU buffer for the dmabuf)
pub struct MemoryRegion<S> {
    mr: *mut ffi::ibv_mr,
    _pd: Arc<ProtectionDomain>,
    storage: S,
}

unsafe impl<S: Send> Send for MemoryRegion<S> {}

impl MemoryRegion<Vec<u8>> {
    pub fn register(
        pd: &Arc<ProtectionDomain>,
        mut storage: Vec<u8>,
        access: ffi::ibv_access_flags,
    ) -> Result<Self, FeroceError> {
        let mr = unsafe {
            ffi::ibv_reg_mr(
                pd.raw(),
                storage.as_mut_ptr() as *mut std::ffi::c_void,
                storage.len(),
                access.0 as i32,
            )
        };

        if mr.is_null() {
            Err(rdma_err("ibv_reg_mr"))
        } else {
            debug!(
                "Registered MR: addr={:#x}, len={}",
                unsafe { (*mr).addr as u64 },
                unsafe { (*mr).length as u32 }
            );
            Ok(MemoryRegion {
                mr,
                _pd: Arc::clone(pd),
                storage,
            })
        }
    }
}

impl<S> MemoryRegion<S> {
    #[cfg(feature = "gpu")]
    pub fn register_dmabuf(
        pd: &Arc<ProtectionDomain>,
        offset: u64,
        length: usize,
        iova: u64,
        dmabuf_fd: i32,
        access: ffi::ibv_access_flags,
        storage: S,
    ) -> Result<Self, FeroceError> {
        let mr = unsafe {
            ffi::ibv_reg_dmabuf_mr(pd.raw(), offset, length, iova, dmabuf_fd, access.0 as i32)
        };

        if mr.is_null() {
            Err(rdma_err("ibv_reg_dmabuf_mr"))
        } else {
            debug!(
                "Registered DMA BUF MR: addr={:#x}, len={}",
                unsafe { (*mr).addr as u64 },
                unsafe { (*mr).length as u32 }
            );
            Ok(MemoryRegion {
                mr,
                _pd: Arc::clone(pd),
                storage,
            })
        }
    }

    pub fn addr(&self) -> u64 {
        unsafe { (*self.mr).addr as u64 }
    }

    pub fn length(&self) -> u32 {
        unsafe { (*self.mr).length as u32 }
    }

    pub fn lkey(&self) -> u32 {
        unsafe { (*self.mr).lkey }
    }

    pub fn rkey(&self) -> u32 {
        unsafe { (*self.mr).rkey }
    }

    // Shared access to the owned backing storage.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    // Mutable access to the owned backing storage.
    pub fn storage_mut(&mut self) -> &mut S {
        &mut self.storage
    }
}

impl<S> Drop for MemoryRegion<S> {
    fn drop(&mut self) {
        // the NIC registration is always destroyed before
        // storage frees the underlying memory.
        let ret = unsafe { ffi::ibv_dereg_mr(self.mr) };
        if ret != 0 {
            error!("ibv_dereg_mr failed: errno={}", ret);
        }
    }
}

// Discover the first active RoCEv2 device on the host. Returns the device
// name, an Arc<Device>, and the validated LocalLink (port + GID +
// active MTU).
pub fn find_roce_device() -> Option<(String, Arc<Device>, LocalLink)> {
    let devices = DeviceList::new().ok()?;
    for i in 0..devices.len() {
        // skip to the next device on failure
        let Some(name) = devices.device_name(i) else {
            continue;
        };
        let Ok(device) = Device::open(name) else {
            continue;
        };
        let Ok(entries) = device.query_gid_table() else {
            continue;
        };

        for entry in &entries {
            let (port, gid_index) = (entry.port_num as u8, entry.gid_index as u8);
            if let Ok(Some(link)) = device.probe_link(port, gid_index) {
                info!(
                    "Discovery: device={}, port={}, gid_index={}, mtu={:?}",
                    name, port, gid_index, link.mtu as u32
                );
                return Some((name.to_string(), Arc::new(device), link));
            }
        }
    }
    None
}

#[cfg(test)]
#[cfg(feature = "rdma-test")]
mod test {
    use super::*;
    use crate::rdma;
    use crate::rdma::test_utils::*;

    #[test]
    fn list_devices() {
        let devices = DeviceList::new().expect("no RDMA devices found");
        println!("Found {} RDMA devices", devices.len());
        for i in 0..devices.len() {
            println!("\t{}: {}", i, devices.device_name(i).unwrap_or("unknown"));
        }

        assert!(devices.len() > 0);
    }

    #[test]
    fn open_device() {
        let devices = DeviceList::new().expect("no RDMA devices found");
        let name = devices.device_name(0).expect("no devices");
        let _device = Device::open(name).expect("failed to open device");
    }

    #[test]
    fn alloc_pd_and_cq() {
        let devices = DeviceList::new().expect("no RDMA devices found");
        let name = devices.device_name(0).expect("no devices");
        let device = Arc::new(Device::open(name).expect("failed to open device"));

        let _pd = device.alloc_pd().expect("failed to allocate PD");
        let _cq = device.create_cq(128).expect("failed to create CQ");
    }

    #[test]
    fn create_qp() {
        let (_name, device, link) =
            find_roce_device().expect("no active RoCE device found — skipping");

        let pd = Arc::new(device.alloc_pd().expect("failed to allocate PD"));
        let cq = Arc::new(device.create_cq(128).expect("failed to create CQ"));

        let _prepared = pd
            .create_qp(&cq, &cq, rdma::ibv_qp_type::IBV_QPT_RC, link)
            .set_max_wr(16)
            .set_max_sge(4)
            .build()
            .expect("failed to create qp");
    }

    #[test]
    fn qp_state_transitions_rts() {
        let (_name, device, link) =
            find_roce_device().expect("no active RoCE device found — skipping");

        let pd = Arc::new(device.alloc_pd().expect("failed to allocate PD"));
        let cq = Arc::new(device.create_cq(128).expect("failed to create CQ"));

        let prepared = pd
            .create_qp(&cq, &cq, rdma::ibv_qp_type::IBV_QPT_RC, link)
            .set_max_wr(16)
            .set_max_sge(4)
            .set_sq_psn(4321)
            .build()
            .expect("failed to create qp");

        let loc_gid = device
            .query_gid(link.port_num, link.gid_index as i32)
            .expect("failed to query gid");

        let fake_remote_info = QpConnectionInfo {
            qp_num: prepared.qp_num(),
            psn: 1234,
            rkey: 0xABCD,
            addr: 0x1234ABCD,
            gid: loc_gid.raw,
        };

        let _qp = prepared
            .handshake(&fake_remote_info)
            .expect("failed to handshake qp to RTS");
    }

    #[test]
    fn register_memory_region() {
        let (_name, device, _link) =
            find_roce_device().expect("no active RoCE device found — skipping");

        let pd = Arc::new(device.alloc_pd().expect("failed to allocate PD"));

        let buf = vec![0u8; 128];

        let mr = MemoryRegion::register(
            &pd,
            buf,
            ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE,
        )
        .expect("failed to register memory region");

        assert_eq!(mr.length(), 128);
        assert!(mr.lkey() != 0);
        assert!(mr.rkey() != 0);
    }

    #[test]
    fn loopback_send_recv() {
        let buf_size = 64;

        let (_name, device, link) =
            find_roce_device().expect("no active RoCE device found — skipping");

        let mut pair = create_loopback_qp(&device, link, buf_size);

        // post reveice wr
        let recv_lkey = pair.recv.mr.lkey();
        let mut recv_sg_list = make_sge_list(1, pair.recv.mr.storage_mut(), recv_lkey);

        let mut recv_wr = QueuePair::build_recv_wr(2, &mut recv_sg_list);
        pair.recv
            .qp
            .post_recv(&mut recv_wr)
            .expect("failed to post recv wr");

        // post send wr
        let send_buf = pair.send.mr.storage_mut();
        send_buf[0] = 0xff;
        send_buf[1] = 0xde;
        send_buf[2] = 0xad;

        let send_lkey = pair.send.mr.lkey();
        let mut send_sg_list = make_sge_list(1, pair.send.mr.storage_mut(), send_lkey);

        let mut send_wr = QueuePair::build_send_wr(2, &mut send_sg_list, SendOp::Send, true);
        pair.send
            .qp
            .post_send(&mut send_wr)
            .expect("failed to post send wr");

        // poll the completion queue and hope for the best
        let mut recv_wc = Vec::<ffi::ibv_wc>::new();
        recv_wc.push(ffi::ibv_wc {
            ..Default::default()
        });

        let _n_wce_recv = poll_cq_with_timeout(
            &pair.recv.qp.recv_cq(),
            &mut recv_wc,
            std::time::Duration::from_secs(2),
        );

        // found event in the recv_cq, check send.. it should be there as well
        let mut send_wc = Vec::<ffi::ibv_wc>::new();
        send_wc.push(ffi::ibv_wc {
            ..Default::default()
        });

        let n_wce_send = poll_cq_with_timeout(
            &pair.send.qp.send_cq(),
            &mut send_wc,
            std::time::Duration::from_secs(2),
        );

        assert!(matches!(
            send_wc[0].status,
            ffi::ibv_wc_status::IBV_WC_SUCCESS
        ));
        assert_eq!(n_wce_send, 1);
        assert_eq!(pair.recv.mr.storage(), pair.send.mr.storage());
        assert_eq!(recv_wc[0].byte_len, buf_size as u32);
    }

    #[test]
    fn loopback_send_recv_linked() {
        let buf_size = 64;
        let half_buf_size = buf_size as u32 / 2;

        let (_name, device, link) =
            find_roce_device().expect("no active RoCE device found — skipping");

        let mut pair = create_loopback_qp(&device, link, buf_size);

        // post reveice wr
        let mut recv_wr_list = Vec::<rdma::ibv_recv_wr>::new();
        let mut recv_sg_list = Vec::<Vec<rdma::ibv_sge>>::new();

        recv_sg_list.push(vec![
            ffi::ibv_sge {
                addr: pair.recv.mr.addr() as u64,
                length: half_buf_size,
                lkey: pair.recv.mr.lkey(),
            };
            1
        ]);

        recv_sg_list.push(vec![
            ffi::ibv_sge {
                addr: pair.recv.mr.addr() + half_buf_size as u64,
                length: half_buf_size,
                lkey: pair.recv.mr.lkey(),
            };
            1
        ]);

        for idx in 0..2 {
            recv_wr_list.push(QueuePair::build_recv_wr(idx as u64, &mut recv_sg_list[idx]));
        }

        recv_wr_list[0].next = &mut recv_wr_list[1];

        pair.recv
            .qp
            .post_recv(&mut recv_wr_list[0])
            .expect("failed to post recv wr");

        // post send wr
        let send_buf = pair.send.mr.storage_mut();
        send_buf[0] = 0xff;
        send_buf[1] = 0xde;
        send_buf[2] = 0xad;

        let mut send_wr_list = Vec::<rdma::ibv_send_wr>::new();
        let mut send_sg_list = Vec::<Vec<rdma::ibv_sge>>::new();

        send_sg_list.push(vec![
            ffi::ibv_sge {
                addr: pair.send.mr.addr() as u64,
                length: half_buf_size,
                lkey: pair.send.mr.lkey(),
            };
            1
        ]);
        send_wr_list.push(QueuePair::build_send_wr(
            0,
            &mut send_sg_list[0],
            SendOp::Send,
            false,
        ));

        send_sg_list.push(vec![
            ffi::ibv_sge {
                addr: pair.send.mr.addr() + half_buf_size as u64,
                length: half_buf_size,
                lkey: pair.send.mr.lkey(),
            };
            1
        ]);

        send_wr_list.push(QueuePair::build_send_wr(
            1,
            &mut send_sg_list[1],
            SendOp::Send,
            true,
        ));

        // link the two recv requests
        send_wr_list[0].next = &mut send_wr_list[1];
        pair.send
            .qp
            .post_send(&mut send_wr_list[0])
            .expect("failed to post send wr");

        // poll the completion queue and hope for the best
        let mut recv_wc = Vec::<ffi::ibv_wc>::new();
        recv_wc.push(ffi::ibv_wc {
            ..Default::default()
        });
        recv_wc.push(ffi::ibv_wc {
            ..Default::default()
        });

        let _n_wce_recv = poll_cq_with_timeout(
            &pair.recv.qp.recv_cq(),
            &mut recv_wc,
            std::time::Duration::from_secs(2),
        );

        // found event in the recv_cq, check send.. it should be there as well
        let mut send_wc = Vec::<ffi::ibv_wc>::new();
        send_wc.push(ffi::ibv_wc {
            ..Default::default()
        });
        send_wc.push(ffi::ibv_wc {
            ..Default::default()
        });

        let n_wce_send = poll_cq_with_timeout(
            &pair.send.qp.send_cq(),
            &mut send_wc,
            std::time::Duration::from_secs(2),
        );

        assert!(matches!(
            send_wc[0].status,
            ffi::ibv_wc_status::IBV_WC_SUCCESS
        ));
        assert_eq!(n_wce_send, 1);
        assert_eq!(send_wc[0].wr_id, 1);
        assert_eq!(pair.recv.mr.storage(), pair.send.mr.storage());
        assert_eq!(recv_wc[0].byte_len, half_buf_size as u32);
    }

    #[test]
    fn loopback_send_recv_error_transition() {
        let buf_size = 64;

        let (_name, device, link) =
            find_roce_device().expect("no active RoCE device found — skipping");

        let mut pair = create_loopback_qp(&device, link, buf_size);

        // post reveice wr before transitioning to error
        let recv_lkey = pair.recv.mr.lkey();
        let mut recv_sg_list = make_sge_list(1, pair.recv.mr.storage_mut(), recv_lkey);

        let mut recv_wr = QueuePair::build_recv_wr(1, &mut recv_sg_list);
        pair.recv
            .qp
            .post_recv(&mut recv_wr)
            .expect("failed to post recv wr");

        // transition the QP to error state
        pair.recv
            .qp
            .modify_to_error()
            .expect("Failed to transition to error");

        let mut recv_wc = Vec::<ffi::ibv_wc>::new();
        recv_wc.push(ffi::ibv_wc {
            ..Default::default()
        });

        let _n = poll_cq_with_timeout(
            &pair.recv.qp.recv_cq(),
            &mut recv_wc,
            std::time::Duration::from_secs(2),
        );

        assert!(matches!(
            recv_wc[0].status,
            ffi::ibv_wc_status::IBV_WC_WR_FLUSH_ERR
        ));
    }

    #[test]
    fn loopback_rdma_write() {
        let buf_size = 64;

        let (_name, device, link) =
            find_roce_device().expect("no active RoCE device found — skipping");

        let mut pair = create_loopback_qp(&device, link, buf_size);

        // RDMA WRITE is one-sided: no recv WR needed on the receiver.
        let send_buf = pair.send.mr.storage_mut();
        send_buf[0] = 0xde;
        send_buf[1] = 0xad;
        send_buf[2] = 0xbe;
        send_buf[3] = 0xef;

        let send_lkey = pair.send.mr.lkey();
        let mut send_sg_list = make_sge_list(1, pair.send.mr.storage_mut(), send_lkey);

        let mut send_wr = QueuePair::build_send_wr(
            1,
            &mut send_sg_list,
            SendOp::Write {
                remote_addr: pair.recv.mr.addr(),
                rkey: pair.recv.mr.rkey(),
            },
            true,
        );
        pair.send
            .qp
            .post_send(&mut send_wr)
            .expect("failed to post rdma write wr");

        // poll the send CQ for completion
        let mut send_wc = vec![ffi::ibv_wc {
            ..Default::default()
        }];
        let n_wce_send = poll_cq_with_timeout(
            &pair.send.qp.send_cq(),
            &mut send_wc,
            std::time::Duration::from_secs(2),
        );
        assert_eq!(n_wce_send, 1);
        assert!(
            matches!(send_wc[0].status, ffi::ibv_wc_status::IBV_WC_SUCCESS),
            "send WC status: {:?}",
            send_wc[0].status
        );

        // verify the receiver's buffer was written directly
        let recv_buf = pair.recv.mr.storage();
        assert_eq!(&recv_buf[0..4], &[0xde, 0xad, 0xbe, 0xef]);
    }

    #[test]
    fn loopback_rdma_write_with_imm() {
        let buf_size = 64;
        let imm_data: u32 = 0x12345678;

        let (_name, device, link) =
            find_roce_device().expect("no active RoCE device found — skipping");

        let mut pair = create_loopback_qp(&device, link, buf_size);

        // WRITE with immediate generates a receive completion on the receiver,
        // so we must post a recv WR first.
        let recv_lkey = pair.recv.mr.lkey();
        let mut recv_sg_list = make_sge_list(1, pair.recv.mr.storage_mut(), recv_lkey);
        let mut recv_wr = QueuePair::build_recv_wr(1, &mut recv_sg_list);
        pair.recv
            .qp
            .post_recv(&mut recv_wr)
            .expect("failed to post recv wr");

        let send_buf = pair.send.mr.storage_mut();
        send_buf[0] = 0xc0;
        send_buf[1] = 0xff;
        send_buf[2] = 0xee;
        send_buf[3] = 0x00;

        let send_lkey = pair.send.mr.lkey();
        let mut send_sg_list = make_sge_list(1, pair.send.mr.storage_mut(), send_lkey);

        let mut send_wr = QueuePair::build_send_wr(
            1,
            &mut send_sg_list,
            SendOp::WriteWithImm {
                remote_addr: pair.recv.mr.addr(),
                rkey: pair.recv.mr.rkey(),
                imm_data,
            },
            true,
        );
        pair.send
            .qp
            .post_send(&mut send_wr)
            .expect("failed to post rdma write-with-imm wr");

        // poll first the send CQ
        let mut send_wc = vec![ffi::ibv_wc {
            ..Default::default()
        }];
        let n_wce_send = poll_cq_with_timeout(
            &pair.send.qp.send_cq(),
            &mut send_wc,
            std::time::Duration::from_secs(2),
        );
        assert_eq!(n_wce_send, 1);
        assert!(matches!(
            send_wc[0].status,
            ffi::ibv_wc_status::IBV_WC_SUCCESS
        ));

        // poll now the recv cq
        let mut recv_wc = vec![ffi::ibv_wc {
            ..Default::default()
        }];
        let n_wce_recv = poll_cq_with_timeout(
            &pair.recv.qp.recv_cq(),
            &mut recv_wc,
            std::time::Duration::from_secs(2),
        );
        assert_eq!(n_wce_recv, 1);
        assert!(matches!(
            recv_wc[0].status,
            ffi::ibv_wc_status::IBV_WC_SUCCESS
        ));

        // the wc_flags must indicate immediate data is present
        assert_ne!(
            recv_wc[0].wc_flags & ffi::ibv_wc_flags::IBV_WC_WITH_IMM,
            ffi::ibv_wc_flags(0)
        );

        // verify the immediate data value
        assert_eq!(recv_wc[0].imm_data, imm_data);

        // verify the receiver's buffer was written
        let recv_buf = pair.recv.mr.storage();
        assert_eq!(&recv_buf[0..4], &[0xc0, 0xff, 0xee, 0x00]);
    }

    #[test]
    fn two_qps_share_pd_and_cq() {
        let (_name, device, link) =
            find_roce_device().expect("no active RoCE device found — skipping");

        let pd = Arc::new(device.alloc_pd().expect("failed to alloc pd"));
        let cq = Arc::new(device.create_cq(16).expect("failed to create cq"));

        // create two qps sharing pd and cq; build() also brings each to INIT
        let qp1 = pd
            .create_qp(&cq, &cq, rdma::ibv_qp_type::IBV_QPT_RC, link)
            .set_max_wr(16)
            .build()
            .expect("failed to create QP");
        let _qp2 = pd
            .create_qp(&cq, &cq, rdma::ibv_qp_type::IBV_QPT_RC, link)
            .set_max_wr(16)
            .build()
            .expect("failed to create QP");

        // drop one of the qps, the other should still be alive
        drop(qp1);
    }

    #[test]
    fn completion_channel_firing() {
        let (_name, device, link) =
            find_roce_device().expect("no active RoCE device found — skipping");

        let buf_size = 64;
        let mut pair = create_loopback_qp(&device, link, buf_size);

        // request notifications for cq of pair.send.qp
        pair.send
            .qp
            .send_cq()
            .req_notify_cq(false)
            .expect("failed to request notification to cq");

        // post reveice wr
        let recv_lkey = pair.recv.mr.lkey();
        let mut recv_sg_list = make_sge_list(1, pair.recv.mr.storage_mut(), recv_lkey);
        let mut recv_wr = QueuePair::build_recv_wr(1, &mut recv_sg_list);
        pair.recv
            .qp
            .post_recv(&mut recv_wr)
            .expect("failed to post recv wr");

        // post send wr
        let send_buf = pair.send.mr.storage_mut();
        send_buf[0] = 0xff;
        send_buf[1] = 0xde;
        send_buf[2] = 0xad;

        let send_lkey = pair.send.mr.lkey();
        let mut send_sg_list = make_sge_list(1, pair.send.mr.storage_mut(), send_lkey);

        let mut send_wr = QueuePair::build_send_wr(2, &mut send_sg_list, SendOp::Send, true);
        pair.send
            .qp
            .post_send(&mut send_wr)
            .expect("faieled to post send wr");

        let mut recv_wc = Vec::<ffi::ibv_wc>::new();
        recv_wc.push(ffi::ibv_wc {
            ..Default::default()
        });
        let _n_wce_recv = poll_cq_with_timeout(
            &pair.recv.qp.recv_cq(),
            &mut recv_wc,
            std::time::Duration::from_secs(2),
        );

        // wait for event in send channel
        // this should be blocking with a timeout...
        // the returned guard acks the event when it drops at end of scope
        let _cq_event = pair
            .send
            .channel
            .get_cq_event()
            .expect("failed to get completion event!");

        // now we should have an event!
        let mut send_wc = Vec::<ffi::ibv_wc>::new();
        send_wc.push(ffi::ibv_wc {
            ..Default::default()
        });

        let n_wce_send = pair
            .send
            .qp
            .send_cq()
            .poll(&mut send_wc)
            .expect("failed to poll completion queue");

        assert_eq!(n_wce_send, 1);
        assert!(matches!(
            send_wc[0].status,
            ffi::ibv_wc_status::IBV_WC_SUCCESS
        ));

        // _cq_event acks the event here as it drops at end of scope
    }
}
