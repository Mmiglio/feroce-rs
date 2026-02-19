use crate::protocol::QpConnectionInfo;

use super::ffi;

pub struct DeviceList {
    list: *mut *mut ffi::ibv_device,
    count: i32,
}

impl DeviceList {
    pub fn new() -> Result<Self, String> {
        let mut count: i32 = 0;
        let list = unsafe { ffi::ibv_get_device_list(&mut count) };

        if list.is_null() {
            Err("failed to get rdma device list".to_string())
        } else {
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

impl Device {
    pub fn open(name: &str) -> Result<Self, String> {
        let devices = DeviceList::new()?;

        for i in 0..devices.len() {
            if devices.device_name(i) == Some(name) {
                let device_ptr = devices.raw_device(i).ok_or("unable to get device")?;
                let context = unsafe { ffi::ibv_open_device(device_ptr) };
                if context.is_null() {
                    return Err(format!("failed to open device {}", name));
                } else {
                    return Ok(Device { context });
                }
            }
        }
        Err(format!("device '{}' not found", name))
    }

    pub fn alloc_pd(&self) -> Result<ProtectionDomain, String> {
        let pd = unsafe { ffi::ibv_alloc_pd(self.context) };
        if pd.is_null() {
            Err("failed to alloc protection domain".to_string())
        } else {
            Ok(ProtectionDomain { pd })
        }
    }

    pub fn create_cq(&self, cqe: i32) -> Result<CompletionQueue, String> {
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
            Err("failed to create completion queue".to_string())
        } else {
            Ok(CompletionQueue { cq })
        }
    }

    pub fn query_gid(&self, port_num: u8, gid_index: i32) -> Result<ffi::ibv_gid, String> {
        let mut gid = ffi::ibv_gid::default();
        let ret = unsafe { ffi::ibv_query_gid(self.context, port_num, gid_index, &mut gid) };

        if ret != 0 {
            Err(format!("failed to query GID: {}", ret))
        } else {
            Ok(gid)
        }
    }
}

impl Drop for Device {
    fn drop(&mut self) {
        unsafe {
            ffi::ibv_close_device(self.context);
        }
    }
}

pub struct ProtectionDomain {
    pd: *mut ffi::ibv_pd,
}

impl ProtectionDomain {
    pub fn raw(&self) -> *mut ffi::ibv_pd {
        self.pd
    }
}

impl Drop for ProtectionDomain {
    fn drop(&mut self) {
        unsafe {
            ffi::ibv_dealloc_pd(self.pd);
        }
    }
}

pub struct CompletionQueue {
    cq: *mut ffi::ibv_cq,
}

impl CompletionQueue {
    pub fn raw(&self) -> *mut ffi::ibv_cq {
        self.cq
    }
}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        unsafe { ffi::ibv_destroy_cq(self.cq) };
    }
}

pub struct QueuePair {
    qp: *mut ffi::ibv_qp,
    _pd: ProtectionDomain,
    _cq: CompletionQueue,
}

impl QueuePair {
    pub fn create_qp(
        pd: ProtectionDomain,
        cq: CompletionQueue,
        max_wr: u32,
        max_sge: u32,
        qp_type: ffi::ibv_qp_type,
    ) -> Result<Self, String> {
        let mut init_attr = ffi::ibv_qp_init_attr {
            qp_context: std::ptr::null_mut(),
            send_cq: cq.raw(),
            recv_cq: cq.raw(),
            srq: std::ptr::null_mut(),
            cap: ffi::ibv_qp_cap {
                max_send_wr: max_wr,
                max_recv_wr: max_wr,
                max_send_sge: max_sge,
                max_recv_sge: max_sge,
                max_inline_data: 0,
            },
            qp_type: qp_type,
            sq_sig_all: 0,
        };

        let qp = unsafe { ffi::ibv_create_qp(pd.raw(), &mut init_attr) };
        if qp.is_null() {
            Err("failed to create QP".to_string())
        } else {
            Ok(QueuePair {
                qp,
                _pd: pd,
                _cq: cq,
            })
        }
    }

    pub fn qp_num(&self) -> u32 {
        unsafe { (*self.qp).qp_num }
    }

    pub fn modify_to_init(&self, port_num: u8) -> Result<(), String> {
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_INIT,
            port_num,
            ..Default::default()
        };
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE
            | ffi::ibv_qp_attr_mask::IBV_QP_PORT
            | ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;

        let ret = unsafe { ffi::ibv_modify_qp(self.qp, &mut attr, mask) };
        if ret != 0 {
            Err(format!("Failed to modify QP to INIT: error {}", ret))
        } else {
            Ok(())
        }
    }

    pub fn modify_to_rtr(
        &self,
        remote_info: &QpConnectionInfo,
        gid_index: u8,
        port_num: u8,
        mtu: ffi::ibv_mtu,
    ) -> Result<(), String> {
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RTR,
            path_mtu: mtu,
            dest_qp_num: remote_info.qp_num,
            rq_psn: remote_info.psn,
            max_dest_rd_atomic: 1,
            min_rnr_timer: 12,
            ah_attr: ffi::ibv_ah_attr {
                is_global: 1,
                port_num: port_num,
                grh: ffi::ibv_global_route {
                    sgid_index: gid_index,
                    hop_limit: 1,
                    dgid: ffi::ibv_gid {
                        raw: remote_info.gid,
                    },
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

        let ret = unsafe { ffi::ibv_modify_qp(self.qp, &mut attr, mask) };

        if ret != 0 {
            Err(format!("Failed to modify QP to RTR: error {}", ret))
        } else {
            Ok(())
        }
    }

    pub fn modify_to_rts(&self, sq_psn: u32) -> Result<(), String> {
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RTS,
            timeout: 14,
            retry_cnt: 7,
            rnr_retry: 7,
            sq_psn: sq_psn,
            max_rd_atomic: 1,
            ..Default::default()
        };

        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE
            | ffi::ibv_qp_attr_mask::IBV_QP_TIMEOUT
            | ffi::ibv_qp_attr_mask::IBV_QP_RETRY_CNT
            | ffi::ibv_qp_attr_mask::IBV_QP_RNR_RETRY
            | ffi::ibv_qp_attr_mask::IBV_QP_SQ_PSN
            | ffi::ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;

        let ret = unsafe { ffi::ibv_modify_qp(self.qp, &mut attr, mask) };

        if ret != 0 {
            Err(format!("Failed to modify QP to RTS: error {}", ret))
        } else {
            Ok(())
        }
    }
}

impl Drop for QueuePair {
    fn drop(&mut self) {
        unsafe { ffi::ibv_destroy_qp(self.qp) };
    }
}

#[cfg(test)]
#[cfg(feature = "rdma-test")]
mod test {
    use super::*;
    use crate::rdma;

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
        let device = Device::open(name).expect("failed to open device");

        let _pd = device.alloc_pd().expect("failed to allocate PD");
        let _cq = device.create_cq(128).expect("failed to create CQ");
    }

    #[test]
    fn create_qp() {
        let devices = DeviceList::new().expect("no RDMA devices found");
        let name = devices.device_name(0).expect("no devices");
        let device = Device::open(name).expect("failed to open device");

        let pd = device.alloc_pd().expect("failed to allocate PD");
        let cq = device.create_cq(128).expect("failed to create CQ");

        let _qp = QueuePair::create_qp(pd, cq, 16, 8, rdma::ibv_qp_type::IBV_QPT_RC)
            .expect("failed to create qp");
    }

    #[test]
    fn qp_state_transitions_rts() {
        let devices = DeviceList::new().expect("no RDMA devices found");
        let name = devices.device_name(0).expect("no devices");
        let device = Device::open(name).expect("failed to open device");

        let pd = device.alloc_pd().expect("failed to allocate PD");
        let cq = device.create_cq(128).expect("failed to create CQ");

        let qp = QueuePair::create_qp(pd, cq, 16, 8, rdma::ibv_qp_type::IBV_QPT_RC)
            .expect("failed to create qp");

        let loc_gid = device.query_gid(1, 3).expect("failed to query fid");
        let qp_num = qp.qp_num();

        let fake_remote_info = QpConnectionInfo {
            qp_num: qp_num,
            psn: 1234,
            rkey: 0xABCD,
            addr: 0x1234ABCD,
            gid: loc_gid.raw, // gid_from_ipv4(Ipv4Addr::new(10, 0, 1, 120).into()),
        };

        qp.modify_to_init(1).expect("failed to modify qp to INIT");
        qp.modify_to_rtr(&fake_remote_info, 3, 1, rdma::ffi::ibv_mtu::IBV_MTU_1024)
            .expect("failed to modify qp to RTR");
        qp.modify_to_rts(4321).expect("failed to modify qp to RTS");
    }
}
