use super::ffi;
use std::ffi::CStr;

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
        !self.len() > 0
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
            CStr::from_ptr(name_ptr).to_str().ok()
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
}

impl Drop for Device {
    fn drop(&mut self) {
        unsafe {
            ffi::ibv_close_device(self.context);
        }
    }
}

#[cfg(test)]
#[cfg(feature = "rdma-test")]
mod test {
    use super::*;

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
}
