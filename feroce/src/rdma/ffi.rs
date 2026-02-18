#[repr(C)]
pub enum ibv_device {}

#[repr(C)]
pub enum ibv_context {}

extern "C" {
    pub fn ibv_get_device_list(num_devices: *mut i32) -> *mut *mut ibv_device;
    pub fn ibv_free_device_list(list: *mut *mut ibv_device);
    pub fn ibv_get_device_name(device: *mut ibv_device) -> *const std::os::raw::c_char;
    pub fn ibv_open_device(device: *mut ibv_device) -> *mut ibv_context;
    pub fn ibv_close_device(context: *mut ibv_context) -> i32;
}
