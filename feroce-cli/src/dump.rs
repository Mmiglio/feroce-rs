use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use feroce::FeroceError;

const DUMP_BUF_CAP: usize = 1 << 20;

pub trait DumpSink: Send + 'static {
    fn record(&mut self, addr: *mut u8, byte_len: usize) -> Result<(), FeroceError>;

    fn record_batch(&mut self, bufs: &[(*mut u8, usize)]) -> Result<(), FeroceError> {
        for &(addr, byte_len) in bufs {
            self.record(addr, byte_len)?;
        }
        Ok(())
    }
}

pub trait DumpSinkFactory: Send {
    type Sink: DumpSink;
    fn make(&self, stream_id: u32) -> Result<Self::Sink, FeroceError>;
}

pub fn derive_stream_path(base: &Path, stream_id: u32) -> PathBuf {
    let parent = base.parent().unwrap_or_else(|| Path::new(""));
    let stem = base
        .file_stem()
        .expect("dump-file path must have a filename")
        .to_string_lossy();
    let suffix = format!("{:03}", stream_id);
    let new_name = match base.extension() {
        Some(ext) => format!("{}.{}.{}", stem, suffix, ext.to_string_lossy()),
        None => format!("{}.{}", stem, suffix),
    };
    parent.join(new_name)
}

pub struct CpuDumpSink {
    file: BufWriter<File>,
}

impl CpuDumpSink {
    pub fn open(base: &Path, stream_id: u32) -> Result<Self, FeroceError> {
        let path = derive_stream_path(base, stream_id);
        let file = File::create(&path)?;
        Ok(Self {
            file: BufWriter::with_capacity(DUMP_BUF_CAP, file),
        })
    }
}

impl DumpSink for CpuDumpSink {
    fn record(&mut self, addr: *mut u8, byte_len: usize) -> Result<(), FeroceError> {
        // addr is the registered RDMA buffer for this WC, will not be overwritten untill reposted
        let bytes = unsafe { std::slice::from_raw_parts(addr, byte_len) };
        self.file.write_all(bytes)?;
        Ok(())
    }
}

pub struct CpuDumpFactory {
    base: PathBuf,
}

impl CpuDumpFactory {
    pub fn new(base: PathBuf) -> Self {
        Self { base }
    }
}

impl DumpSinkFactory for CpuDumpFactory {
    type Sink = CpuDumpSink;
    fn make(&self, stream_id: u32) -> Result<CpuDumpSink, FeroceError> {
        CpuDumpSink::open(&self.base, stream_id)
    }
}

#[cfg(feature = "gpu")]
pub struct GpuDumpSink {
    file: BufWriter<File>,
    staging: Vec<u8>,
    device: i32,
    ctx: Option<feroce::rdma::gpu::CudaContext>,
}

#[cfg(feature = "gpu")]
impl GpuDumpSink {
    pub fn open(
        base: &Path,
        stream_id: u32,
        buf_size: usize,
        device: i32,
    ) -> Result<Self, FeroceError> {
        let path = derive_stream_path(base, stream_id);
        let file = File::create(&path)?;
        Ok(Self {
            file: BufWriter::with_capacity(DUMP_BUF_CAP, file),
            staging: vec![0u8; buf_size],
            device,
            ctx: None,
        })
    }
}

#[cfg(feature = "gpu")]
impl DumpSink for GpuDumpSink {
    fn record(&mut self, addr: *mut u8, byte_len: usize) -> Result<(), FeroceError> {
        // CUDA contexts are thread-local. record() runs on the poller thread,
        // which doesn't share the main thread's context stack
        if self.ctx.is_none() {
            self.ctx = Some(feroce::rdma::gpu::CudaContext::new(self.device)?);
        }
        debug_assert!(byte_len <= self.staging.len());
        feroce::rdma::gpu::copy_device_to_host(&mut self.staging[..byte_len], addr as u64)?;
        self.file.write_all(&self.staging[..byte_len])?;
        Ok(())
    }
}

#[cfg(feature = "gpu")]
pub struct GpuDumpFactory {
    base: PathBuf,
    buf_size: usize,
    device: i32,
}

#[cfg(feature = "gpu")]
impl GpuDumpFactory {
    pub fn new(base: PathBuf, buf_size: usize, device: i32) -> Self {
        Self {
            base,
            buf_size,
            device,
        }
    }
}

#[cfg(feature = "gpu")]
impl DumpSinkFactory for GpuDumpFactory {
    type Sink = GpuDumpSink;
    fn make(&self, stream_id: u32) -> Result<GpuDumpSink, FeroceError> {
        GpuDumpSink::open(&self.base, stream_id, self.buf_size, self.device)
    }
}

#[cfg(feature = "gpu")]
unsafe extern "C" {
    fn feroce_sum_batch(dptrs: *const u64, lens: *const u32, n: u32, out_host: *mut u64) -> i32;
}

#[cfg(feature = "gpu")]
pub struct SumSink {
    device: i32,
    ctx: Option<feroce::rdma::gpu::CudaContext>,
    max_batch: usize,
    msgs: u64,
    launches: u64,
}

#[cfg(feature = "gpu")]
impl DumpSink for SumSink {
    fn record(&mut self, addr: *mut u8, byte_len: usize) -> Result<(), FeroceError> {
        self.record_batch(&[(addr, byte_len)])
    }

    fn record_batch(&mut self, bufs: &[(*mut u8, usize)]) -> Result<(), FeroceError> {
        if bufs.is_empty() {
            return Ok(());
        }
        if self.ctx.is_none() {
            self.ctx = Some(feroce::rdma::gpu::CudaContext::new(self.device)?);
        }

        for chunk in bufs.chunks(self.max_batch) {
            let dptrs: Vec<u64> = chunk.iter().map(|&(a, _)| a as u64).collect();
            let lens: Vec<u32> = chunk.iter().map(|&(_, l)| l as u32).collect();
            let mut out = vec![0u64; chunk.len()];

            let code = unsafe {
                feroce_sum_batch(
                    dptrs.as_ptr(),
                    lens.as_ptr(),
                    chunk.len() as u32,
                    out.as_mut_ptr(),
                )
            };
            if code != 0 {
                return Err(FeroceError::Cuda {
                    call: "feroce_sum_batch",
                    code,
                });
            }
            self.launches += 1;
        }
        self.msgs += bufs.len() as u64;
        Ok(())
    }
}

#[cfg(feature = "gpu")]
impl Drop for SumSink {
    fn drop(&mut self) {
        if self.launches > 0 {
            let cap = if self.max_batch == usize::MAX {
                "unlimited".to_string()
            } else {
                self.max_batch.to_string()
            };
            log::info!(
                "SumSink: {} msgs in {} launches (avg {:.1} msgs/launch, max_batch={})",
                self.msgs,
                self.launches,
                self.msgs as f64 / self.launches as f64,
                cap
            );
        }
    }
}

#[cfg(feature = "gpu")]
pub struct SumFactory {
    device: i32,
    max_batch: usize,
}

#[cfg(feature = "gpu")]
impl SumFactory {
    pub fn new(device: i32) -> Self {
        let max_batch = std::env::var("FEROCE_SUM_MAX_BATCH")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(usize::MAX);
        Self { device, max_batch }
    }
}

#[cfg(feature = "gpu")]
impl DumpSinkFactory for SumFactory {
    type Sink = SumSink;
    fn make(&self, _stream_id: u32) -> Result<SumSink, FeroceError> {
        Ok(SumSink {
            device: self.device,
            ctx: None,
            max_batch: self.max_batch,
            msgs: 0,
            launches: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Read;

    fn scratch_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("feroce-dump-{}-{}.bin", std::process::id(), tag))
    }

    #[test]
    fn cpu_dump_sink_writes_payload() {
        let base = scratch_path("cpu_writes");
        let _ = fs::remove_file(derive_stream_path(&base, 0));

        {
            let mut sink = CpuDumpSink::open(&base, 0).unwrap();
            let mut payload = [0xDEu8, 0xAD, 0xBE, 0xEF];
            sink.record(payload.as_mut_ptr(), payload.len()).unwrap();
        }

        let path = derive_stream_path(&base, 0);
        let mut buf = Vec::new();
        File::open(&path).unwrap().read_to_end(&mut buf).unwrap();
        assert_eq!(buf, vec![0xDE, 0xAD, 0xBE, 0xEF]);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn cpu_dump_sink_concatenates_multiple_records() {
        let base = scratch_path("cpu_concat");
        let _ = fs::remove_file(derive_stream_path(&base, 0));

        {
            let mut sink = CpuDumpSink::open(&base, 0).unwrap();
            let mut a = [0x11u8; 4];
            let mut b = [0x22u8; 6];
            sink.record(a.as_mut_ptr(), a.len()).unwrap();
            sink.record(b.as_mut_ptr(), b.len()).unwrap();
        }
        let path = derive_stream_path(&base, 0);
        let mut buf = Vec::new();
        File::open(&path).unwrap().read_to_end(&mut buf).unwrap();
        assert_eq!(buf.len(), 10);
        assert_eq!(&buf[..4], &[0x11; 4]);
        assert_eq!(&buf[4..], &[0x22; 6]);

        let _ = fs::remove_file(&path);
    }
}

#[cfg(all(test, feature = "gpu"))]
mod gpu_tests {
    use super::feroce_sum_batch;
    use std::ffi::c_void;

    unsafe extern "C" {
        fn cudaMalloc(dev_ptr: *mut *mut c_void, size: usize) -> i32;
        fn cudaMemcpy(dst: *mut c_void, src: *const c_void, count: usize, kind: i32) -> i32;
        fn cudaFree(dev_ptr: *mut c_void) -> i32;
    }
    const CUDA_MEMCPY_HOST_TO_DEVICE: i32 = 1;

    fn stage_on_device(bytes: &[u8]) -> *mut c_void {
        let mut d: *mut c_void = std::ptr::null_mut();
        assert_eq!(unsafe { cudaMalloc(&mut d, bytes.len()) }, 0, "cudaMalloc");
        assert_eq!(
            unsafe {
                cudaMemcpy(
                    d,
                    bytes.as_ptr() as *const c_void,
                    bytes.len(),
                    CUDA_MEMCPY_HOST_TO_DEVICE,
                )
            },
            0,
            "cudaMemcpy H2D"
        );
        d
    }

    #[test]
    fn kernel_sums_bytes_correctly() {
        let _ctx = feroce::rdma::gpu::CudaContext::new(0).unwrap();

        let a: Vec<u8> = (0..1000u32).map(|i| (i % 256) as u8).collect();
        let mut b: Vec<u8> = vec![0u8; 4096];
        b[..8].copy_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());

        let exp_a: u64 = a.iter().map(|&x| x as u64).sum();
        let exp_b: u64 = b.iter().map(|&x| x as u64).sum();

        let da = stage_on_device(&a);
        let db = stage_on_device(&b);

        let dptrs = [da as u64, db as u64];
        let lens = [a.len() as u32, b.len() as u32];
        let mut out = [0u64; 2];
        let code = unsafe { feroce_sum_batch(dptrs.as_ptr(), lens.as_ptr(), 2, out.as_mut_ptr()) };
        assert_eq!(code, 0, "feroce_sum_batch returned CUDA error {code}");

        assert_eq!(out[0], exp_a, "buffer A sum");
        assert_eq!(out[1], exp_b, "buffer B sum");

        unsafe {
            cudaFree(da);
            cudaFree(db);
        }
    }
}
