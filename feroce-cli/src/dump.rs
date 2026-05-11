use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use feroce::FeroceError;

const DUMP_BUF_CAP: usize = 1 << 20;

pub trait DumpSink: Send + 'static {
    fn record(&mut self, addr: *mut u8, byte_len: usize) -> Result<(), FeroceError>;
}

pub trait DumpSinkFactory: Send {
    type Sink: DumpSink;
    fn make(&self, stream_id: u32) -> Result<Self::Sink, FeroceError>;
}

pub struct NoDump;

impl DumpSink for NoDump {
    #[inline(always)]
    fn record(&mut self, _addr: *mut u8, _byte_len: usize) -> Result<(), FeroceError> {
        Ok(())
    }
}

pub struct NoDumpFactory;

impl DumpSinkFactory for NoDumpFactory {
    type Sink = NoDump;
    fn make(&self, _stream_id: u32) -> Result<NoDump, FeroceError> {
        Ok(NoDump)
    }
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
    ctx: feroce::rdma::gpu::CudaCtxToken,
    ctx_attached: bool,
}

#[cfg(feature = "gpu")]
impl GpuDumpSink {
    pub fn open(
        base: &Path,
        stream_id: u32,
        buf_size: usize,
        ctx: feroce::rdma::gpu::CudaCtxToken,
    ) -> Result<Self, FeroceError> {
        let path = derive_stream_path(base, stream_id);
        let file = File::create(&path)?;
        Ok(Self {
            file: BufWriter::with_capacity(DUMP_BUF_CAP, file),
            staging: vec![0u8; buf_size],
            ctx,
            ctx_attached: false,
        })
    }
}

#[cfg(feature = "gpu")]
impl DumpSink for GpuDumpSink {
    fn record(&mut self, addr: *mut u8, byte_len: usize) -> Result<(), FeroceError> {
        // CUDA contexts are thread-local. record() runs on the poller thread,
        // which doesn't share the main thread's context stack — bind it once.
        if !self.ctx_attached {
            self.ctx.set_current()?;
            self.ctx_attached = true;
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
    ctx: feroce::rdma::gpu::CudaCtxToken,
}

#[cfg(feature = "gpu")]
impl GpuDumpFactory {
    // Captures the CUDA context currently bound to the calling thread. Must be
    // invoked after `GpuAllocator::new`, on the same thread. TODO: clean a bit
    pub fn new(base: PathBuf, buf_size: usize) -> Result<Self, FeroceError> {
        let ctx = feroce::rdma::gpu::CudaCtxToken::current()?;
        Ok(Self {
            base,
            buf_size,
            ctx,
        })
    }
}

#[cfg(feature = "gpu")]
impl DumpSinkFactory for GpuDumpFactory {
    type Sink = GpuDumpSink;
    fn make(&self, stream_id: u32) -> Result<GpuDumpSink, FeroceError> {
        GpuDumpSink::open(&self.base, stream_id, self.buf_size, self.ctx)
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
    fn no_dump_is_noop() {
        let mut sink = NoDump;
        assert!(sink.record(std::ptr::null_mut(), 0).is_ok());
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
