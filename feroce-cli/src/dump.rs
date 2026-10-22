use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use feroce::FeroceError;

const DUMP_BUF_CAP: usize = 1 << 20;
const HDR_LEN: usize = 16;

pub trait DumpSink: Send + 'static {
    fn record(&mut self, addr: *mut u8, byte_len: u32, imm_data: u32) -> Result<(), FeroceError>;
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

fn write_header(w: &mut impl Write, seq: u64, imm_data: u32, len: u32) -> std::io::Result<()> {
    // 16B header with [msg sequence number, immediate data, payload length]
    let mut h = [0u8; HDR_LEN];
    h[..8].copy_from_slice(&seq.to_le_bytes());
    h[8..12].copy_from_slice(&imm_data.to_le_bytes());
    h[12..].copy_from_slice(&len.to_le_bytes());
    w.write_all(&h)
}

pub struct CpuDumpSink {
    file: BufWriter<File>,
    seq: u64,
}

impl CpuDumpSink {
    pub fn open(base: &Path, stream_id: u32) -> Result<Self, FeroceError> {
        let path = derive_stream_path(base, stream_id);
        let file = File::create(&path)?;
        Ok(Self {
            file: BufWriter::with_capacity(DUMP_BUF_CAP, file),
            seq: 0,
        })
    }
}

impl DumpSink for CpuDumpSink {
    fn record(&mut self, addr: *mut u8, byte_len: u32, imm_data: u32) -> Result<(), FeroceError> {
        // addr is the registered RDMA buffer for this WC, will not be overwritten untill reposted
        let bytes = unsafe { std::slice::from_raw_parts(addr, byte_len as usize) };
        write_header(&mut self.file, self.seq, imm_data, byte_len)?;
        self.file.write_all(bytes)?;
        self.seq += 1;
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
    seq: u64,
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
            seq: 0,
            staging: vec![0u8; buf_size],
            device,
            ctx: None,
        })
    }
}

#[cfg(feature = "gpu")]
impl DumpSink for GpuDumpSink {
    fn record(&mut self, addr: *mut u8, byte_len: u32, imm_data: u32) -> Result<(), FeroceError> {
        // CUDA contexts are thread-local. record() runs on the poller thread,
        // which doesn't share the main thread's context stack
        if self.ctx.is_none() {
            self.ctx = Some(feroce::rdma::gpu::CudaContext::new(self.device)?);
        }
        let n_bytes: usize = byte_len as usize;
        assert!(n_bytes <= self.staging.len());
        feroce::rdma::gpu::copy_device_to_host(&mut self.staging[..n_bytes], addr as u64)?;

        write_header(&mut self.file, self.seq, imm_data, byte_len)?;
        self.file.write_all(&self.staging[..n_bytes])?;
        self.seq += 1;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Read;

    fn scratch_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("feroce-dump-{}-{}.bin", std::process::id(), tag))
    }

    fn unpack_header(buff: &[u8]) -> (u64, u32, u32) {
        let seq = u64::from_le_bytes(buff[0..8].try_into().unwrap());
        let imm = u32::from_le_bytes(buff[8..12].try_into().unwrap());
        let len = u32::from_le_bytes(buff[12..16].try_into().unwrap());

        (seq, imm, len)
    }

    #[test]
    fn cpu_dump_sink_writes_payload() {
        let base = scratch_path("cpu_writes");
        let _ = fs::remove_file(derive_stream_path(&base, 0));

        {
            let mut sink = CpuDumpSink::open(&base, 0).unwrap();
            let mut payload: [u8; 4] = [0xDE, 0xAD, 0xBE, 0xEF];
            sink.record(payload.as_mut_ptr(), payload.len() as u32, 0xDEADBEEF)
                .unwrap();
        }

        let path = derive_stream_path(&base, 0);
        let mut buf = Vec::new();
        File::open(&path).unwrap().read_to_end(&mut buf).unwrap();
        assert_eq!(buf.len(), HDR_LEN + 4); // check header + data size
        let (seq, imm, len) = unpack_header(&buf[0..HDR_LEN]);
        // check the header
        assert_eq!(seq, 0);
        assert_eq!(imm, 0xDEADBEEF);
        assert_eq!(len, 4);
        // check payload
        assert_eq!(buf[HDR_LEN..], vec![0xDE, 0xAD, 0xBE, 0xEF]);

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
            sink.record(a.as_mut_ptr(), a.len() as u32, 0xDEAD).unwrap();
            sink.record(b.as_mut_ptr(), b.len() as u32, 0xBEEF).unwrap();
        }

        let path = derive_stream_path(&base, 0);
        let mut buf = Vec::new();
        File::open(&path).unwrap().read_to_end(&mut buf).unwrap();
        assert_eq!(buf.len(), 2 * HDR_LEN + 10);

        // check message 1
        let (seq, imm, len) = unpack_header(&buf[0..HDR_LEN]);
        assert_eq!(seq, 0);
        assert_eq!(imm, 0xDEAD);
        assert_eq!(len, 4);
        assert_eq!(&buf[HDR_LEN..HDR_LEN + 4], &[0x11; 4]);

        // check message 2
        let msg2_offset = HDR_LEN + 4;
        let (seq2, imm2, len2) = unpack_header(&buf[msg2_offset..msg2_offset + HDR_LEN]);
        assert_eq!(seq2, 1);
        assert_eq!(imm2, 0xBEEF);
        assert_eq!(len2, 6);
        assert_eq!(
            &buf[(msg2_offset + HDR_LEN)..(msg2_offset + HDR_LEN + 6)],
            &[0x22; 6]
        );

        let _ = fs::remove_file(&path);
    }
}
