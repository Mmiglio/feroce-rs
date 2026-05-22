use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

#[derive(Debug)]
pub struct StreamStats {
    pub id: u32,
    pub qpn: u32,
    pub remote_qpn: u32,
    pub messages: AtomicU64,
    pub bytes: AtomicU64,
    prev_messages: AtomicU64,
    prev_bytes: AtomicU64,
    started_at: Instant,
}

// per-tick stat snapshot
#[derive(Debug, Clone, Copy)]
pub struct StreamSample {
    pub stream_id: u32,
    pub qpn: u32,
    pub remote_qpn: u32,
    pub messages: u64,
    pub bytes: u64,
    pub msg_rate: f64,
    pub gbps: f64,
}

impl StreamStats {
    pub fn new(id: u32, qpn: u32, remote_qpn: u32) -> Self {
        StreamStats {
            id,
            qpn,
            remote_qpn,
            messages: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            prev_messages: AtomicU64::new(0),
            prev_bytes: AtomicU64::new(0),
            started_at: Instant::now(),
        }
    }

    pub fn sample(&self, interval: Duration) -> StreamSample {
        let msgs = self.messages.load(Ordering::Relaxed);
        let bytes = self.bytes.load(Ordering::Relaxed);
        let prev_msgs = self.prev_messages.swap(msgs, Ordering::Relaxed);
        let prev_bytes = self.prev_bytes.swap(bytes, Ordering::Relaxed);
        let secs = interval.as_secs_f64();

        StreamSample {
            stream_id: self.id,
            qpn: self.qpn,
            remote_qpn: self.remote_qpn,
            messages: msgs,
            bytes,
            msg_rate: (msgs - prev_msgs) as f64 / secs,
            gbps: (bytes - prev_bytes) as f64 * 8.0 / (secs * 1e9),
        }
    }

    pub fn print_summary(&self) {
        let total_time = Instant::now().duration_since(self.started_at);
        self.prev_messages.swap(0, Ordering::Relaxed);
        self.prev_bytes.swap(0, Ordering::Relaxed);
        println!(
            "Summary for stream {} (qp {} <-> {}), total duration {}s",
            self.id,
            self.qpn,
            self.remote_qpn,
            total_time.as_secs()
        );
        self.sample(total_time).print();
    }
}

impl StreamSample {
    pub fn print(&self) {
        println!(
            "Stream {} (qp {} <-> {}): {} msgs, {} bytes, {:.1} msg/s, {:.2} Gbit/s",
            self.stream_id,
            self.qpn,
            self.remote_qpn,
            self.messages,
            self.bytes,
            self.msg_rate,
            self.gbps,
        );
    }
}

// per-tick csv sink
pub struct StatsCsvWriter {
    writer: BufWriter<File>,
    start: Instant,
}

impl StatsCsvWriter {
    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut writer = BufWriter::new(File::create(path)?);
        writeln!(
            writer,
            "t_s,interval_s,stream,qpn,remote_qpn,messages,bytes,msg_rate,gbps"
        )?;
        Ok(Self {
            writer,
            start: Instant::now(),
        })
    }

    // all rows in one tick share the same t_s
    pub fn write_tick(&mut self, samples: &[StreamSample], interval: Duration) -> io::Result<()> {
        let t_s = self.start.elapsed().as_secs_f64();
        let dt = interval.as_secs_f64();
        for s in samples {
            writeln!(
                self.writer,
                "{t_s:.6},{dt:.6},{},{},{},{},{},{:.6},{:.6}",
                s.stream_id, s.qpn, s.remote_qpn, s.messages, s.bytes, s.msg_rate, s.gbps,
            )?;
        }
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn scratch_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("feroce-stats-{}-{}.csv", std::process::id(), tag))
    }

    #[test]
    fn csv_writer_emits_header_and_rows_per_tick() {
        let path = scratch_path("tick");
        let _ = fs::remove_file(&path);

        let samples = [
            StreamSample {
                stream_id: 0,
                qpn: 0x100,
                remote_qpn: 0x200,
                messages: 42,
                bytes: 4096,
                msg_rate: 42.0,
                gbps: 0.032768,
            },
            StreamSample {
                stream_id: 1,
                qpn: 0x101,
                remote_qpn: 0x201,
                messages: 7,
                bytes: 700,
                msg_rate: 7.0,
                gbps: 0.0056,
            },
        ];
        {
            let mut csv = StatsCsvWriter::create(&path).unwrap();
            csv.write_tick(&samples, Duration::from_secs(1)).unwrap();
        }

        let contents = fs::read_to_string(&path).unwrap();
        let mut lines = contents.lines();
        assert_eq!(
            lines.next().unwrap(),
            "t_s,interval_s,stream,qpn,remote_qpn,messages,bytes,msg_rate,gbps"
        );
        assert!(
            lines
                .next()
                .unwrap()
                .ends_with(",1.000000,0,256,512,42,4096,42.000000,0.032768")
        );
        assert!(
            lines
                .next()
                .unwrap()
                .ends_with(",1.000000,1,257,513,7,700,7.000000,0.005600")
        );
        assert!(lines.next().is_none());

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn sample_computes_interval_rates_and_advances_prev() {
        let s = StreamStats::new(0, 0x100, 0x200);
        s.messages.store(100, Ordering::Relaxed);
        s.bytes.store(1_000_000_000, Ordering::Relaxed);
        let a = s.sample(Duration::from_secs(1));
        assert_eq!(a.messages, 100);
        assert!((a.msg_rate - 100.0).abs() < 1e-9);
        assert!((a.gbps - 8.0).abs() < 1e-9);

        s.messages.store(150, Ordering::Relaxed);
        s.bytes.store(1_500_000_000, Ordering::Relaxed);
        let b = s.sample(Duration::from_secs(1));
        assert_eq!(b.messages, 150);
        assert!((b.msg_rate - 50.0).abs() < 1e-9);
        assert!((b.gbps - 4.0).abs() < 1e-9);
    }
}
