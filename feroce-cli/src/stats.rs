use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

#[derive(Debug)]
pub struct StreamStats {
    pub id: u32,
    pub qpn: u32,
    pub messages: AtomicU64,
    pub bytes: AtomicU64,
    // interval tracking
    prev_messages: AtomicU64,
    prev_bytes: AtomicU64,
    started_at: Instant,
}

impl StreamStats {
    pub fn new(id: u32, qpn: u32) -> Self {
        let now = Instant::now();
        StreamStats {
            id,
            qpn,
            messages: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            prev_messages: AtomicU64::new(0),
            prev_bytes: AtomicU64::new(0),
            started_at: now,
        }
    }

    pub fn interval_metrics(&self, interval: Duration) -> (u64, u64, f64, f64) {
        let msgs = self.messages.load(Ordering::Relaxed);
        let bytes = self.bytes.load(Ordering::Relaxed);
        let prev_msgs = self.prev_messages.swap(msgs, Ordering::Relaxed);
        let prev_bytes = self.prev_bytes.swap(bytes, Ordering::Relaxed);
        let secs = interval.as_secs_f64();

        let interval_msgs = msgs - prev_msgs;
        let interval_bytes = bytes - prev_bytes;

        let msg_rate = interval_msgs as f64 / secs;
        let gbps_rate = (interval_bytes as f64) * 8.0 / (secs * 1e9);

        (msgs, bytes, msg_rate, gbps_rate)
    }

    pub fn print_interval_metrics(&self, interval: Duration) {
        let (msgs, bytes, msg_rate, gbps_rate) = self.interval_metrics(interval);
        println!(
            "Stream {id} (qp {qpn}): {msgs} msgs, {bytes} bytes, {msg_rate:.1} msg/s, {gbps_rate:.2} Gbit/s",
            id = self.id,
            qpn = self.qpn,
        );
    }

    pub fn print_summary(&self) {
        let total_time = Instant::now().duration_since(self.started_at);

        self.prev_messages.swap(0, Ordering::Relaxed);
        self.prev_bytes.swap(0, Ordering::Relaxed);

        println!(
            "Summary for stream {} (qp {}), total duration {}s",
            self.id,
            self.qpn,
            total_time.as_secs()
        );
        self.print_interval_metrics(total_time);
    }
}
