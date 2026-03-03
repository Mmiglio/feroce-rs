use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

#[derive(Debug, Default)]
pub struct StreamStats {
    pub id: u32,
    pub messages: AtomicU64,
    pub bytes: AtomicU64,
    // more ?
}

impl StreamStats {
    pub fn new(id: u32) -> Self {
        StreamStats {
            id,
            ..Default::default()
        }
    }

    pub fn print_metrics(&self, duration: Duration, prev_msgs: u64, prev_bytes: u64) -> (u64, u64) {
        let msgs = self.messages.load(Ordering::Relaxed);
        let bytes = self.bytes.load(Ordering::Relaxed);
        let secs = duration.as_secs_f64();

        let interval_msgs = msgs - prev_msgs;
        let interval_bytes = bytes - prev_bytes;

        println!(
            "Stream {id}: {tot_msgs} msgs, {tot_bytes} bytes, {rate_msg:.1} msg/s, {rate_bytes:.2} Gbit/s",
            id = self.id,
            tot_msgs = msgs,
            tot_bytes = bytes,
            rate_msg = interval_msgs as f64 / secs,
            rate_bytes = (interval_bytes as f64) / (secs * 1024.0 * 1024.0 * 119.2),
        );

        (msgs, bytes)
    }
}
