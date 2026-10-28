use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct Metric {
    name: &'static str,
    value: AtomicU64,
    prev: AtomicU64,
}

impl Metric {
    #[inline]
    pub fn add(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc(&self) {
        self.add(1);
    }

    pub fn value(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    fn sample_from(&self, value: u64, prev: u64, interval: Duration) -> MetricSample {
        let secs = interval.as_secs_f64();
        MetricSample {
            name: self.name,
            value,
            rate: if secs > 0.0 {
                value.saturating_sub(prev) as f64 / secs
            } else {
                0.0
            },
        }
    }

    // Advances the baseline, so call it once per tick.
    fn sample(&self, interval: Duration) -> MetricSample {
        let value = self.value.load(Ordering::Relaxed);
        let prev = self.prev.swap(value, Ordering::Relaxed);
        self.sample_from(value, prev, interval)
    }

    // Run average, without disturbing the per-tick baseline.
    fn sample_total(&self, total: Duration) -> MetricSample {
        self.sample_from(self.value.load(Ordering::Relaxed), 0, total)
    }
}

/// Metrics sharing a label set, one group per stream.
#[derive(Debug)]
pub struct MetricGroup {
    labels: Vec<(&'static str, String)>,
    metrics: Mutex<Vec<Arc<Metric>>>,
    started_at: Instant,
}

impl MetricGroup {
    pub fn new(labels: Vec<(&'static str, String)>) -> Self {
        MetricGroup {
            labels,
            metrics: Mutex::new(Vec::new()),
            started_at: Instant::now(),
        }
    }

    pub fn register(&self, name: &'static str) -> Arc<Metric> {
        let mut metrics = self.metrics.lock().unwrap();
        assert!(
            !metrics.iter().any(|m| m.name == name),
            "metric {name} already registered in this group"
        );
        let metric = Arc::new(Metric {
            name,
            value: AtomicU64::new(0),
            prev: AtomicU64::new(0),
        });
        metrics.push(Arc::clone(&metric));
        metric
    }

    pub fn sample(&self, interval: Duration) -> GroupSample {
        GroupSample {
            labels: self.labels.clone(),
            metrics: self
                .metrics
                .lock()
                .unwrap()
                .iter()
                .map(|m| m.sample(interval))
                .collect(),
        }
    }

    pub fn sample_total(&self) -> GroupSample {
        let total = self.uptime();
        GroupSample {
            labels: self.labels.clone(),
            metrics: self
                .metrics
                .lock()
                .unwrap()
                .iter()
                .map(|m| m.sample_total(total))
                .collect(),
        }
    }

    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MetricSample {
    pub name: &'static str,
    pub value: u64,
    pub rate: f64,
}

/// Labels and metrics keep registration order.
#[derive(Debug, Clone)]
pub struct GroupSample {
    pub labels: Vec<(&'static str, String)>,
    pub metrics: Vec<MetricSample>,
}

impl GroupSample {
    pub fn get(&self, name: &str) -> Option<&MetricSample> {
        self.metrics.iter().find(|m| m.name == name)
    }
}

#[derive(Clone)]
pub struct StreamMetrics {
    pub group: Arc<MetricGroup>,
    pub messages: Arc<Metric>,
    pub bytes: Arc<Metric>,
}

impl StreamMetrics {
    pub fn new(stream_id: u32, qpn: u32, remote_qpn: u32) -> Self {
        let group = Arc::new(MetricGroup::new(vec![
            ("stream", stream_id.to_string()),
            ("qpn", qpn.to_string()),
            ("remote_qpn", remote_qpn.to_string()),
        ]));
        let messages = group.register("messages");
        let bytes = group.register("bytes");
        StreamMetrics {
            group,
            messages,
            bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_rates_over_the_interval_and_advances_the_baseline() {
        let m = StreamMetrics::new(0, 0x100, 0x200);

        m.messages.add(100);
        let a = m.group.sample(Duration::from_secs(1));
        assert_eq!(a.get("messages").unwrap().value, 100);
        assert!((a.get("messages").unwrap().rate - 100.0).abs() < 1e-9);

        m.messages.add(50);
        let b = m.group.sample(Duration::from_secs(1));
        assert_eq!(b.get("messages").unwrap().value, 150);
        assert!((b.get("messages").unwrap().rate - 50.0).abs() < 1e-9);

        assert_eq!(m.messages.sample(Duration::ZERO).rate, 0.0);
    }

    #[test]
    fn sample_total_averages_over_the_run_and_keeps_the_tick_baseline() {
        let m = StreamMetrics::new(0, 0x100, 0x200);

        m.messages.add(100);
        m.messages.sample(Duration::from_secs(1));
        m.messages.add(20);

        let total = m.messages.sample_total(Duration::from_secs(10));
        assert_eq!(total.value, 120);
        assert!((total.rate - 12.0).abs() < 1e-9);

        assert!((m.messages.sample(Duration::from_secs(1)).rate - 20.0).abs() < 1e-9);
    }
}
