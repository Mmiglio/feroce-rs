pub mod metrics;
pub mod session;

// re-export: apps reach the primitives without declaring their own dependency
pub use feroce;

pub use metrics::{GroupSample, Metric, MetricGroup, MetricSample, StreamMetrics};
pub use session::{Monitor, OnStreamClosed, SessionConfig, SessionRunner, run_session};
