pub mod metrics;
pub mod session;

// re-export: apps reach the primitives without declaring their own dependency
pub use feroce;

pub use metrics::{GroupSample, Metric, MetricGroup, MetricSample, StreamMetrics};
pub use session::{
    Monitor, OnStreamClosed, Peers, SessionConfig, SessionRunner, StreamSpec, run_session,
};
