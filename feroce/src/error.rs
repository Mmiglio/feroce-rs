use std::io;

#[derive(Debug)]
pub enum FeroceError {
    /// OS / socket errors
    Io(io::Error),
    /// CM protocol violations
    Protocol(String),
    /// CM timeout
    Timeout,
    /// Invalid argument
    InvalidArg(String),
    /// RDMA verbs failures
    Rdma { call: &'static str, errno: i32 },
    /// CUDA failures
    #[cfg(feature = "gpu")]
    Cuda { call: &'static str, code: i32 },
}

impl std::error::Error for FeroceError {}

impl From<io::Error> for FeroceError {
    fn from(err: io::Error) -> Self {
        FeroceError::Io(err)
    }
}

impl std::fmt::Display for FeroceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FeroceError::Io(e) => write!(f, "IO error: {}", e),
            FeroceError::Protocol(msg) => write!(f, "Protocol error: {}", msg),
            FeroceError::Timeout => write!(f, "Timeout"),
            FeroceError::InvalidArg(msg) => write!(f, "Invalid argument: {}", msg),
            FeroceError::Rdma { call, errno } => write!(f, "{call} failed (errno {errno})"),
            #[cfg(feature = "gpu")]
            FeroceError::Cuda { call, code } => write!(f, "{call} failed (CUDA errno {code})"),
        }
    }
}
