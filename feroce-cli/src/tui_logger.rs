use std::{
    collections::VecDeque,
    error::Error,
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    sync::{Arc, Mutex},
};

use log::LevelFilter;

pub struct TuiLogger {
    /// Hold the last N log messages
    buffer: Arc<Mutex<VecDeque<String>>>,
    /// Dump logs to file for persistence
    file: Mutex<BufWriter<File>>,
    /// Number of log messages to display in the TUI
    max_lines: usize,
}

impl log::Log for TuiLogger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        let msg = format!(
            "[{}] {} - {}",
            record.level(),
            record.target(),
            record.args()
        );

        // write log to file
        {
            let mut file_guard = self.file.lock().unwrap();
            let _ = writeln!(file_guard, "{}", msg);
            let _ = file_guard.flush();
        }

        // add log to the ring buffer
        {
            let mut buff_guard = self.buffer.lock().unwrap();
            if buff_guard.len() == self.max_lines {
                buff_guard.pop_front();
            }

            buff_guard.push_back(msg);
        }
    }

    fn flush(&self) {
        let mut guard = self.file.lock().unwrap();
        guard.flush().unwrap();
    }
}

/// Create the shared log buffer which we will then pass to the TUI renderer
pub fn init(
    log_file: &Path,
    max_level: LevelFilter,
    max_lines: usize,
) -> Result<Arc<Mutex<VecDeque<String>>>, Box<dyn Error>> {
    let log_file = BufWriter::new(File::create(log_file)?);

    let log_buffer = Arc::new(Mutex::new(VecDeque::new()));

    // create the logger, which we will then leak as log::set_logger requires it
    // to live for the entire execution
    let logger = TuiLogger {
        buffer: Arc::clone(&log_buffer),
        file: Mutex::new(log_file),
        max_lines,
    };

    let static_logger = Box::leak(Box::new(logger));

    log::set_logger(static_logger)?;
    log::set_max_level(max_level);

    Ok(log_buffer)
}
