use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::Path,
    time::{Duration, Instant},
};

use feroce_runtime::{GroupSample, MetricSample, StreamMetrics};

pub fn format_bytes(b: u64) -> String {
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut val = b as f64;
    let mut unit_idx = 0;
    while val > 1000. && unit_idx < units.len() - 1 {
        val /= 1000.0;
        unit_idx += 1;
    }

    format!("{:.2} {}", val, units[unit_idx])
}

pub fn rate_display(m: &MetricSample) -> (f64, &'static str) {
    if m.name == "bytes" {
        (m.rate * 8.0 / 1e9, "Gbit/s")
    } else {
        (m.rate, "msg/s")
    }
}

pub fn format_rate(m: &MetricSample) -> String {
    let (value, unit) = rate_display(m);
    format!("{value:.2} {unit}")
}

pub fn format_value(m: &MetricSample) -> String {
    if m.name == "bytes" {
        format_bytes(m.value)
    } else {
        m.value.to_string()
    }
}

fn format_metric(m: &MetricSample) -> String {
    format!("{} {} ({})", m.name, format_value(m), format_rate(m))
}

pub fn print_sample(s: &GroupSample) {
    let labels: Vec<String> = s.labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
    let metrics: Vec<String> = s.metrics.iter().map(format_metric).collect();
    println!("{} | {}", labels.join(" "), metrics.join(" | "));
}

pub fn print_summary(m: &StreamMetrics) {
    println!("Summary over {}s:", m.group.uptime().as_secs());
    print_sample(&m.group.sample_total());
}

// per-tick csv sink
// shortcut: columns fixed by the first tick, use a long format if streams ever register late
pub struct StatsCsvWriter {
    writer: BufWriter<File>,
    start: Instant,
    labels: Option<Vec<&'static str>>,
    metrics: Option<Vec<&'static str>>,
}

impl StatsCsvWriter {
    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Ok(Self {
            writer: BufWriter::new(File::create(path)?),
            start: Instant::now(),
            labels: None,
            metrics: None,
        })
    }

    fn write_header(&mut self, first: &GroupSample) -> io::Result<()> {
        let labels: Vec<&'static str> = first.labels.iter().map(|(k, _)| *k).collect();
        let metrics: Vec<&'static str> = first.metrics.iter().map(|m| m.name).collect();

        write!(self.writer, "t_s,interval_s")?;
        for k in &labels {
            write!(self.writer, ",{k}")?;
        }
        for name in &metrics {
            write!(self.writer, ",{name},{name}_rate")?;
        }
        writeln!(self.writer)?;

        self.labels = Some(labels);
        self.metrics = Some(metrics);
        Ok(())
    }

    // all rows in one tick share the same t_s
    pub fn write_tick(&mut self, samples: &[GroupSample], interval: Duration) -> io::Result<()> {
        let Some(first) = samples.first() else {
            return Ok(());
        };
        if self.metrics.is_none() {
            self.write_header(first)?;
        }
        let labels = self.labels.clone().unwrap();
        let metrics = self.metrics.clone().unwrap();

        let t_s = self.start.elapsed().as_secs_f64();
        let dt = interval.as_secs_f64();
        for s in samples {
            write!(self.writer, "{t_s:.6},{dt:.6}")?;
            for key in &labels {
                let value = s
                    .labels
                    .iter()
                    .find(|(k, _)| k == key)
                    .map(|(_, v)| v.as_str())
                    .unwrap_or("");
                write!(self.writer, ",{value}")?;
            }
            for name in &metrics {
                match s.get(name) {
                    Some(m) => write!(self.writer, ",{},{:.6}", m.value, m.rate)?,
                    None => write!(self.writer, ",,")?,
                }
            }
            writeln!(self.writer)?;
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

    fn sample(stream: u32, qpn: u32, remote_qpn: u32, msgs: u64, bytes: u64) -> GroupSample {
        GroupSample {
            labels: vec![
                ("stream", stream.to_string()),
                ("qpn", qpn.to_string()),
                ("remote_qpn", remote_qpn.to_string()),
            ],
            metrics: vec![
                MetricSample {
                    name: "messages",
                    value: msgs,
                    rate: msgs as f64,
                },
                MetricSample {
                    name: "bytes",
                    value: bytes,
                    rate: bytes as f64,
                },
            ],
        }
    }

    #[test]
    fn csv_writer_derives_header_and_emits_rows_per_tick() {
        let path = scratch_path("tick");
        let _ = fs::remove_file(&path);

        {
            let mut csv = StatsCsvWriter::create(&path).unwrap();
            csv.write_tick(
                &[
                    sample(0, 0x100, 0x200, 42, 4096),
                    sample(1, 0x101, 0x201, 7, 700),
                ],
                Duration::from_secs(1),
            )
            .unwrap();
        }

        let contents = fs::read_to_string(&path).unwrap();
        let mut lines = contents.lines();
        assert_eq!(
            lines.next().unwrap(),
            "t_s,interval_s,stream,qpn,remote_qpn,messages,messages_rate,bytes,bytes_rate"
        );
        assert!(
            lines
                .next()
                .unwrap()
                .ends_with(",1.000000,0,256,512,42,42.000000,4096,4096.000000")
        );
        assert!(
            lines
                .next()
                .unwrap()
                .ends_with(",1.000000,1,257,513,7,7.000000,700,700.000000")
        );
        assert!(lines.next().is_none());

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn bytes_metric_is_rendered_as_link_rate() {
        let s = sample(0, 0x100, 0x200, 1, 2_000_000_000);
        let bytes = s.get("bytes").unwrap();
        assert_eq!(format_value(bytes), "2.00 GB");
        assert_eq!(format_rate(bytes), "16.00 Gbit/s");

        let msgs = s.get("messages").unwrap();
        assert_eq!(format_value(msgs), "1");
        assert_eq!(format_rate(msgs), "1.00 msg/s");
    }
}
