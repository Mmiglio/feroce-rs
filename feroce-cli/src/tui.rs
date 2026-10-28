use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    crossterm::terminal::{
        EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
    },
    layout::{Constraint, Layout},
    style::{Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Cell, Paragraph, Row, Table},
};

use std::{
    collections::VecDeque,
    error::Error,
    io::{Stdout, stdout},
    sync::{Arc, Mutex},
    time::Duration,
};

use feroce_runtime::{GroupSample, MetricSample};

use crate::stats::{format_rate, format_value, rate_display};
pub struct Tui {
    terminal: Terminal<CrosstermBackend<Stdout>>,
    log_buffer: Arc<Mutex<VecDeque<String>>>,
}

impl Tui {
    pub fn new(log_buffer: Arc<Mutex<VecDeque<String>>>) -> Result<Self, Box<dyn Error>> {
        let backend = CrosstermBackend::new(stdout());
        let mut terminal = Terminal::new(backend)?;

        enable_raw_mode()?;
        ratatui::crossterm::execute!(stdout(), EnterAlternateScreen)?;

        let original_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic_info| {
            let _ = ratatui::crossterm::execute!(stdout(), LeaveAlternateScreen);
            let _ = disable_raw_mode();
            original_hook(panic_info);
        }));

        terminal.clear()?;

        Ok(Tui {
            terminal,
            log_buffer,
        })
    }

    pub fn draw(&mut self, samples: &[GroupSample]) -> Result<bool, Box<dyn Error>> {
        let logs = self.log_buffer.lock().unwrap();
        self.terminal
            .draw(|frame| Tui::draw_frame(frame, samples, &logs))?;
        drop(logs);

        // check for Ctrl+C or 'q' keypress
        use ratatui::crossterm::event::{self, Event, KeyCode, KeyModifiers};
        if event::poll(Duration::from_millis(0))? {
            if let Event::Key(key) = event::read()? {
                if (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
                    || key.code == KeyCode::Char('q')
                {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    const SHOWN: [&'static str; 2] = ["messages", "bytes"];

    fn shown(sample: &GroupSample) -> impl Iterator<Item = &MetricSample> {
        Self::SHOWN.iter().filter_map(|n| sample.get(n))
    }

    fn header_cells(sample: &GroupSample) -> Vec<String> {
        let mut cols: Vec<String> = sample.labels.iter().map(|(k, _)| k.to_string()).collect();
        for m in Self::shown(sample) {
            cols.push(m.name.to_string());
            cols.push(rate_display(m).1.to_string());
        }
        cols
    }

    fn row_cells(sample: &GroupSample) -> Vec<String> {
        let mut cols: Vec<String> = sample.labels.iter().map(|(_, v)| v.clone()).collect();
        for m in Self::shown(sample) {
            cols.push(format_value(m));
            cols.push(format!("{:.2}", rate_display(m).0));
        }
        cols
    }

    fn totals_line(samples: &[GroupSample]) -> Line<'static> {
        let Some(first) = samples.first() else {
            return Line::from("no open streams".to_string());
        };

        let mut spans: Vec<Span> = vec!["Total: ".bold()];
        for m in Self::shown(first) {
            let matching = || samples.iter().filter_map(|s| s.get(m.name));
            let total = MetricSample {
                value: matching().map(|x| x.value).sum(),
                rate: matching().map(|x| x.rate).sum(),
                ..*m
            };

            if spans.len() > 1 {
                spans.push("  |  ".into());
            }
            spans.push(format!("{} {}", total.name, format_value(&total)).into());
            let rate = format!(" ({})", format_rate(&total));
            spans.push(if total.name == "bytes" {
                rate.bold().green()
            } else {
                rate.into()
            });
        }
        Line::from(spans)
    }

    fn draw_frame(frame: &mut Frame, samples: &[GroupSample], logs: &VecDeque<String>) {
        let [top, bottom] =
            Layout::vertical([Constraint::Percentage(50), Constraint::Percentage(50)])
                .areas(frame.area());

        let [table_area, totals_area] =
            Layout::vertical([Constraint::Min(0), Constraint::Length(3)]).areas(top);

        // render connections
        let (header_cells, num_labels) = match samples.first() {
            Some(first) => (Tui::header_cells(first), first.labels.len()),
            None => (Vec::new(), 0),
        };
        let header = Row::new(header_cells.clone()).style(Style::new().cyan().bold());

        let rows: Vec<Row> = samples
            .iter()
            .map(|s| Row::new(Tui::row_cells(s).into_iter().map(Cell::from)))
            .collect();

        let widths: Vec<Constraint> = (0..header_cells.len())
            .map(|i| {
                if i < num_labels {
                    Constraint::Min(8)
                } else {
                    Constraint::Min(12)
                }
            })
            .collect();

        let table = Table::new(rows, widths)
            .header(header)
            .block(Block::bordered().title("Open QPs".cyan().bold()));

        frame.render_widget(table, table_area);

        let totals = Paragraph::new(Tui::totals_line(samples))
            .block(Block::bordered().title("Aggregated".cyan().bold()));

        frame.render_widget(totals, totals_area);

        // Render logs tab

        let log_lines: Vec<Line> = logs
            .iter()
            .map(|s| {
                if let Some(end) = s.find(']') {
                    let (prefix, rest) = s.split_at(end + 1);
                    let styled_prefix: Span = match prefix {
                        "[ERROR]" => prefix.red(),
                        "[WARN]" => prefix.yellow(),
                        "[INFO]" => prefix.green(),
                        "[DEBUG]" => prefix.blue(),
                        _ => Span::raw(prefix),
                    };
                    Line::from(vec![styled_prefix, Span::raw(rest)])
                } else {
                    Line::from(Span::raw(s.as_str()))
                }
            })
            .collect();
        let visible_height = bottom.height.saturating_sub(2);
        let scroll = (log_lines.len() as u16).saturating_sub(visible_height);

        let log_panel = Paragraph::new(log_lines)
            .block(Block::bordered().title("Logs".cyan().bold()))
            .scroll((scroll, 0));

        frame.render_widget(log_panel, bottom);
    }
}

impl Drop for Tui {
    fn drop(&mut self) {
        let _ = ratatui::crossterm::execute!(stdout(), LeaveAlternateScreen);
        let _ = disable_raw_mode();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(stream: u32, msgs: u64, bytes: u64, extra: Option<u64>) -> GroupSample {
        let mut metrics = vec![
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
        ];
        if let Some(d) = extra {
            metrics.push(MetricSample {
                name: "dropped",
                value: d,
                rate: d as f64,
            });
        }
        GroupSample {
            labels: vec![("stream", stream.to_string()), ("qpn", "256".to_string())],
            metrics,
        }
    }

    #[test]
    fn table_shows_messages_and_bytes_only() {
        let s = sample(0, 1, 2, Some(3));
        assert_eq!(
            Tui::header_cells(&s),
            ["stream", "qpn", "messages", "msg/s", "bytes", "Gbit/s"]
        );
        assert_eq!(
            Tui::row_cells(&s),
            ["0", "256", "1", "1.00", "2.00 B", "0.00"]
        );
    }

    #[test]
    fn totals_sum_across_streams_and_render_bytes_as_a_link_rate() {
        let samples = [
            sample(0, 10, 1_000_000_000, None),
            sample(1, 5, 1_000_000_000, None),
        ];
        let line = Tui::totals_line(&samples).to_string();
        assert!(line.contains("messages 15"), "{line}");
        assert!(line.contains("2.00 GB"), "{line}");
        assert!(line.contains("16.00 Gbit/s"), "{line}");

        assert_eq!(Tui::totals_line(&[]).to_string(), "no open streams");
    }
}
