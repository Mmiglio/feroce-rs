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

use crate::stats::StreamStats;
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

    pub fn draw(
        &mut self,
        stats: &[Arc<StreamStats>],
        elapsed: Duration,
    ) -> Result<bool, Box<dyn Error>> {
        let logs = self.log_buffer.lock().unwrap();
        self.terminal
            .draw(|frame| Tui::draw_frame(frame, stats, &logs, elapsed))?;
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

    fn draw_frame(
        frame: &mut Frame,
        stats: &[Arc<StreamStats>],
        logs: &VecDeque<String>,
        elapsed: Duration,
    ) {
        let [top, bottom] =
            Layout::vertical([Constraint::Percentage(50), Constraint::Percentage(50)])
                .areas(frame.area());

        let [table_area, totals_area] =
            Layout::vertical([Constraint::Min(0), Constraint::Length(3)]).areas(top);

        // render connections
        let header = Row::new([
            "Stream", "QPN", "Remote QPN", "Messages", "Bytes", "msg/s", "Gbit/s",
        ])
        .style(Style::new().cyan().bold());

        let mut total_msgs = 0u64;
        let mut total_bytes = 0u64;
        let mut total_msg_rate = 0.0;
        let mut total_gbps = 0.0;

        let mut rows: Vec<Row> = Vec::with_capacity(stats.len());
        for s in stats {
            let (msgs, bytes, msg_rate, gbps) = s.interval_metrics(elapsed);
            total_msgs += msgs;
            total_bytes += bytes;
            total_msg_rate += msg_rate;
            total_gbps += gbps;
            rows.push(Row::new([
                Cell::from(s.id.to_string()),
                Cell::from(s.qpn.to_string()),
                Cell::from(s.remote_qpn.to_string()),
                Cell::from(msgs.to_string()),
                Cell::from(Tui::format_bytes(bytes)),
                Cell::from(format!("{:.1}", msg_rate)),
                Cell::from(format!("{:.2}", gbps)),
            ]));
        }

        let table = Table::new(
            rows,
            [
                Constraint::Min(8),  // Stream
                Constraint::Min(8),  // QPN
                Constraint::Min(12), // Remote QPN
                Constraint::Min(12), // Messages
                Constraint::Min(12), // Bytes
                Constraint::Min(12), // msg/s
                Constraint::Min(10), // Gbit/s
            ],
        )
        .header(header)
        .block(Block::bordered().title("Open QPs".cyan().bold()));

        frame.render_widget(table, table_area);

        let totals_line = Line::from(vec![
            "Total: ".bold(),
            format!("{} msgs", total_msgs).into(),
            "  |  ".into(),
            Tui::format_bytes(total_bytes).into(),
            "  |  ".into(),
            format!("{:.1} msg/s", total_msg_rate).into(),
            "  |  ".into(),
            format!("{:.2} Gbit/s", total_gbps).bold().green(),
        ]);

        let totals = Paragraph::new(totals_line)
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

    pub fn restore(&mut self) -> Result<(), Box<dyn Error>> {
        ratatui::crossterm::execute!(stdout(), LeaveAlternateScreen)?;
        disable_raw_mode()?;

        Ok(())
    }

    fn format_bytes(b: u64) -> String {
        let units = ["B", "KB", "MB", "GB", "TB"];
        let mut val = b as f64;
        let mut unit_idx = 0;
        while val > 1000. && unit_idx < units.len() - 1 {
            val /= 1000.0;
            unit_idx += 1;
        }

        format!("{:.2} {}", val, units[unit_idx])
    }
}
