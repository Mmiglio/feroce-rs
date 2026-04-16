use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    buffer::Buffer,
    crossterm::terminal::{
        EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
    },
    layout::{Constraint, Layout, Rect},
    style::Stylize,
    symbols::border,
    text::{Line, Text},
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
                if (key.code == KeyCode::Char('c')
                    && key.modifiers.contains(KeyModifiers::CONTROL))
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

        // render connections
        let header = Row::new(["Stream", "QPN", "Messages", "msg/s", "Gbit/s"]);

        let rows: Vec<Row> = stats
            .iter()
            .map(|s| {
                let (msgs, bytes, msg_rate, gbps) = s.interval_metrics(elapsed);
                Row::new([
                    Cell::from(s.id.to_string()),
                    Cell::from(s.qpn.to_string()),
                    Cell::from(msgs.to_string()),
                    Cell::from(format!("{:.1}", msg_rate)),
                    Cell::from(format!("{:.2}", gbps)),
                ])
            })
            .collect();

        let table = Table::new(
            rows,
            [
                Constraint::Min(8),  // Stream
                Constraint::Min(8),  // QPN
                Constraint::Min(12), // Messages
                Constraint::Min(12), // Bytes
                Constraint::Min(10), // Gbit/s
            ],
        )
        .header(header)
        .block(Block::bordered().title("Open QPs".bold()));

        frame.render_widget(table, top);

        // Render logs

        let log_lines: Vec<Line> = logs.iter().map(|s| Line::from(s.as_str())).collect();
        let visible_height = bottom.height.saturating_sub(2);
        let scroll = (log_lines.len() as u16).saturating_sub(visible_height);

        let log_panel = Paragraph::new(log_lines)
            .block(Block::bordered().title("Logs".bold()))
            .scroll((scroll, 0));

        frame.render_widget(log_panel, bottom);
    }

    pub fn restore(&mut self) -> Result<(), Box<dyn Error>> {
        ratatui::crossterm::execute!(stdout(), LeaveAlternateScreen)?;
        disable_raw_mode()?;

        Ok(())
    }
}
