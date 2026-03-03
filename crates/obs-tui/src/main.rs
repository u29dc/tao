use std::io::{self, Stdout};
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    Placeholder,
    Notes,
    Search,
    Bases,
}

impl Route {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Placeholder => "placeholder",
            Self::Notes => "notes",
            Self::Search => "search",
            Self::Bases => "bases",
        }
    }

    const fn help_text(self) -> &'static str {
        match self {
            Self::Placeholder => {
                "Placeholder route active. Use keymap or command palette to switch routes."
            }
            Self::Notes => {
                "Notes route shell active. Note list/view integration arrives in TUI-003."
            }
            Self::Search => "Search route shell active. Search integration arrives in TUI-004.",
            Self::Bases => "Bases route shell active. Table integration arrives in TUI-005.",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AppState {
    route: Route,
    status: String,
    palette_open: bool,
    palette_input: String,
    should_quit: bool,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            route: Route::Placeholder,
            status: "ready".to_string(),
            palette_open: false,
            palette_input: String::new(),
            should_quit: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PaletteCommand {
    SwitchRoute(Route),
    Quit,
}

impl AppState {
    fn switch_route(&mut self, route: Route) {
        self.route = route;
        self.status = format!("route switched to {}", route.as_str());
    }

    fn open_palette(&mut self) {
        self.palette_open = true;
        self.palette_input.clear();
        self.status = "command palette opened".to_string();
    }

    fn close_palette(&mut self) {
        self.palette_open = false;
        self.palette_input.clear();
    }

    fn handle_key(&mut self, key: KeyEvent) {
        if self.palette_open {
            self.handle_palette_key(key);
            return;
        }

        match key.code {
            KeyCode::Char('q') => {
                self.should_quit = true;
                self.status = "quit requested".to_string();
            }
            KeyCode::Char('1') => self.switch_route(Route::Placeholder),
            KeyCode::Char('2') => self.switch_route(Route::Notes),
            KeyCode::Char('3') => self.switch_route(Route::Search),
            KeyCode::Char('4') => self.switch_route(Route::Bases),
            KeyCode::Char(':') => self.open_palette(),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.should_quit = true;
                self.status = "quit requested (ctrl-c)".to_string();
            }
            _ => {}
        }
    }

    fn handle_palette_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.close_palette();
                self.status = "command palette closed".to_string();
            }
            KeyCode::Backspace => {
                self.palette_input.pop();
            }
            KeyCode::Enter => {
                let raw = self.palette_input.trim().to_string();
                match parse_palette_command(&raw) {
                    Ok(PaletteCommand::SwitchRoute(route)) => {
                        self.switch_route(route);
                    }
                    Ok(PaletteCommand::Quit) => {
                        self.should_quit = true;
                        self.status = "quit requested from palette".to_string();
                    }
                    Err(message) => {
                        self.status = format!("palette error: {message}");
                    }
                }
                self.close_palette();
            }
            KeyCode::Char(ch) => {
                self.palette_input.push(ch);
            }
            _ => {}
        }
    }
}

fn parse_palette_command(input: &str) -> std::result::Result<PaletteCommand, String> {
    let normalized = input.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err("empty command".to_string());
    }

    if normalized == "quit" || normalized == "q" {
        return Ok(PaletteCommand::Quit);
    }

    if let Some(route_name) = normalized.strip_prefix("route ").map(str::trim) {
        let route = match route_name {
            "placeholder" => Route::Placeholder,
            "notes" => Route::Notes,
            "search" => Route::Search,
            "bases" => Route::Bases,
            _ => {
                return Err(format!("unknown route '{route_name}'"));
            }
        };
        return Ok(PaletteCommand::SwitchRoute(route));
    }

    Err(format!("unsupported command '{normalized}'"))
}

fn render(frame: &mut ratatui::Frame<'_>, app: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(3),
            Constraint::Length(3),
        ])
        .split(frame.area());

    let header = Paragraph::new(Line::from(format!(
        "obs-tui | route={} | keys: 1 placeholder, 2 notes, 3 search, 4 bases, : palette, q quit",
        app.route.as_str()
    )))
    .block(Block::default().borders(Borders::ALL).title("Route Shell"));
    frame.render_widget(header, chunks[0]);

    let body = Paragraph::new(app.route.help_text())
        .block(Block::default().borders(Borders::ALL).title("Route"))
        .wrap(ratatui::widgets::Wrap { trim: true });
    frame.render_widget(body, chunks[1]);

    let footer = Paragraph::new(app.status.as_str())
        .block(Block::default().borders(Borders::ALL).title("Status"));
    frame.render_widget(footer, chunks[2]);

    if app.palette_open {
        let popup = centered_rect(80, 20, frame.area());
        frame.render_widget(Clear, popup);
        let palette = Paragraph::new(format!(":{}", app.palette_input)).block(
            Block::default()
                .borders(Borders::ALL)
                .title("Command Palette"),
        );
        frame.render_widget(palette, popup);
    }
}

fn centered_rect(horizontal_percent: u16, vertical_percent: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - vertical_percent) / 2),
            Constraint::Percentage(vertical_percent),
            Constraint::Percentage((100 - vertical_percent) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - horizontal_percent) / 2),
            Constraint::Percentage(horizontal_percent),
            Constraint::Percentage((100 - horizontal_percent) / 2),
        ])
        .split(vertical[1])[1]
}

fn init_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let terminal = Terminal::new(backend)?;
    Ok(terminal)
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

fn run(mut terminal: Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
    let mut app = AppState::default();

    loop {
        terminal.draw(|frame| render(frame, &app))?;
        if app.should_quit {
            break;
        }

        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            app.handle_key(key);
        }
    }

    restore_terminal(&mut terminal)?;
    Ok(())
}

fn main() -> Result<()> {
    let terminal = init_terminal()?;
    if let Err(source) = run(terminal) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        return Err(source);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        AppState, KeyCode, KeyEvent, KeyModifiers, PaletteCommand, Route, parse_palette_command,
    };

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[test]
    fn default_route_is_placeholder() {
        let app = AppState::default();
        assert_eq!(app.route, Route::Placeholder);
        assert_eq!(app.route.as_str(), "placeholder");
    }

    #[test]
    fn keymap_switches_routes() {
        let mut app = AppState::default();
        app.handle_key(key(KeyCode::Char('2')));
        assert_eq!(app.route, Route::Notes);
        app.handle_key(key(KeyCode::Char('3')));
        assert_eq!(app.route, Route::Search);
        app.handle_key(key(KeyCode::Char('4')));
        assert_eq!(app.route, Route::Bases);
        app.handle_key(key(KeyCode::Char('1')));
        assert_eq!(app.route, Route::Placeholder);
    }

    #[test]
    fn palette_command_parser_supports_route_switch_and_quit() {
        assert_eq!(
            parse_palette_command("route notes").expect("route notes command"),
            PaletteCommand::SwitchRoute(Route::Notes)
        );
        assert_eq!(
            parse_palette_command("q").expect("quit command"),
            PaletteCommand::Quit
        );
    }

    #[test]
    fn palette_command_parser_rejects_invalid_commands() {
        assert!(parse_palette_command("").is_err());
        assert!(parse_palette_command("route unknown").is_err());
        assert!(parse_palette_command("help").is_err());
    }

    #[test]
    fn palette_route_switch_flow_updates_route() {
        let mut app = AppState::default();
        app.handle_key(key(KeyCode::Char(':')));
        for ch in "route search".chars() {
            app.handle_key(key(KeyCode::Char(ch)));
        }
        app.handle_key(key(KeyCode::Enter));
        assert_eq!(app.route, Route::Search);
        assert!(!app.palette_open);
    }
}
