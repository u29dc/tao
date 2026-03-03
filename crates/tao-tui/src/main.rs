use std::io::{self, Stdout};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{
    Block, Borders, Cell, Clear, List, ListItem, ListState, Paragraph, Row, Table, Wrap,
};
use tao_sdk_bridge::{
    BridgeBaseRef, BridgeBaseTablePage, BridgeEnvelope, BridgeKernel, BridgeNoteSummary,
    BridgeNoteView,
};

#[derive(Debug, Parser)]
#[command(name = "tao-tui", version, about = "tao terminal ui")]
struct CliArgs {
    /// Absolute vault root path for SDK-backed routes.
    #[arg(long)]
    vault_root: Option<PathBuf>,
    /// SQLite database path for SDK-backed routes.
    #[arg(long)]
    db_path: Option<PathBuf>,
}

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
                "Notes route active. Use up/down (or j/k) to select and enter to reload."
            }
            Self::Search => "Search route shell active. Search integration arrives in TUI-004.",
            Self::Bases => "Bases route shell active. Table integration arrives in TUI-005.",
        }
    }
}

#[derive(Debug)]
struct AppContext {
    bridge: Option<BridgeKernel>,
    startup_status: Option<String>,
}

impl AppContext {
    fn from_args(args: &CliArgs) -> Self {
        match (&args.vault_root, &args.db_path) {
            (Some(vault_root), Some(db_path)) => match BridgeKernel::open(vault_root, db_path) {
                Ok(bridge) => Self {
                    bridge: Some(bridge),
                    startup_status: Some(format!("bridge ready ({})", vault_root.display())),
                },
                Err(source) => Self {
                    bridge: None,
                    startup_status: Some(format!("bridge init failed: {source}")),
                },
            },
            (None, None) => Self {
                bridge: None,
                startup_status: Some(
                    "bridge disabled: pass --vault-root and --db-path for notes/search/bases"
                        .to_string(),
                ),
            },
            _ => Self {
                bridge: None,
                startup_status: Some(
                    "bridge disabled: provide both --vault-root and --db-path".to_string(),
                ),
            },
        }
    }

    #[cfg(test)]
    fn with_bridge(bridge: BridgeKernel) -> Self {
        Self {
            bridge: Some(bridge),
            startup_status: None,
        }
    }

    fn startup_status(&self) -> Option<&str> {
        self.startup_status.as_deref()
    }

    fn load_notes(&self) -> std::result::Result<Vec<BridgeNoteSummary>, String> {
        let bridge = self.bridge.as_ref().ok_or_else(|| {
            "notes route unavailable without --vault-root and --db-path".to_string()
        })?;

        let mut after_path: Option<String> = None;
        let mut items = Vec::new();
        loop {
            let page =
                expect_bridge_value(bridge.notes_list(after_path.as_deref(), 500), "notes.list")?;
            after_path = page.next_cursor;
            items.extend(page.items);
            if after_path.is_none() {
                break;
            }
        }
        Ok(items)
    }

    fn load_note_view(&self, normalized_path: &str) -> std::result::Result<BridgeNoteView, String> {
        let bridge = self.bridge.as_ref().ok_or_else(|| {
            "note view unavailable without --vault-root and --db-path".to_string()
        })?;
        expect_bridge_value(bridge.note_get(normalized_path), "note.get")
    }

    fn search_notes(&self, query: &str) -> std::result::Result<Vec<BridgeNoteSummary>, String> {
        let query = query.trim();
        if query.is_empty() {
            return Ok(Vec::new());
        }

        let query_lc = query.to_ascii_lowercase();
        let mut matches = self
            .load_notes()?
            .into_iter()
            .filter(|note| {
                note.path.to_ascii_lowercase().contains(&query_lc)
                    || note.title.to_ascii_lowercase().contains(&query_lc)
            })
            .collect::<Vec<_>>();
        matches.sort_unstable_by(|left, right| left.path.cmp(&right.path));
        Ok(matches)
    }

    fn list_bases(&self) -> std::result::Result<Vec<BridgeBaseRef>, String> {
        let bridge = self.bridge.as_ref().ok_or_else(|| {
            "bases route unavailable without --vault-root and --db-path".to_string()
        })?;
        let mut bases = expect_bridge_value(bridge.bases_list(), "bases.list")?;
        bases.sort_unstable_by(|left, right| left.file_path.cmp(&right.file_path));
        Ok(bases)
    }

    fn load_base_view(
        &self,
        path_or_id: &str,
        view_name: &str,
        page: u32,
        page_size: u32,
    ) -> std::result::Result<BridgeBaseTablePage, String> {
        let bridge = self.bridge.as_ref().ok_or_else(|| {
            "bases route unavailable without --vault-root and --db-path".to_string()
        })?;
        expect_bridge_value(
            bridge.bases_view(path_or_id, view_name, page, page_size),
            "bases.view",
        )
    }
}

fn expect_bridge_value<T>(
    envelope: BridgeEnvelope<T>,
    action: &str,
) -> std::result::Result<T, String> {
    if envelope.ok {
        return envelope
            .value
            .ok_or_else(|| format!("{action} returned success without payload"));
    }

    match envelope.error {
        Some(error) => {
            let mut message = format!("{action} failed [{}]: {}", error.code, error.message);
            if let Some(hint) = error.hint {
                message.push_str(&format!("; hint: {hint}"));
            }
            Err(message)
        }
        None => Err(format!("{action} failed without an error payload")),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AppState {
    route: Route,
    status: String,
    palette_open: bool,
    palette_input: String,
    should_quit: bool,
    notes: Vec<BridgeNoteSummary>,
    selected_note_index: usize,
    selected_note_view: Option<BridgeNoteView>,
    search_query: String,
    search_input_mode: bool,
    search_results: Vec<BridgeNoteSummary>,
    selected_search_index: usize,
    bases: Vec<BridgeBaseRef>,
    selected_base_index: usize,
    selected_view_index: usize,
    base_page_number: u32,
    base_page_size: u32,
    base_table_page: Option<BridgeBaseTablePage>,
    base_sort_mode: BaseSortMode,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            route: Route::Placeholder,
            status: "ready".to_string(),
            palette_open: false,
            palette_input: String::new(),
            should_quit: false,
            notes: Vec::new(),
            selected_note_index: 0,
            selected_note_view: None,
            search_query: String::new(),
            search_input_mode: false,
            search_results: Vec::new(),
            selected_search_index: 0,
            bases: Vec::new(),
            selected_base_index: 0,
            selected_view_index: 0,
            base_page_number: 1,
            base_page_size: 25,
            base_table_page: None,
            base_sort_mode: BaseSortMode::PathAsc,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PaletteCommand {
    SwitchRoute(Route),
    Quit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BaseSortMode {
    PathAsc,
    PathDesc,
}

impl BaseSortMode {
    fn toggle(self) -> Self {
        match self {
            Self::PathAsc => Self::PathDesc,
            Self::PathDesc => Self::PathAsc,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::PathAsc => "path asc",
            Self::PathDesc => "path desc",
        }
    }
}

impl AppState {
    fn apply_startup_status(&mut self, context: &AppContext) {
        if let Some(status) = context.startup_status() {
            self.status = status.to_string();
        }
    }

    fn switch_route(&mut self, route: Route, context: &AppContext) {
        self.route = route;
        self.search_input_mode = false;
        self.status = format!("route switched to {}", route.as_str());
        match route {
            Route::Notes => self.refresh_notes(context),
            Route::Search => self.refresh_search_results(context),
            Route::Bases => self.refresh_bases(context),
            Route::Placeholder => {}
        }
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

    fn handle_key(&mut self, key: KeyEvent, context: &AppContext) {
        if self.palette_open {
            self.handle_palette_key(key, context);
            return;
        }

        match key.code {
            KeyCode::Char('q') => {
                self.should_quit = true;
                self.status = "quit requested".to_string();
            }
            KeyCode::Char('1') => self.switch_route(Route::Placeholder, context),
            KeyCode::Char('2') => self.switch_route(Route::Notes, context),
            KeyCode::Char('3') => self.switch_route(Route::Search, context),
            KeyCode::Char('4') => self.switch_route(Route::Bases, context),
            KeyCode::Char(':') => self.open_palette(),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.should_quit = true;
                self.status = "quit requested (ctrl-c)".to_string();
            }
            _ => self.handle_route_key(key, context),
        }
    }

    fn handle_route_key(&mut self, key: KeyEvent, context: &AppContext) {
        match self.route {
            Route::Notes => match key.code {
                KeyCode::Up | KeyCode::Char('k') => self.move_note_selection(-1, context),
                KeyCode::Down | KeyCode::Char('j') => self.move_note_selection(1, context),
                KeyCode::Enter => self.reload_selected_note_view(context),
                KeyCode::Char('r') => self.refresh_notes(context),
                _ => {}
            },
            Route::Search => self.handle_search_key(key, context),
            Route::Bases => self.handle_bases_key(key, context),
            Route::Placeholder => {}
        }
    }

    fn handle_palette_key(&mut self, key: KeyEvent, context: &AppContext) {
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
                        self.switch_route(route, context);
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

    fn refresh_notes(&mut self, context: &AppContext) {
        match context.load_notes() {
            Ok(notes) => {
                self.notes = notes;
                if self.notes.is_empty() {
                    self.selected_note_index = 0;
                    self.selected_note_view = None;
                    self.status = "notes route active: no indexed markdown notes".to_string();
                    return;
                }

                if self.selected_note_index >= self.notes.len() {
                    self.selected_note_index = 0;
                }
                self.reload_selected_note_view(context);
                self.status = format!("notes loaded: {}", self.notes.len());
            }
            Err(message) => {
                self.notes.clear();
                self.selected_note_index = 0;
                self.selected_note_view = None;
                self.status = format!("notes load error: {message}");
            }
        }
    }

    fn reload_selected_note_view(&mut self, context: &AppContext) {
        let Some(path) = self.selected_note_path() else {
            self.selected_note_view = None;
            return;
        };

        match context.load_note_view(path) {
            Ok(view) => {
                self.selected_note_view = Some(view);
            }
            Err(message) => {
                self.selected_note_view = None;
                self.status = format!("note load error: {message}");
            }
        }
    }

    fn selected_note_path(&self) -> Option<&str> {
        self.notes
            .get(self.selected_note_index)
            .map(|note| note.path.as_str())
    }

    fn move_note_selection(&mut self, delta: i32, context: &AppContext) {
        if self.notes.is_empty() {
            return;
        }
        let max_index = i32::try_from(self.notes.len().saturating_sub(1)).unwrap_or(i32::MAX);
        let current = i32::try_from(self.selected_note_index).unwrap_or(0);
        let next = (current + delta).clamp(0, max_index);
        let next_index = usize::try_from(next).unwrap_or(0);
        if next_index != self.selected_note_index {
            self.selected_note_index = next_index;
            self.reload_selected_note_view(context);
        }
    }

    fn handle_search_key(&mut self, key: KeyEvent, context: &AppContext) {
        if self.search_input_mode {
            self.handle_search_input_key(key, context);
            return;
        }

        match key.code {
            KeyCode::Char('/') => {
                self.search_input_mode = true;
                self.status = "search input mode".to_string();
            }
            KeyCode::Char('r') => self.refresh_search_results(context),
            KeyCode::Up | KeyCode::Char('k') => self.move_search_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_search_selection(1),
            KeyCode::Enter => self.open_selected_search_result(context),
            _ => {}
        }
    }

    fn handle_search_input_key(&mut self, key: KeyEvent, context: &AppContext) {
        match key.code {
            KeyCode::Esc => {
                self.search_input_mode = false;
                self.status = "search input cancelled".to_string();
            }
            KeyCode::Backspace => {
                self.search_query.pop();
            }
            KeyCode::Enter => {
                self.search_input_mode = false;
                self.refresh_search_results(context);
            }
            KeyCode::Char(ch) => {
                self.search_query.push(ch);
            }
            _ => {}
        }
    }

    fn refresh_search_results(&mut self, context: &AppContext) {
        if self.search_query.trim().is_empty() {
            self.search_results.clear();
            self.selected_search_index = 0;
            self.status = "search query empty: press / to type query".to_string();
            return;
        }

        match context.search_notes(&self.search_query) {
            Ok(results) => {
                self.search_results = results;
                self.selected_search_index = 0;
                self.status = format!(
                    "search results: {} for '{}'",
                    self.search_results.len(),
                    self.search_query
                );
            }
            Err(message) => {
                self.search_results.clear();
                self.selected_search_index = 0;
                self.status = format!("search error: {message}");
            }
        }
    }

    fn move_search_selection(&mut self, delta: i32) {
        if self.search_results.is_empty() {
            return;
        }
        let max_index =
            i32::try_from(self.search_results.len().saturating_sub(1)).unwrap_or(i32::MAX);
        let current = i32::try_from(self.selected_search_index).unwrap_or(0);
        let next = (current + delta).clamp(0, max_index);
        self.selected_search_index = usize::try_from(next).unwrap_or(0);
    }

    fn open_selected_search_result(&mut self, context: &AppContext) {
        let Some(target) = self
            .search_results
            .get(self.selected_search_index)
            .map(|item| item.path.clone())
        else {
            self.status = "no search result selected".to_string();
            return;
        };

        self.switch_route(Route::Notes, context);
        if self.select_note_path(&target, context) {
            self.status = format!("opened note from search: {target}");
        } else {
            self.status = format!("search result not found in notes route: {target}");
        }
    }

    fn select_note_path(&mut self, path: &str, context: &AppContext) -> bool {
        let Some(index) = self.notes.iter().position(|item| item.path == path) else {
            return false;
        };
        self.selected_note_index = index;
        self.reload_selected_note_view(context);
        true
    }

    fn handle_bases_key(&mut self, key: KeyEvent, context: &AppContext) {
        match key.code {
            KeyCode::Char('r') => self.refresh_bases(context),
            KeyCode::Char('n') => self.next_base_page(context),
            KeyCode::Char('p') => self.previous_base_page(context),
            KeyCode::Char('s') => self.toggle_base_sort(),
            KeyCode::Char('[') => self.previous_base(context),
            KeyCode::Char(']') => self.next_base(context),
            KeyCode::Char('v') => self.next_base_view(context),
            _ => {}
        }
    }

    fn refresh_bases(&mut self, context: &AppContext) {
        match context.list_bases() {
            Ok(bases) => {
                self.bases = bases;
                self.selected_base_index = 0;
                self.selected_view_index = 0;
                self.base_page_number = 1;
                self.base_table_page = None;
                if self.bases.is_empty() {
                    self.status = "bases route active: no indexed bases found".to_string();
                    return;
                }
                self.load_selected_base_table(context);
            }
            Err(message) => {
                self.bases.clear();
                self.base_table_page = None;
                self.status = format!("bases load error: {message}");
            }
        }
    }

    fn current_base(&self) -> Option<&BridgeBaseRef> {
        self.bases.get(self.selected_base_index)
    }

    fn current_view_name(&self) -> Option<&str> {
        let base = self.current_base()?;
        base.views
            .get(self.selected_view_index)
            .map(std::string::String::as_str)
    }

    fn load_selected_base_table(&mut self, context: &AppContext) {
        let (base_id, view_name) = match (self.current_base(), self.current_view_name()) {
            (Some(base), Some(view_name)) => (base.base_id.clone(), view_name.to_string()),
            (Some(_), None) => {
                self.base_table_page = None;
                self.status = "selected base has no views".to_string();
                return;
            }
            (None, _) => {
                self.base_table_page = None;
                self.status = "no base selected".to_string();
                return;
            }
        };

        match context.load_base_view(
            &base_id,
            &view_name,
            self.base_page_number,
            self.base_page_size,
        ) {
            Ok(mut page) => {
                apply_base_sort_mode(&mut page, self.base_sort_mode);
                self.base_table_page = Some(page);
                self.status = format!(
                    "bases view loaded: base={} view={} page={}",
                    base_id, view_name, self.base_page_number
                );
            }
            Err(message) => {
                self.base_table_page = None;
                self.status = format!("bases view error: {message}");
            }
        }
    }

    fn next_base_page(&mut self, context: &AppContext) {
        if let Some(page) = &self.base_table_page
            && !page.has_more
        {
            return;
        }
        self.base_page_number = self.base_page_number.saturating_add(1);
        self.load_selected_base_table(context);
    }

    fn previous_base_page(&mut self, context: &AppContext) {
        if self.base_page_number <= 1 {
            return;
        }
        self.base_page_number = self.base_page_number.saturating_sub(1);
        self.load_selected_base_table(context);
    }

    fn previous_base(&mut self, context: &AppContext) {
        if self.bases.is_empty() || self.selected_base_index == 0 {
            return;
        }
        self.selected_base_index = self.selected_base_index.saturating_sub(1);
        self.selected_view_index = 0;
        self.base_page_number = 1;
        self.load_selected_base_table(context);
    }

    fn next_base(&mut self, context: &AppContext) {
        if self.bases.is_empty() || self.selected_base_index + 1 >= self.bases.len() {
            return;
        }
        self.selected_base_index += 1;
        self.selected_view_index = 0;
        self.base_page_number = 1;
        self.load_selected_base_table(context);
    }

    fn next_base_view(&mut self, context: &AppContext) {
        let Some(base) = self.current_base() else {
            return;
        };
        if base.views.is_empty() {
            return;
        }
        self.selected_view_index = (self.selected_view_index + 1) % base.views.len();
        self.base_page_number = 1;
        self.load_selected_base_table(context);
    }

    fn toggle_base_sort(&mut self) {
        self.base_sort_mode = self.base_sort_mode.toggle();
        if let Some(page) = &mut self.base_table_page {
            apply_base_sort_mode(page, self.base_sort_mode);
        }
        self.status = format!("bases sort mode: {}", self.base_sort_mode.as_str());
    }
}

fn apply_base_sort_mode(page: &mut BridgeBaseTablePage, mode: BaseSortMode) {
    page.rows
        .sort_unstable_by(|left, right| left.file_path.cmp(&right.file_path));
    if mode == BaseSortMode::PathDesc {
        page.rows.reverse();
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
        "tao-tui | route={} | keys: 1 placeholder, 2 notes, 3 search, 4 bases, : palette, q quit",
        app.route.as_str()
    )))
    .block(Block::default().borders(Borders::ALL).title("Route Shell"));
    frame.render_widget(header, chunks[0]);

    render_route_body(frame, chunks[1], app);

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

fn render_route_body(frame: &mut ratatui::Frame<'_>, area: Rect, app: &AppState) {
    match app.route {
        Route::Notes => render_notes_route(frame, area, app),
        Route::Search => render_search_route(frame, area, app),
        Route::Bases => render_bases_route(frame, area, app),
        _ => {
            let body = Paragraph::new(app.route.help_text())
                .block(Block::default().borders(Borders::ALL).title("Route"))
                .wrap(Wrap { trim: true });
            frame.render_widget(body, area);
        }
    }
}

fn render_notes_route(frame: &mut ratatui::Frame<'_>, area: Rect, app: &AppState) {
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(area);

    let list_items = app
        .notes
        .iter()
        .map(|note| {
            let updated = note.updated_at.as_deref().unwrap_or("unknown");
            ListItem::new(Line::from(format!("{}  [{updated}]", note.path)))
        })
        .collect::<Vec<_>>();

    let list_title = format!("Notes ({})", app.notes.len());
    let list = List::new(list_items)
        .block(Block::default().borders(Borders::ALL).title(list_title))
        .highlight_symbol("> ")
        .highlight_style(Style::default().add_modifier(Modifier::BOLD));
    let mut list_state = ListState::default();
    if !app.notes.is_empty() {
        list_state.select(Some(app.selected_note_index));
    }
    frame.render_stateful_widget(list, columns[0], &mut list_state);

    let note_text = if let Some(view) = &app.selected_note_view {
        format!(
            "path: {}\ntitle: {}\nheadings: {}\n\n{}",
            view.path, view.title, view.headings_total, view.body
        )
    } else if app.notes.is_empty() {
        "No indexed notes available. Run vault reindex and reopen notes route.".to_string()
    } else {
        "No note selected.".to_string()
    };

    let viewer = Paragraph::new(note_text)
        .block(Block::default().borders(Borders::ALL).title("Viewer"))
        .wrap(Wrap { trim: false });
    frame.render_widget(viewer, columns[1]);
}

fn render_search_route(frame: &mut ratatui::Frame<'_>, area: Rect, app: &AppState) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(3)])
        .split(area);

    let mode = if app.search_input_mode {
        "input"
    } else {
        "navigate"
    };
    let query = Paragraph::new(format!(
        "query: {} | mode: {} | keys: / enter esc, up/down, enter=open",
        app.search_query, mode
    ))
    .block(Block::default().borders(Borders::ALL).title("Search"));
    frame.render_widget(query, rows[0]);

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(rows[1]);

    let items = app
        .search_results
        .iter()
        .map(|item| ListItem::new(Line::from(format!("{} ({})", item.path, item.title))))
        .collect::<Vec<_>>();
    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!("Results ({})", app.search_results.len())),
        )
        .highlight_symbol("> ")
        .highlight_style(Style::default().add_modifier(Modifier::BOLD));
    let mut list_state = ListState::default();
    if !app.search_results.is_empty() {
        list_state.select(Some(app.selected_search_index));
    }
    frame.render_stateful_widget(list, columns[0], &mut list_state);

    let preview = if let Some(selected) = app.search_results.get(app.selected_search_index) {
        format!(
            "path: {}\ntitle: {}\nupdated_at: {}",
            selected.path,
            selected.title,
            selected.updated_at.as_deref().unwrap_or("unknown")
        )
    } else {
        "No results loaded. Press /, type a query, then Enter.".to_string()
    };
    let details = Paragraph::new(preview)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Selected Result"),
        )
        .wrap(Wrap { trim: true });
    frame.render_widget(details, columns[1]);
}

fn render_bases_route(frame: &mut ratatui::Frame<'_>, area: Rect, app: &AppState) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(3)])
        .split(area);

    let selected_base = app.current_base();
    let selected_view = app.current_view_name().unwrap_or("n/a");
    let header = Paragraph::new(format!(
        "base={} | view={} | page={} | sort={} | keys: [ ] base, v view, n/p page, s sort, r refresh",
        selected_base
            .map(|base| base.file_path.as_str())
            .unwrap_or("n/a"),
        selected_view,
        app.base_page_number,
        app.base_sort_mode.as_str()
    ))
    .block(Block::default().borders(Borders::ALL).title("Bases"));
    frame.render_widget(header, rows[0]);

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(32), Constraint::Percentage(68)])
        .split(rows[1]);

    let base_items = app
        .bases
        .iter()
        .map(|base| ListItem::new(Line::from(base.file_path.clone())))
        .collect::<Vec<_>>();
    let base_list = List::new(base_items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!("Base Files ({})", app.bases.len())),
        )
        .highlight_symbol("> ")
        .highlight_style(Style::default().add_modifier(Modifier::BOLD));
    let mut base_state = ListState::default();
    if !app.bases.is_empty() {
        base_state.select(Some(app.selected_base_index));
    }
    frame.render_stateful_widget(base_list, columns[0], &mut base_state);

    if let Some(page) = &app.base_table_page {
        let table_rows = page
            .rows
            .iter()
            .map(|row| {
                let mut cells = Vec::new();
                cells.push(Cell::from(row.file_path.clone()));
                for column in &page.columns {
                    let value = row.values.get(&column.key).cloned().unwrap_or_default();
                    cells.push(Cell::from(value));
                }
                Row::new(cells)
            })
            .collect::<Vec<_>>();

        let mut header_cells = Vec::new();
        header_cells.push(Cell::from("file"));
        for column in &page.columns {
            header_cells.push(Cell::from(
                column.label.clone().unwrap_or_else(|| column.key.clone()),
            ));
        }
        let widths = vec![Constraint::Min(18); header_cells.len().max(1)];
        let table = Table::new(table_rows, widths)
            .header(Row::new(header_cells).style(Style::default().add_modifier(Modifier::BOLD)))
            .block(Block::default().borders(Borders::ALL).title(format!(
                "Rows total={} has_more={}",
                page.total, page.has_more
            )))
            .column_spacing(1);
        frame.render_widget(table, columns[1]);
    } else {
        let empty = Paragraph::new(
            "No base table page loaded. Ensure at least one .base file is indexed and has table views.",
        )
        .block(Block::default().borders(Borders::ALL).title("Rows"))
        .wrap(Wrap { trim: true });
        frame.render_widget(empty, columns[1]);
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

fn run(mut terminal: Terminal<CrosstermBackend<Stdout>>, context: AppContext) -> Result<()> {
    let mut app = AppState::default();
    app.apply_startup_status(&context);

    loop {
        terminal.draw(|frame| render(frame, &app))?;
        if app.should_quit {
            break;
        }

        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            app.handle_key(key, &context);
        }
    }

    restore_terminal(&mut terminal)?;
    Ok(())
}

fn main() -> Result<()> {
    let args = CliArgs::parse();
    let context = AppContext::from_args(&args);
    let terminal = init_terminal()?;
    if let Err(source) = run(terminal, context) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        return Err(source);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tao_sdk_bridge::{BridgeBaseColumn, BridgeBaseTableRow};

    use super::{
        AppContext, AppState, BaseSortMode, BridgeBaseTablePage, BridgeEnvelope, BridgeKernel,
        KeyCode, KeyEvent, KeyModifiers, PaletteCommand, Route, apply_base_sort_mode,
        parse_palette_command,
    };

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn assert_bridge_ok<T>(envelope: BridgeEnvelope<T>) -> T {
        assert!(
            envelope.ok,
            "bridge envelope expected ok=true, got error: {:?}",
            envelope.error
        );
        envelope
            .value
            .expect("bridge envelope should contain value when ok=true")
    }

    #[test]
    fn default_route_is_placeholder() {
        let app = AppState::default();
        assert_eq!(app.route, Route::Placeholder);
        assert_eq!(app.route.as_str(), "placeholder");
    }

    #[test]
    fn keymap_switches_routes() {
        let context = AppContext::from_args(&super::CliArgs {
            vault_root: None,
            db_path: None,
        });
        let mut app = AppState::default();
        app.handle_key(key(KeyCode::Char('2')), &context);
        assert_eq!(app.route, Route::Notes);
        app.handle_key(key(KeyCode::Char('3')), &context);
        assert_eq!(app.route, Route::Search);
        app.handle_key(key(KeyCode::Char('4')), &context);
        assert_eq!(app.route, Route::Bases);
        app.handle_key(key(KeyCode::Char('1')), &context);
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
        let context = AppContext::from_args(&super::CliArgs {
            vault_root: None,
            db_path: None,
        });
        let mut app = AppState::default();
        app.handle_key(key(KeyCode::Char(':')), &context);
        for ch in "route search".chars() {
            app.handle_key(key(KeyCode::Char(ch)), &context);
        }
        app.handle_key(key(KeyCode::Enter), &context);
        assert_eq!(app.route, Route::Search);
        assert!(!app.palette_open);
    }

    #[test]
    fn notes_route_loads_note_list_and_view_content() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        let db_path = tempdir.path().join("obs.sqlite");
        fs::create_dir_all(&vault_root).expect("create vault root");

        let mut setup_kernel = BridgeKernel::open(&vault_root, &db_path).expect("open bridge");
        let _ = assert_bridge_ok(setup_kernel.note_put("notes/alpha.md", "# Alpha\n\nBody A"));
        let _ = assert_bridge_ok(setup_kernel.note_put("notes/beta.md", "# Beta\n\nBody B"));

        let context = AppContext::with_bridge(setup_kernel);
        let mut app = AppState::default();
        app.switch_route(Route::Notes, &context);

        assert_eq!(app.route, Route::Notes);
        assert_eq!(app.notes.len(), 2);
        assert_eq!(app.notes[0].path, "notes/alpha.md");
        let view = app
            .selected_note_view
            .as_ref()
            .expect("selected note view should be loaded");
        assert_eq!(view.path, "notes/alpha.md");
        assert!(view.body.contains("Body A"));

        app.handle_key(key(KeyCode::Down), &context);
        assert_eq!(app.selected_note_index, 1);
        let second = app
            .selected_note_view
            .as_ref()
            .expect("second selected note view should be loaded");
        assert_eq!(second.path, "notes/beta.md");
        assert!(second.body.contains("Body B"));
    }

    #[test]
    fn search_route_filters_results_and_opens_selected_note() {
        let tempdir = tempfile::tempdir().expect("create tempdir");
        let vault_root = tempdir.path().join("vault");
        let db_path = tempdir.path().join("obs.sqlite");
        fs::create_dir_all(&vault_root).expect("create vault root");

        let mut setup_kernel = BridgeKernel::open(&vault_root, &db_path).expect("open bridge");
        let _ = assert_bridge_ok(setup_kernel.note_put("notes/alpha.md", "# Alpha\n\nBody A"));
        let _ = assert_bridge_ok(setup_kernel.note_put("notes/beta.md", "# Beta\n\nBody B"));
        let _ = assert_bridge_ok(setup_kernel.note_put("notes/project-beta.md", "# Project Beta"));

        let context = AppContext::with_bridge(setup_kernel);
        let mut app = AppState::default();
        app.switch_route(Route::Search, &context);
        app.handle_key(key(KeyCode::Char('/')), &context);
        for ch in "beta".chars() {
            app.handle_key(key(KeyCode::Char(ch)), &context);
        }
        app.handle_key(key(KeyCode::Enter), &context);

        assert_eq!(app.route, Route::Search);
        assert_eq!(app.search_query, "beta");
        assert_eq!(app.search_results.len(), 2);
        assert_eq!(app.search_results[0].path, "notes/beta.md");

        app.handle_key(key(KeyCode::Enter), &context);
        assert_eq!(app.route, Route::Notes);
        let selected = app
            .selected_note_view
            .as_ref()
            .expect("selected note should open from search");
        assert_eq!(selected.path, "notes/beta.md");
        assert!(selected.body.contains("Body B"));
    }

    fn sample_base_page(has_more: bool) -> BridgeBaseTablePage {
        BridgeBaseTablePage {
            base_id: "b_projects".to_string(),
            file_path: "views/projects.base".to_string(),
            view_name: "Projects".to_string(),
            page: 1,
            page_size: 25,
            total: 2,
            has_more,
            columns: vec![BridgeBaseColumn {
                key: "status".to_string(),
                label: Some("status".to_string()),
                hidden: false,
                width: None,
            }],
            rows: vec![
                BridgeBaseTableRow {
                    file_id: "f_b".to_string(),
                    file_path: "notes/beta.md".to_string(),
                    values: [("status".to_string(), "paused".to_string())]
                        .into_iter()
                        .collect(),
                },
                BridgeBaseTableRow {
                    file_id: "f_a".to_string(),
                    file_path: "notes/alpha.md".to_string(),
                    values: [("status".to_string(), "active".to_string())]
                        .into_iter()
                        .collect(),
                },
            ],
        }
    }

    #[test]
    fn base_sort_mode_reorders_rows_by_file_path() {
        let mut page = sample_base_page(false);
        apply_base_sort_mode(&mut page, BaseSortMode::PathAsc);
        assert_eq!(page.rows[0].file_path, "notes/alpha.md");
        assert_eq!(page.rows[1].file_path, "notes/beta.md");

        apply_base_sort_mode(&mut page, BaseSortMode::PathDesc);
        assert_eq!(page.rows[0].file_path, "notes/beta.md");
        assert_eq!(page.rows[1].file_path, "notes/alpha.md");
    }

    #[test]
    fn bases_pagination_respects_has_more_gate() {
        let context = AppContext::from_args(&super::CliArgs {
            vault_root: None,
            db_path: None,
        });
        let mut app = AppState {
            base_page_number: 1,
            base_table_page: Some(sample_base_page(false)),
            ..AppState::default()
        };
        app.next_base_page(&context);
        assert_eq!(app.base_page_number, 1);

        app.base_table_page = Some(sample_base_page(true));
        app.next_base_page(&context);
        assert_eq!(app.base_page_number, 2);
        app.previous_base_page(&context);
        assert_eq!(app.base_page_number, 1);
    }
}
