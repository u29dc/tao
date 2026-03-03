# obs-tui

Terminal UI adapter over the Rust SDK stack.

Current status (`TUI-004`):

- startup route bootstrapped as `placeholder`
- alternate-screen route shell with keymap (`1`/`2`/`3`/`4`, `q`)
- command palette via `:` with `route <name>` and `quit` commands
- notes route loads indexed note windows via SDK bridge and renders selected note content
- notes selection keymap: `up`/`down` (`k`/`j`), `enter` to reload, `r` to refresh
- search route supports query input (`/`), result navigation, and open-note handoff into Notes route
