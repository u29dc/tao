CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY,
  file_id TEXT NOT NULL,
  file_path TEXT NOT NULL,
  file_path_lc TEXT NOT NULL,
  line_number INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('open', 'done', 'cancelled')),
  text TEXT NOT NULL,
  text_lc TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  FOREIGN KEY(file_id) REFERENCES files(file_id) ON DELETE CASCADE,
  UNIQUE(file_id, line_number)
);

CREATE INDEX IF NOT EXISTS idx_tasks_file ON tasks(file_id);
CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state);
CREATE INDEX IF NOT EXISTS idx_tasks_text_lc ON tasks(text_lc);
CREATE INDEX IF NOT EXISTS idx_tasks_file_line ON tasks(file_id, line_number);
CREATE INDEX IF NOT EXISTS idx_tasks_path_line ON tasks(file_path, line_number);
CREATE INDEX IF NOT EXISTS idx_tasks_path_lc ON tasks(file_path_lc);
