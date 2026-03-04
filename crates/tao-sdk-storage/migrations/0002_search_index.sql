CREATE TABLE IF NOT EXISTS search_index (
  file_id TEXT PRIMARY KEY,
  normalized_path_lc TEXT NOT NULL,
  title_lc TEXT NOT NULL,
  content_lc TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  FOREIGN KEY(file_id) REFERENCES files(file_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_search_index_path_lc ON search_index(normalized_path_lc);
CREATE INDEX IF NOT EXISTS idx_search_index_title_lc ON search_index(title_lc);
