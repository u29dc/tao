CREATE TABLE IF NOT EXISTS schema_migrations (
  id TEXT PRIMARY KEY,
  checksum TEXT NOT NULL,
  applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS files (
  file_id TEXT PRIMARY KEY,
  normalized_path TEXT NOT NULL UNIQUE,
  match_key TEXT NOT NULL UNIQUE,
  absolute_path TEXT NOT NULL,
  size_bytes INTEGER NOT NULL,
  modified_unix_ms INTEGER NOT NULL,
  hash_blake3 TEXT NOT NULL,
  is_markdown INTEGER NOT NULL CHECK (is_markdown IN (0, 1)),
  indexed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_files_match_key ON files(match_key);
CREATE INDEX IF NOT EXISTS idx_files_is_markdown ON files(is_markdown);

CREATE TABLE IF NOT EXISTS links (
  link_id TEXT PRIMARY KEY,
  source_file_id TEXT NOT NULL,
  raw_target TEXT NOT NULL,
  resolved_file_id TEXT,
  heading_slug TEXT,
  block_id TEXT,
  is_unresolved INTEGER NOT NULL CHECK (is_unresolved IN (0, 1)),
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  FOREIGN KEY(source_file_id) REFERENCES files(file_id) ON DELETE CASCADE,
  FOREIGN KEY(resolved_file_id) REFERENCES files(file_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_links_source ON links(source_file_id);
CREATE INDEX IF NOT EXISTS idx_links_resolved ON links(resolved_file_id);
CREATE INDEX IF NOT EXISTS idx_links_unresolved ON links(is_unresolved);

CREATE TABLE IF NOT EXISTS properties (
  property_id TEXT PRIMARY KEY,
  file_id TEXT NOT NULL,
  key TEXT NOT NULL,
  value_type TEXT NOT NULL,
  value_json TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  FOREIGN KEY(file_id) REFERENCES files(file_id) ON DELETE CASCADE,
  UNIQUE(file_id, key)
);

CREATE INDEX IF NOT EXISTS idx_properties_key ON properties(key);
CREATE INDEX IF NOT EXISTS idx_properties_file ON properties(file_id);

CREATE TABLE IF NOT EXISTS bases (
  base_id TEXT PRIMARY KEY,
  file_id TEXT NOT NULL UNIQUE,
  config_json TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  FOREIGN KEY(file_id) REFERENCES files(file_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS render_cache (
  cache_key TEXT PRIMARY KEY,
  file_id TEXT,
  html TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  FOREIGN KEY(file_id) REFERENCES files(file_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_render_cache_file ON render_cache(file_id);

CREATE TABLE IF NOT EXISTS index_state (
  key TEXT PRIMARY KEY,
  value_json TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
