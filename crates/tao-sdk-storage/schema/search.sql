-- Reproducible derived search schema, part of the current format.

CREATE TABLE derived_dirty_files(file_id TEXT PRIMARY KEY);

CREATE TABLE search_segments (
  segment_id TEXT PRIMARY KEY,
  surface TEXT NOT NULL CHECK (surface IN ('docs', 'files', 'properties', 'tasks', 'graph', 'bases')),
  file_id TEXT NOT NULL,
  normalized_path TEXT NOT NULL,
  normalized_path_lc TEXT NOT NULL,
  extension TEXT NOT NULL,
  field TEXT NOT NULL,
  record_id TEXT,
  label TEXT NOT NULL,
  weight INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  path_text TEXT NOT NULL DEFAULT '',
  title_text TEXT NOT NULL DEFAULT '',
  alias_text TEXT NOT NULL DEFAULT '',
  body_text TEXT NOT NULL DEFAULT '',
  property_text TEXT NOT NULL DEFAULT '',
  task_text TEXT NOT NULL DEFAULT '',
  link_text TEXT NOT NULL DEFAULT '',
  base_text TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  FOREIGN KEY(file_id) REFERENCES files(file_id) ON DELETE CASCADE
);

CREATE TABLE search_aliases (
  alias_id TEXT PRIMARY KEY,
  file_id TEXT NOT NULL,
  normalized_path TEXT NOT NULL,
  normalized_path_lc TEXT NOT NULL,
  extension TEXT NOT NULL,
  surface TEXT NOT NULL CHECK (surface IN ('docs', 'files', 'properties', 'tasks', 'graph', 'bases')),
  alias_norm TEXT NOT NULL,
  alias_compact TEXT NOT NULL,
  source TEXT NOT NULL,
  weight INTEGER NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  FOREIGN KEY(file_id) REFERENCES files(file_id) ON DELETE CASCADE
);

CREATE VIRTUAL TABLE search_segments_fts USING fts5(path_text, title_text, alias_text, body_text, property_text, task_text, link_text, base_text,content='search_segments',content_rowid='rowid',tokenize='unicode61');

CREATE INDEX idx_search_segments_surface ON search_segments(surface, normalized_path, segment_id);

CREATE INDEX idx_search_segments_file ON search_segments(file_id);

CREATE INDEX idx_search_segments_path ON search_segments(normalized_path);

CREATE INDEX idx_search_segments_path_lc ON search_segments(normalized_path_lc);

CREATE INDEX idx_search_segments_extension ON search_segments(extension);

CREATE INDEX idx_search_segments_record ON search_segments(surface, record_id);

CREATE INDEX idx_search_aliases_norm ON search_aliases(alias_norm);

CREATE INDEX idx_search_aliases_compact ON search_aliases(alias_compact);

CREATE INDEX idx_search_aliases_surface ON search_aliases(surface);

CREATE INDEX idx_search_aliases_path ON search_aliases(normalized_path);

CREATE INDEX idx_search_aliases_extension ON search_aliases(extension);

CREATE INDEX idx_search_aliases_file ON search_aliases(file_id);

CREATE TRIGGER count_search_segments_insert AFTER INSERT ON search_segments BEGIN
  UPDATE index_generations SET derived_generation=derived_generation+1,segments_total=segments_total+1 WHERE singleton=1;
  INSERT INTO derived_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM derived_dirty_files WHERE file_id=new.file_id);
END;

CREATE TRIGGER count_search_segments_delete AFTER DELETE ON search_segments BEGIN
  UPDATE index_generations SET derived_generation=derived_generation+1,segments_total=segments_total-1 WHERE singleton=1;
  INSERT INTO derived_dirty_files(file_id) SELECT old.file_id WHERE NOT EXISTS(SELECT 1 FROM derived_dirty_files WHERE file_id=old.file_id);
END;

CREATE TRIGGER count_search_aliases_insert AFTER INSERT ON search_aliases BEGIN
  UPDATE index_generations SET derived_generation=derived_generation+1,aliases_total=aliases_total+1 WHERE singleton=1;
  INSERT INTO derived_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM derived_dirty_files WHERE file_id=new.file_id);
END;

CREATE TRIGGER count_search_aliases_delete AFTER DELETE ON search_aliases BEGIN
  UPDATE index_generations SET derived_generation=derived_generation+1,aliases_total=aliases_total-1 WHERE singleton=1;
  INSERT INTO derived_dirty_files(file_id) SELECT old.file_id WHERE NOT EXISTS(SELECT 1 FROM derived_dirty_files WHERE file_id=old.file_id);
END;

CREATE TRIGGER search_segments_fts_ai AFTER INSERT ON search_segments BEGIN
INSERT INTO search_segments_fts(rowid,path_text, title_text, alias_text, body_text, property_text, task_text, link_text, base_text) VALUES(new.rowid,new.path_text,new.title_text,new.alias_text,new.body_text,new.property_text,new.task_text,new.link_text,new.base_text);
END;

CREATE TRIGGER search_segments_fts_au AFTER UPDATE ON search_segments BEGIN
INSERT INTO search_segments_fts(search_segments_fts,rowid,path_text, title_text, alias_text, body_text, property_text, task_text, link_text, base_text) VALUES('delete',old.rowid,old.path_text,old.title_text,old.alias_text,old.body_text,old.property_text,old.task_text,old.link_text,old.base_text);
INSERT INTO search_segments_fts(rowid,path_text, title_text, alias_text, body_text, property_text, task_text, link_text, base_text) VALUES(new.rowid,new.path_text,new.title_text,new.alias_text,new.body_text,new.property_text,new.task_text,new.link_text,new.base_text);
END;

CREATE TRIGGER search_segments_fts_ad AFTER DELETE ON search_segments BEGIN
INSERT INTO search_segments_fts(search_segments_fts,rowid,path_text, title_text, alias_text, body_text, property_text, task_text, link_text, base_text) VALUES('delete',old.rowid,old.path_text,old.title_text,old.alias_text,old.body_text,old.property_text,old.task_text,old.link_text,old.base_text);
END;

CREATE TRIGGER mark_search_segments_update AFTER UPDATE ON search_segments BEGIN
  UPDATE index_generations SET derived_generation=derived_generation+1 WHERE singleton=1;
  INSERT INTO derived_dirty_files(file_id) SELECT old.file_id WHERE NOT EXISTS(SELECT 1 FROM derived_dirty_files WHERE file_id=old.file_id);
  INSERT INTO derived_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM derived_dirty_files WHERE file_id=new.file_id);
END;
CREATE TRIGGER mark_search_aliases_update AFTER UPDATE ON search_aliases BEGIN
  UPDATE index_generations SET derived_generation=derived_generation+1 WHERE singleton=1;
  INSERT INTO derived_dirty_files(file_id) SELECT old.file_id WHERE NOT EXISTS(SELECT 1 FROM derived_dirty_files WHERE file_id=old.file_id);
  INSERT INTO derived_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM derived_dirty_files WHERE file_id=new.file_id);
END;
