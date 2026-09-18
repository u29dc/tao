-- Current canonical and extraction schema. Incompatible changes require a new format epoch.

CREATE TABLE schema_migrations (
  id TEXT PRIMARY KEY,
  checksum TEXT NOT NULL,
  applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE files (
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

CREATE TABLE links (
  link_id TEXT PRIMARY KEY,
  source_file_id TEXT NOT NULL,
  raw_target TEXT NOT NULL,
  resolved_file_id TEXT,
  heading_slug TEXT,
  block_id TEXT,
  is_unresolved INTEGER NOT NULL CHECK (is_unresolved IN (0, 1)),
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  unresolved_reason TEXT,
  source_field TEXT NOT NULL DEFAULT 'body',
  FOREIGN KEY(source_file_id) REFERENCES files(file_id) ON DELETE CASCADE,
  FOREIGN KEY(resolved_file_id) REFERENCES files(file_id) ON DELETE SET NULL
);

CREATE TABLE properties (
  property_id TEXT PRIMARY KEY,
  file_id TEXT NOT NULL,
  key TEXT NOT NULL,
  value_type TEXT NOT NULL,
  value_json TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  FOREIGN KEY(file_id) REFERENCES files(file_id) ON DELETE CASCADE,
  UNIQUE(file_id, key)
);

CREATE TABLE bases (
  base_id TEXT PRIMARY KEY,
  file_id TEXT NOT NULL UNIQUE,
  config_json TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  FOREIGN KEY(file_id) REFERENCES files(file_id) ON DELETE CASCADE
);

CREATE TABLE index_state (
  key TEXT PRIMARY KEY,
  value_json TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE tasks (
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

CREATE TABLE canonical_documents (
  file_id TEXT PRIMARY KEY REFERENCES files(file_id) ON DELETE CASCADE,
  source_hash TEXT NOT NULL,
  parser_version INTEGER NOT NULL,
  raw_text TEXT NOT NULL,
  body_text TEXT NOT NULL,
  title TEXT NOT NULL,
  structure_json TEXT NOT NULL CHECK(json_valid(structure_json))
);

CREATE TABLE file_diagnostics (
  path TEXT NOT NULL,
  file_id TEXT REFERENCES files(file_id) ON DELETE SET NULL,
  kind TEXT NOT NULL,
  message TEXT NOT NULL,
  observed_at TEXT NOT NULL DEFAULT(strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  PRIMARY KEY(path,kind)
);

CREATE TABLE index_generations (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  canonical_generation INTEGER NOT NULL DEFAULT 0,
  search_generation INTEGER NOT NULL DEFAULT -1,
  derived_generation INTEGER NOT NULL DEFAULT 0,
  published_derived_generation INTEGER NOT NULL DEFAULT 0,
  files_total INTEGER NOT NULL DEFAULT 0,
  segments_total INTEGER NOT NULL DEFAULT 0,
  aliases_total INTEGER NOT NULL DEFAULT 0,
  published_segments INTEGER NOT NULL DEFAULT 0,
  published_aliases INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE index_dirty_files (file_id TEXT PRIMARY KEY);

CREATE TABLE link_evidence (
 link_id TEXT PRIMARY KEY REFERENCES links(link_id) ON DELETE CASCADE,
 source_start INTEGER NOT NULL, source_end INTEGER NOT NULL,
 line INTEGER NOT NULL, end_line INTEGER NOT NULL,
 raw_expression TEXT NOT NULL, syntax TEXT NOT NULL, fragment_json TEXT NOT NULL CHECK(json_valid(fragment_json)),
 fragment_status TEXT NOT NULL, resolution_rule TEXT NOT NULL,
 candidates_json TEXT NOT NULL CHECK(json_valid(candidates_json))
);

CREATE TABLE content_documents (
    file_id TEXT PRIMARY KEY REFERENCES files(file_id) ON DELETE CASCADE,
    format TEXT NOT NULL,
    file_group TEXT NOT NULL,
    observed_size INTEGER NOT NULL,
    observed_modified_ms INTEGER NOT NULL,
    desired_revision TEXT NOT NULL,
    served_revision TEXT,
    served_extractor_identity TEXT,
    extractor_identity TEXT NOT NULL,
    coverage TEXT NOT NULL,
    availability TEXT NOT NULL DEFAULT 'accessible',
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    diagnostics_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(diagnostics_json))
);

CREATE TABLE content_segments (
    file_id TEXT NOT NULL REFERENCES content_documents(file_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 1),
    locator_kind TEXT NOT NULL CHECK(locator_kind IN ('line','page')),
    source_start INTEGER NOT NULL CHECK(source_start >= 1),
    source_end INTEGER NOT NULL CHECK(source_end >= source_start),
    text TEXT NOT NULL,
    method TEXT NOT NULL,
    coverage TEXT NOT NULL,
    PRIMARY KEY(file_id, ordinal)
);

CREATE TABLE extraction_jobs (
    job_id TEXT PRIMARY KEY,
    file_id TEXT NOT NULL REFERENCES content_documents(file_id) ON DELETE CASCADE,
    desired_revision TEXT NOT NULL,
    extractor_identity TEXT NOT NULL,
    spool_name TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('queued','running','retry','done','failed','superseded')),
    lease_token TEXT,
    lease_until_ms INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_ms INTEGER NOT NULL DEFAULT 0,
    diagnostic TEXT NOT NULL DEFAULT '',
    UNIQUE(file_id,desired_revision,extractor_identity)
);

CREATE TABLE extraction_staged_pages (
  job_id TEXT NOT NULL REFERENCES extraction_jobs(job_id) ON DELETE CASCADE,
  ordinal INTEGER NOT NULL CHECK(ordinal>=1),
  text TEXT NOT NULL,
  method TEXT NOT NULL,
  coverage TEXT NOT NULL,
  PRIMARY KEY(job_id,ordinal)
);

CREATE TABLE extraction_stage_usage(singleton INTEGER PRIMARY KEY CHECK(singleton=1),bytes_total INTEGER NOT NULL DEFAULT 0);

CREATE INDEX idx_files_is_markdown ON files(is_markdown);

CREATE INDEX idx_properties_key ON properties(key);

CREATE INDEX idx_tasks_state ON tasks(state, file_path, line_number, task_id);

CREATE INDEX idx_tasks_file_line ON tasks(file_id, line_number);

CREATE INDEX idx_tasks_path_line ON tasks(file_path, line_number);

CREATE INDEX idx_links_unresolved_link_id ON links(is_unresolved, link_id);

CREATE INDEX idx_links_unresolved_reason ON links(unresolved_reason);

CREATE INDEX idx_links_source_field ON links(source_field);

CREATE INDEX idx_file_diagnostics_file ON file_diagnostics(file_id);

CREATE INDEX idx_links_source_id ON links(source_file_id,link_id);

CREATE INDEX idx_links_target_id ON links(resolved_file_id,link_id);

CREATE INDEX idx_extraction_jobs_ready ON extraction_jobs(state,next_attempt_ms);

CREATE INDEX idx_extraction_jobs_file ON extraction_jobs(file_id);

CREATE TRIGGER canonical_files_insert AFTER INSERT ON files BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.file_id);
  UPDATE index_generations SET files_total=files_total + 1 WHERE singleton=1;
END;

CREATE TRIGGER canonical_files_update AFTER UPDATE ON files BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.file_id);
END;

CREATE TRIGGER canonical_files_delete AFTER DELETE ON files BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT old.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=old.file_id);
  UPDATE index_generations SET files_total=files_total - 1 WHERE singleton=1;
END;

CREATE TRIGGER canonical_canonical_documents_insert AFTER INSERT ON canonical_documents BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.file_id);
END;

CREATE TRIGGER canonical_canonical_documents_update AFTER UPDATE ON canonical_documents BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.file_id);
END;

CREATE TRIGGER canonical_canonical_documents_delete AFTER DELETE ON canonical_documents BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT old.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=old.file_id);
END;

CREATE TRIGGER canonical_properties_insert AFTER INSERT ON properties BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.file_id);
END;

CREATE TRIGGER canonical_properties_update AFTER UPDATE ON properties BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.file_id);
END;

CREATE TRIGGER canonical_properties_delete AFTER DELETE ON properties BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT old.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=old.file_id);
END;

CREATE TRIGGER canonical_tasks_insert AFTER INSERT ON tasks BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.file_id);
END;

CREATE TRIGGER canonical_tasks_update AFTER UPDATE ON tasks BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.file_id);
END;

CREATE TRIGGER canonical_tasks_delete AFTER DELETE ON tasks BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT old.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=old.file_id);
END;

CREATE TRIGGER canonical_links_insert AFTER INSERT ON links BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.source_file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.source_file_id);
END;

CREATE TRIGGER canonical_links_update AFTER UPDATE ON links BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.source_file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.source_file_id);
END;

CREATE TRIGGER canonical_links_delete AFTER DELETE ON links BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT old.source_file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=old.source_file_id);
END;

CREATE TRIGGER canonical_bases_insert AFTER INSERT ON bases BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.file_id);
END;

CREATE TRIGGER canonical_bases_update AFTER UPDATE ON bases BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.file_id);
END;

CREATE TRIGGER canonical_bases_delete AFTER DELETE ON bases BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT old.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=old.file_id);
END;

CREATE TRIGGER canonical_link_evidence_insert AFTER INSERT ON link_evidence BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT source_file_id FROM links WHERE link_id=new.link_id AND NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=links.source_file_id);
END;

CREATE TRIGGER canonical_link_evidence_update AFTER UPDATE ON link_evidence BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT source_file_id FROM links WHERE link_id=new.link_id AND NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=links.source_file_id);
END;

CREATE TRIGGER canonical_link_evidence_delete AFTER DELETE ON link_evidence BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT source_file_id FROM links WHERE link_id=old.link_id AND NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=links.source_file_id);
END;

CREATE TRIGGER canonical_file_diagnostics_insert AFTER INSERT ON file_diagnostics BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.file_id WHERE new.file_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.file_id);
END;

CREATE TRIGGER canonical_file_diagnostics_update AFTER UPDATE ON file_diagnostics BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.file_id WHERE new.file_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.file_id);
END;

CREATE TRIGGER canonical_file_diagnostics_delete AFTER DELETE ON file_diagnostics BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT old.file_id WHERE old.file_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=old.file_id);
END;

CREATE TRIGGER canonical_content_documents_insert AFTER INSERT ON content_documents BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.file_id);
END;

CREATE TRIGGER canonical_content_documents_update AFTER UPDATE ON content_documents BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.file_id);
END;

CREATE TRIGGER canonical_content_documents_delete AFTER DELETE ON content_documents BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT old.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=old.file_id);
END;

CREATE TRIGGER canonical_content_segments_insert AFTER INSERT ON content_segments BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.file_id);
END;

CREATE TRIGGER canonical_content_segments_update AFTER UPDATE ON content_segments BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT new.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=new.file_id);
END;

CREATE TRIGGER canonical_content_segments_delete AFTER DELETE ON content_segments BEGIN
  UPDATE index_generations SET canonical_generation=canonical_generation+1 WHERE singleton=1;
  INSERT INTO index_dirty_files(file_id) SELECT old.file_id WHERE NOT EXISTS(SELECT 1 FROM index_dirty_files WHERE file_id=old.file_id);
END;

CREATE TRIGGER extraction_staged_insert AFTER INSERT ON extraction_staged_pages BEGIN
 UPDATE extraction_stage_usage SET bytes_total=bytes_total+length(CAST(new.text AS BLOB)) WHERE singleton=1;
END;

CREATE TRIGGER extraction_staged_update AFTER UPDATE ON extraction_staged_pages BEGIN
 UPDATE extraction_stage_usage SET bytes_total=bytes_total-length(CAST(old.text AS BLOB))+length(CAST(new.text AS BLOB)) WHERE singleton=1;
END;

CREATE TRIGGER extraction_staged_delete AFTER DELETE ON extraction_staged_pages BEGIN
 UPDATE extraction_stage_usage SET bytes_total=bytes_total-length(CAST(old.text AS BLOB)) WHERE singleton=1;
END;

INSERT INTO index_generations(singleton) VALUES(1);

INSERT INTO extraction_stage_usage(singleton) VALUES(1);
