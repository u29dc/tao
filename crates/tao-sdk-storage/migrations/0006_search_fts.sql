CREATE VIRTUAL TABLE IF NOT EXISTS search_index_fts USING fts5 (
  file_id UNINDEXED,
  normalized_path,
  title,
  content,
  tokenize = 'unicode61'
);

CREATE TRIGGER IF NOT EXISTS search_index_fts_ai
AFTER INSERT ON search_index
BEGIN
  INSERT INTO search_index_fts (rowid, file_id, normalized_path, title, content)
  VALUES (new.rowid, new.file_id, new.normalized_path_lc, new.title_lc, new.content_lc);
END;

CREATE TRIGGER IF NOT EXISTS search_index_fts_au
AFTER UPDATE ON search_index
BEGIN
  DELETE FROM search_index_fts
  WHERE rowid = old.rowid;
  INSERT INTO search_index_fts (rowid, file_id, normalized_path, title, content)
  VALUES (new.rowid, new.file_id, new.normalized_path_lc, new.title_lc, new.content_lc);
END;

CREATE TRIGGER IF NOT EXISTS search_index_fts_ad
AFTER DELETE ON search_index
BEGIN
  DELETE FROM search_index_fts
  WHERE rowid = old.rowid;
END;

INSERT OR REPLACE INTO search_index_fts (rowid, file_id, normalized_path, title, content)
SELECT rowid, file_id, normalized_path_lc, title_lc, content_lc
FROM search_index;
