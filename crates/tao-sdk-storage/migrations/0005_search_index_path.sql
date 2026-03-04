ALTER TABLE search_index ADD COLUMN normalized_path TEXT;

UPDATE search_index
SET normalized_path = COALESCE(
  (
    SELECT f.normalized_path
    FROM files f
    WHERE f.file_id = search_index.file_id
  ),
  normalized_path_lc
)
WHERE normalized_path IS NULL;

CREATE INDEX IF NOT EXISTS idx_search_index_path ON search_index(normalized_path);
