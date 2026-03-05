ALTER TABLE links ADD COLUMN unresolved_reason TEXT;
ALTER TABLE links ADD COLUMN source_field TEXT NOT NULL DEFAULT 'body';

UPDATE links
SET source_field = 'body'
WHERE source_field IS NULL
   OR trim(source_field) = '';

CREATE INDEX IF NOT EXISTS idx_links_unresolved_reason ON links(unresolved_reason);
CREATE INDEX IF NOT EXISTS idx_links_source_field ON links(source_field);
