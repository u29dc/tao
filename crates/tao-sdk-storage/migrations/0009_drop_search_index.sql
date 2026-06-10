DROP TRIGGER IF EXISTS search_index_fts_ai;
DROP TRIGGER IF EXISTS search_index_fts_au;
DROP TRIGGER IF EXISTS search_index_fts_ad;
DROP TABLE IF EXISTS search_index_fts;
DROP INDEX IF EXISTS idx_search_index_path;
DROP INDEX IF EXISTS idx_search_index_path_lc;
DROP INDEX IF EXISTS idx_search_index_title_lc;
DROP TABLE IF EXISTS search_index;
