CREATE SCHEMA IF NOT EXISTS manage_indexes;

CREATE TABLE IF NOT EXISTS manage_indexes.index_definitions (
    schemaname TEXT
    , indexname TEXT
    , tablename TEXT
    , indexdef TEXT
    , repl_name TEXT
    , PRIMARY KEY (schemaname, indexname, repl_name)
);

COMMENT ON TABLE manage_indexes.index_definitions IS 'This table helps the bucardo script generate CREATE and DROP statements for large indexes.';

COMMENT ON COLUMN manage_indexes.index_definitions.repl_name IS 'An arbitrary name given by the user in the config to the current replication job, to group indexes together in the event of more than one job running concurrently.';
COMMENT ON COLUMN manage_indexes.index_definitions.schemaname IS 'Name of the schema on which the index marked for removal lives.';
COMMENT ON COLUMN manage_indexes.index_definitions.indexname IS 'Name of the non-primary key index that is safe to remove.';
COMMENT ON COLUMN manage_indexes.index_definitions.tablename IS 'Name of the table on which the index marked for removal lives.';
COMMENT ON COLUMN manage_indexes.index_definitions.indexdef IS 'SQL for recreating the index after removal.'
