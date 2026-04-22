CREATE TABLE IF NOT EXISTS raw.pipeline_metadata(
    pipeline_name TEXT PRIMARY KEY,
    last_run TIMESTAMPTZ
);
