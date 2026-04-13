CREATE TABLE IF NOT EXISTS raw.raw_nppes_weekly (
    data JSONB,
    loaded_at TIMESTAMPTZ DEFAULT NOW()
);
