CREATE TABLE IF NOT EXISTS raw.raw_nppes(
    data JSONB,
    loaded_at TIMESTAMPTZ DEFAULT NOW()
);
