CREATE TABLE IF NOT EXISTS raw.nppes_bulk(
    data JSONB,
    loaded_at TIMESTAMPTZ DEFAULT NOW()
);
