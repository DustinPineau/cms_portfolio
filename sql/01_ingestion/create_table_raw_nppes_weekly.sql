CREATE TABLE IF NOT EXISTS raw.nppes_weekly (
    data JSONB,
    loaded_at TIMESTAMPTZ DEFAULT NOW()
);
