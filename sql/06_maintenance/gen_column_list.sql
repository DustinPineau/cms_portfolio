SELECT 'n.' || column_name || ','
FROM information_schema.columns
WHERE table_schema = 'stg'
AND table_name = 'nppes'
ORDER BY ordinal_position;

