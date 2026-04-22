CREATE OR REPLACE PROCEDURE adm.refresh_nppes_weekly()
LANGUAGE plpgsql
AS $$
BEGIN

    -- append new records from xf.nppes_weekly into stg.nppes
    INSERT INTO stg.nppes
    SELECT * FROM xf.nppes_weekly;

    -- deduplicate stg.nppes keeping most recent record per npi
    WITH dupes AS (
        SELECT ctid,
            ROW_NUMBER() OVER (
                PARTITION BY npi
                ORDER BY loaded_at DESC
            ) AS rn
        FROM stg.nppes
    )
    DELETE FROM stg.nppes
    WHERE ctid IN (SELECT ctid FROM dupes WHERE rn > 1);

    RAISE NOTICE 'refresh_nppes_weekly completed';

END;
$$;
