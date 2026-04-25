CREATE OR REPLACE PROCEDURE adm.update_dim_provider()
LANGUAGE plpgsql
AS $$
DECLARE 
    v_today DATE := CURRENT_DATE;
BEGIN
 
    -- step 0: initial nppes enrichment for existing records
    UPDATE dm.dim_provider d
    SET
        entity_type_code     = n.entity_type_code,
        status               = CASE WHEN n.deactivation_date IS NOT NULL THEN 'INACTIVE' ELSE 'ACTIVE' END,
        credential           = n.credential,
        sex                  = n.sex,
        sole_proprietor      = n.sole_proprietor,
        enumeration_date     = n.enumeration_date,
        last_updated         = n.last_updated,
        deactivation_date    = n.deactivation_date,
        reactivation_date    = n.reactivation_date,
        mailing_address_1    = n.mailing_address_1,
        mailing_address_2    = n.mailing_address_2,
        mailing_city         = n.mailing_city,
        mailing_state        = n.mailing_state,
        mailing_postal_code  = n.mailing_postal_code,
        mailing_telephone    = n.mailing_telephone,
        location_address_1   = n.location_address_1,
        location_address_2   = n.location_address_2,
        location_city        = n.location_city,
        location_state       = n.location_state,
        location_postal_code = n.location_postal_code,
        location_telephone   = n.location_telephone,
        taxonomy_code_1      = n.taxonomy_code_1,
        taxonomy_license_1   = n.taxonomy_license_1,
        taxonomy_state_1     = n.taxonomy_state_1,
        taxonomy_primary_1   = NULLIF(n.taxonomy_primary_1, '')::boolean,
        taxonomy_code_2      = n.taxonomy_code_2,
        taxonomy_license_2   = n.taxonomy_license_2,
        taxonomy_state_2     = n.taxonomy_state_2,
        taxonomy_primary_2   = NULLIF(n.taxonomy_primary_2, '')::boolean,
        taxonomy_code_3      = n.taxonomy_code_3,
        taxonomy_license_3   = n.taxonomy_license_3,
        taxonomy_state_3     = n.taxonomy_state_3,
        taxonomy_primary_3   = NULLIF(n.taxonomy_primary_3, '')::boolean,
        taxonomy_code_4      = n.taxonomy_code_4,
        taxonomy_license_4   = n.taxonomy_license_4,
        taxonomy_state_4     = n.taxonomy_state_4,
        taxonomy_primary_4   = NULLIF(n.taxonomy_primary_4, '')::boolean,
        taxonomy_code_5      = n.taxonomy_code_5,
        taxonomy_license_5   = n.taxonomy_license_5,
        taxonomy_state_5     = n.taxonomy_state_5,
        taxonomy_primary_5   = NULLIF(n.taxonomy_primary_5, '')::boolean
    FROM stg.nppes n
    WHERE d.npi = n.npi
        AND d.entity_type_code IS NULL;

    -- step 0b: populate prscrbr_type and prscrbr_type_src from stg.part_d where null
    UPDATE dm.dim_provider d
    SET
        prscrbr_type     = p.prscrbr_type,
        prscrbr_type_src = p.prscrbr_type_src
    FROM (
        SELECT DISTINCT prscrbr_npi, prscrbr_type, prscrbr_type_src
        FROM stg.part_d
        WHERE prscrbr_npi IS NOT NULL
    ) p
    WHERE d.npi = p.prscrbr_npi
        AND d.prscrbr_type IS NULL;
    
    -- step 1: narrow scope to npis with different values in last_updated
    WITH candidates AS (
        SELECT
            n.npi,
            n.last_updated
        FROM stg.nppes n
        INNER JOIN dm.dim_provider d
            ON n.npi = d.npi
        WHERE COALESCE(n.last_updated, '1900-01-01') >= COALESCE(d.last_updated, '1900-01-01')
            AND d.is_current = true
    ),
 
    -- step 2: identify attribute changes for candidate record pairs
    changed AS (
        SELECT n.npi
        FROM stg.nppes n
        INNER JOIN dm.dim_provider d
            ON n.npi = d.npi
            WHERE n.npi IN (SELECT npi FROM candidates)
            AND d.is_current = true
            AND (
                -- high churn
                COALESCE(n.last_updated::text, '')              != COALESCE(d.last_updated::text, '')   
                OR COALESCE(n.location_address_1, '')           != COALESCE(d.location_address_1, '')
                OR COALESCE(n.location_city, '')                != COALESCE(d.location_city, '')
                OR COALESCE(n.location_state, '')               != COALESCE(d.location_state, '')               
                OR COALESCE(n.location_postal_code, '')         != COALESCE(d.location_postal_code, '')               
                OR COALESCE(n.deactivation_date::text, '')      != COALESCE(d.deactivation_date::text, '')               
                OR COALESCE(n.reactivation_date::text, '')      != COALESCE(d.reactivation_date::text, '')               
                OR COALESCE(n.taxonomy_code_1, '')              != COALESCE(d.taxonomy_code_1, '')               
                OR COALESCE(n.taxonomy_state_1, '')             != COALESCE(d.taxonomy_state_1, '')               
                OR COALESCE(n.taxonomy_license_1, '')           != COALESCE(d.taxonomy_license_1, '')               
                -- medium churn
                OR COALESCE(n.credential, '')                   != COALESCE(d.credential, '')               
                OR COALESCE(n.mailing_address_1, '')            != COALESCE(d.mailing_address_1, '')               
                OR COALESCE(n.mailing_city, '')                 != COALESCE(d.mailing_city, '')               
                OR COALESCE(n.mailing_state, '')                != COALESCE(d.mailing_state, '')               
                OR COALESCE(n.mailing_postal_code, '')          != COALESCE(d.mailing_postal_code, '')               
                OR COALESCE(n.taxonomy_code_2, '')              != COALESCE(d.taxonomy_code_2, '')               
                OR COALESCE(n.taxonomy_code_3, '')              != COALESCE(d.taxonomy_code_3, '')               
                OR COALESCE(n.taxonomy_code_4, '')              != COALESCE(d.taxonomy_code_4, '')               
                OR COALESCE(n.taxonomy_code_5, '')              != COALESCE(d.taxonomy_code_5, '')               
                -- low churn
                OR COALESCE(n.sole_proprietor, '')              != COALESCE(d.sole_proprietor, '')               
                OR COALESCE(n.entity_type_code, '')             != COALESCE(d.entity_type_code, '')               
                OR COALESCE(n.last_name, '')                    != COALESCE(d.prscrbr_last_org_name, '')               
                OR COALESCE(n.first_name, '')                   != COALESCE(d.prscrbr_first_name, '')               
                OR COALESCE(n.sex, '')                          != COALESCE(d.sex, '')               
                OR COALESCE(n.enumeration_date::text, '')       != COALESCE(d.enumeration_date::text, '')               
            )
    )
 
    -- step 3: where npi attributes have changed, flag old records as expired
    UPDATE dm.dim_provider
    SET is_current = false,
        valid_to = v_today
    WHERE npi IN (SELECT npi FROM changed)   
        AND is_current = true;
 
    -- step 4: insert new versions for changed npis
    WITH candidates AS (
        SELECT n.npi
        FROM stg.nppes n
        INNER JOIN dm.dim_provider d 
            ON n.npi = d.npi
            WHERE COALESCE(n.last_updated, '1900-01-01') >= COALESCE(d.last_updated, '1900-01-01')
            AND d.is_current = false
            AND d.valid_to = v_today
    ),
 
    changed AS (
        SELECT DISTINCT n.npi
        FROM stg.nppes n
        INNER JOIN dm.dim_provider d ON n.npi = d.npi
        WHERE n.npi IN (SELECT npi FROM candidates)
    )
    
    INSERT INTO dm.dim_provider (
        provider_key,
        npi,
        prscrbr_last_org_name,
        prscrbr_first_name,
        prscrbr_city,
        prscrbr_state_abrvtn,
        prscrbr_state_fips,
        prscrbr_type,
        prscrbr_type_src,
        entity_type_code,
        status,
        credential,
        sex,
        sole_proprietor,
        enumeration_date,
        last_updated,
        deactivation_date,
        reactivation_date,
        mailing_address_1,
        mailing_address_2,
        mailing_city,
        mailing_state,
        mailing_postal_code,
        mailing_telephone,
        location_address_1,
        location_address_2,
        location_city,
        location_state,
        location_postal_code,
        location_telephone,
        taxonomy_code_1,
        taxonomy_desc_1,
        taxonomy_license_1,
        taxonomy_state_1,
        taxonomy_primary_1,
        taxonomy_code_2,
        taxonomy_desc_2,
        taxonomy_license_2,
        taxonomy_state_2,
        taxonomy_primary_2,
        taxonomy_code_3,
        taxonomy_desc_3,
        taxonomy_license_3,
        taxonomy_state_3,
        taxonomy_primary_3,
        taxonomy_code_4,
        taxonomy_desc_4,
        taxonomy_license_4,
        taxonomy_state_4,
        taxonomy_primary_4,
        taxonomy_code_5,
        taxonomy_desc_5,
        taxonomy_license_5,
        taxonomy_state_5,
        taxonomy_primary_5,
        valid_from,
        valid_to,
        is_current,
        inserted_at
    )
 
    SELECT
        md5(n.npi || v_today::text)::uuid,
        n.npi,
        n.last_name,
        n.first_name,
        n.location_city,
        n.location_state,
        old.prscrbr_state_fips,
        old.prscrbr_type,
        old.prscrbr_type_src,
        n.entity_type_code,
        CASE WHEN n.deactivation_date IS NOT NULL THEN 'INACTIVE' ELSE 'ACTIVE' END,
        n.credential,
        n.sex,
        n.sole_proprietor,
        n.enumeration_date,
        n.last_updated,
        n.deactivation_date,
        n.reactivation_date,
        n.mailing_address_1,
        n.mailing_address_2,
        n.mailing_city,
        n.mailing_state,
        n.mailing_postal_code,
        n.mailing_telephone,
        n.location_address_1,
        n.location_address_2,
        n.location_city,
        n.location_state,
        n.location_postal_code,
        n.location_telephone,
        n.taxonomy_code_1,
        null,
        n.taxonomy_license_1,
        n.taxonomy_state_1,
        NULLIF(n.taxonomy_primary_1, '')::boolean,
        n.taxonomy_code_2,
        null,
        n.taxonomy_license_2,
        n.taxonomy_state_2,
        NULLIF(n.taxonomy_primary_2, '')::boolean,
        n.taxonomy_code_3,
        null,
        n.taxonomy_license_3,
        n.taxonomy_state_3,
        NULLIF(n.taxonomy_primary_3, '')::boolean,
        n.taxonomy_code_4,
        null,
        n.taxonomy_license_4,
        n.taxonomy_state_4,
        NULLIF(n.taxonomy_primary_4, '')::boolean,
        n.taxonomy_code_5,
        null,
        n.taxonomy_license_5,
        n.taxonomy_state_5,
        NULLIF(n.taxonomy_primary_5, '')::boolean,
        v_today,
        null::date,
        true,
        now()
    FROM stg.nppes n
    INNER JOIN dm.dim_provider old ON n.npi = old.npi
    WHERE n.npi IN (SELECT npi FROM changed)
        AND old.is_current = false
        AND old.valid_to = v_today;
 
    -- step 5: insert new npis with no existing record in dm.dim_provider
    -- restricted to npis that exist in stg.part_d (part d providers only)
    INSERT INTO dm.dim_provider (
        provider_key,
        npi,
        prscrbr_last_org_name,
        prscrbr_first_name,
        prscrbr_city,
        prscrbr_state_abrvtn,
        prscrbr_state_fips,
        prscrbr_type,
        prscrbr_type_src,
        entity_type_code,
        status,
        credential,
        sex,
        sole_proprietor,
        enumeration_date,
        last_updated,
        deactivation_date,
        reactivation_date,
        mailing_address_1,
        mailing_address_2,
        mailing_city,
        mailing_state,
        mailing_postal_code,
        mailing_telephone,
        location_address_1,
        location_address_2,
        location_city,
        location_state,
        location_postal_code,
        location_telephone,
        taxonomy_code_1,
        taxonomy_desc_1,
        taxonomy_license_1,
        taxonomy_state_1,
        taxonomy_primary_1,
        taxonomy_code_2,
        taxonomy_desc_2,
        taxonomy_license_2,
        taxonomy_state_2,
        taxonomy_primary_2,
        taxonomy_code_3,
        taxonomy_desc_3,
        taxonomy_license_3,
        taxonomy_state_3,
        taxonomy_primary_3,
        taxonomy_code_4,
        taxonomy_desc_4,
        taxonomy_license_4,
        taxonomy_state_4,
        taxonomy_primary_4,
        taxonomy_code_5,
        taxonomy_desc_5,
        taxonomy_license_5,
        taxonomy_state_5,
        taxonomy_primary_5,
        valid_from,
        valid_to,
        is_current,
        inserted_at
    )
 
    SELECT
        md5(n.npi || v_today::text)::uuid,
        n.npi,
        n.last_name,
        n.first_name,
        n.location_city,
        n.location_state,
        null,
        p.prscrbr_type,
        p.prscrbr_type_src,
        n.entity_type_code,
        CASE WHEN n.deactivation_date IS NOT NULL THEN 'INACTIVE' ELSE 'ACTIVE' END,
        n.credential,
        n.sex,
        n.sole_proprietor,
        n.enumeration_date,
        n.last_updated,
        n.deactivation_date,
        n.reactivation_date,
        n.mailing_address_1,
        n.mailing_address_2,
        n.mailing_city,
        n.mailing_state,
        n.mailing_postal_code,
        n.mailing_telephone,
        n.location_address_1,
        n.location_address_2,
        n.location_city,
        n.location_state,
        n.location_postal_code,
        n.location_telephone,
        n.taxonomy_code_1,
        null,
        n.taxonomy_license_1,
        n.taxonomy_state_1,
        NULLIF(n.taxonomy_primary_1, '')::boolean,
        n.taxonomy_code_2,
        null,
        n.taxonomy_license_2,
        n.taxonomy_state_2,
        NULLIF(n.taxonomy_primary_2, '')::boolean,
        n.taxonomy_code_3,
        null,
        n.taxonomy_license_3,
        n.taxonomy_state_3,
        NULLIF(n.taxonomy_primary_3, '')::boolean,
        n.taxonomy_code_4,
        null,
        n.taxonomy_license_4,
        n.taxonomy_state_4,
        NULLIF(n.taxonomy_primary_4, '')::boolean,
        n.taxonomy_code_5,
        null,
        n.taxonomy_license_5,
        n.taxonomy_state_5,
        NULLIF(n.taxonomy_primary_5, '')::boolean,
        v_today,
        null::date,
        true,
        now()
    FROM stg.nppes n
    LEFT JOIN (
        SELECT DISTINCT prscrbr_npi, prscrbr_type, prscrbr_type_src
        FROM stg.part_d
        WHERE prscrbr_npi IS NOT NULL
    ) p ON n.npi = p.prscrbr_npi
    WHERE NOT EXISTS (
        SELECT 1 FROM dm.dim_provider d
        WHERE d.npi = n.npi
    )
    AND EXISTS (
        SELECT 1 FROM stg.part_d p
        WHERE p.prscrbr_npi = n.npi
    );
 
    RAISE NOTICE 'update_dim_provider complete';
END;
$$;
