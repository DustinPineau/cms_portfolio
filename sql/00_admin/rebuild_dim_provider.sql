TRUNCATE dm.dim_provider;

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
    valid_from,
    valid_to,
    is_current
)
SELECT
    md5(prscrbr_npi || '2023-01-01')::uuid,
    prscrbr_npi,
    prscrbr_last_org_name,
    prscrbr_first_name,
    prscrbr_city,
    prscrbr_state_abrvtn,
    prscrbr_state_fips,
    prscrbr_type,
    prscrbr_type_src,
    '2023-01-01'::date,
    null::date,
    true                                                                   
FROM (
    SELECT DISTINCT        
        prscrbr_npi,
        prscrbr_last_org_name,
        prscrbr_first_name,
        prscrbr_city,
        prscrbr_state_abrvtn,
        prscrbr_state_fips,
        prscrbr_type,
        prscrbr_type_src
    FROM stg.part_d
) AS providers;                                            
