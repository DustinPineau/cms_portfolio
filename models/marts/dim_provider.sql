{{ config(materialized='table') }}

select
    md5(prscrbr_npi || '2023-01-01')::uuid as provider_key,
    prscrbr_npi                 as npi,
    prscrbr_last_org_name,
    prscrbr_first_name,
    prscrbr_city,
    prscrbr_state_abrvtn,
    prscrbr_state_fips,
    prscrbr_type,
    prscrbr_type_src,
    '2023-01-01'::date          as valid_from,
    null::date                  as valid_to,
    true                        as is_current
    
from (
    select distinct
        prscrbr_npi,
        prscrbr_last_org_name,
        prscrbr_first_name,
        prscrbr_city,
        prscrbr_state_abrvtn,
        prscrbr_state_fips,
        prscrbr_type,
        prscrbr_type_src
    from {{ ref('stg_part_d') }}
) as providers

