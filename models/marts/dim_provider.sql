{{ config(materialized='table', tags=['annual']) }}

select
    md5(prscrbr_npi || '2023-01-01')::uuid  as provider_key,
    prscrbr_npi                              as npi,

    -- part d fields
    prscrbr_last_org_name,
    prscrbr_first_name,
    prscrbr_city,
    prscrbr_state_abrvtn,
    prscrbr_state_fips,
    prscrbr_type,
    prscrbr_type_src,

    -- nppes fields (null until scd2 procedure populates them)
    null::text      as entity_type_code,
    null::text      as status,
    null::text      as credential,
    null::text      as sex,
    null::text      as sole_proprietor,
    null::date      as enumeration_date,
    null::date      as last_updated,
    null::date      as deactivation_date,
    null::date      as reactivation_date,
    null::text      as mailing_address_1,
    null::text      as mailing_address_2,
    null::text      as mailing_city,
    null::text      as mailing_state,
    null::text      as mailing_postal_code,
    null::text      as mailing_telephone,
    null::text      as location_address_1,
    null::text      as location_address_2,
    null::text      as location_city,
    null::text      as location_state,
    null::text      as location_postal_code,
    null::text      as location_telephone,
    null::text      as taxonomy_code_1,
    null::text      as taxonomy_desc_1,
    null::text      as taxonomy_license_1,
    null::text      as taxonomy_state_1,
    null::boolean   as taxonomy_primary_1,
    null::text      as taxonomy_code_2,
    null::text      as taxonomy_desc_2,
    null::text      as taxonomy_license_2,
    null::text      as taxonomy_state_2,
    null::boolean   as taxonomy_primary_2,
    null::text      as taxonomy_code_3,
    null::text      as taxonomy_desc_3,
    null::text      as taxonomy_license_3,
    null::text      as taxonomy_state_3,
    null::boolean   as taxonomy_primary_3,
    null::text      as taxonomy_code_4,
    null::text      as taxonomy_desc_4,
    null::text      as taxonomy_license_4,
    null::text      as taxonomy_state_4,
    null::boolean   as taxonomy_primary_4,
    null::text      as taxonomy_code_5,
    null::text      as taxonomy_desc_5,
    null::text      as taxonomy_license_5,
    null::text      as taxonomy_state_5,
    null::boolean   as taxonomy_primary_5,

    -- scd2 columns
    '2023-01-01'::date  as valid_from,
    null::date          as valid_to,
    true                as is_current

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
    from {{ ref('part_d') }}
) as providers
