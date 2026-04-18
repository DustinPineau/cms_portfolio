{{ config(materialized='table', tags=['annual']) }}

select
    md5(brnd_name || gnrc_name)::uuid           as drug_key,
    brnd_name,
    gnrc_name

from(
    select distinct
        brnd_name,
        gnrc_name
    from {{ ref('part_d') }}
) as drugs
