{{ config(materialized='table') }}

select
    md5(brnd_name || gnrc_name)::uuid           as drug_key,
    brnd_name,
    gnrc_name

from(
    select distinct
        brnd_name,
        gnrc_name
    from {{ ref('stg_part_d') }}
) as drugs
