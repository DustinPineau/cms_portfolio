{{ config(materialized='table', tags=['weekly']) }}

with combined as (
    select * from {{ ref('xf_nppes_bulk') }}
    union all
    select * from {{ ref('xf_nppes_weekly') }}
),

deduped as (
    select distinct on (npi)
        *
    from combined
    order by npi, loaded_at desc
)

select * from deduped
