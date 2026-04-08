{{ config(materialized='table') }}

select
    md5(s.prscrbr_npi || '2023-01-01')::uuid            as provider_key,
    md5(s.brnd_name || s.gnrc_name)::uuid               as drug_key,
    2023                                                as claim_year,

    -- claim metrics
    s.tot_clms,
    s.tot_30day_fills,
    s.tot_day_suply,
    s.tot_drug_cst,
    s.tot_benes,

    -- 65+ metrics
    s.ge65_tot_clms,
    s.ge65_tot_30day_fills,
    s.ge65_tot_day_suply,
    s.ge65_tot_drug_cst,
    s.ge65_tot_benes,

    -- suppression flags
    s.ge65_sprsn_flag,
    s.ge65_bene_sprsn_flag

from {{ ref('stg_part_d') }} s

