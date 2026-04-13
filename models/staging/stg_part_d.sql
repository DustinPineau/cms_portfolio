{{ config(materialized='table', tags=['annual']) }}

select
    -- identifiers and descriptors (text)
    data->>'Prscrbr_NPI'            as prscrbr_npi,
    data->>'Prscrbr_Last_Org_Name'  as prscrbr_last_org_name,
    data->>'Prscrbr_First_Name'     as prscrbr_first_name,
    data->>'Prscrbr_City'           as prscrbr_city,
    data->>'Prscrbr_State_Abrvtn'   as prscrbr_state_abrvtn,
    data->>'Prscrbr_State_FIPS'     as prscrbr_state_fips,
    data->>'Prscrbr_Type'           as prscrbr_type,
    data->>'Prscrbr_Type_Src'       as prscrbr_type_src,
    data->>'Brnd_Name'              as brnd_name,
    data->>'Gnrc_Name'              as gnrc_name,

    -- claim metrics (numeric)
    nullif(data->>'Tot_Clms', '')::integer          as tot_clms,
    nullif(data->>'Tot_30day_Fills', '')::numeric   as tot_30day_fills,
    nullif(data->>'Tot_Day_Suply', '')::integer     as tot_day_suply,
    nullif(data->>'Tot_Drug_Cst', '')::numeric      as tot_drug_cst,
    nullif(data->>'Tot_Benes', '')::numeric         as tot_benes,

    -- 65+ metrics (numeric)
    nullif(data->>'GE65_Tot_Clms', '')::integer         as ge65_tot_clms,
    nullif(data->>'GE65_Tot_30day_Fills', '')::numeric  as ge65_tot_30day_fills,
    nullif(data->>'GE65_Tot_Day_Suply', '')::integer    as ge65_tot_day_suply,
    nullif(data->>'GE65_Tot_Drug_Cst', '')::numeric     as ge65_tot_drug_cst,
    nullif(data->>'GE65_Tot_Benes', '')::numeric        as ge65_tot_benes,

    -- suppression flags (text)
    data->>'GE65_Sprsn_Flag'        as ge65_sprsn_flag,
    data->>'GE65_Bene_Sprsn_Flag'   as ge65_bene_sprsn_flag,

    -- metadata
    loaded_at

from raw.raw_part_d
