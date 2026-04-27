select
    count(*)                        as total_claims,
    sum(tot_30day_fills)            as total_prescriptions,
    sum(tot_drug_cst)               as total_drug_cost,
    count(distinct provider_key)    as provider_count,
    count(distinct drug_key)        as unique_drugs
from fact_part_d_claims
