select
    p.prscrbr_npi,
    d.brnd_name,
    d.gnrc_name,
    sum(p.tot_clms) as total_claims,
    sum(p.tot_30day_fills) as total_prescriptions,
    sum(p.tot_drug_cst) as total_drug_cost
from dm.fact_part_d_claims p
join dm.dim_drug d on p.drug_key = d.drug_key
group by p.prscrbr_npi, d.brnd_name, d.gnrc_name
