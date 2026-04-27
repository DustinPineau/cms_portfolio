select
    d.drug_key::text,
    d.brnd_name,
    d.gnrc_name,
    sum(f.tot_clms) as total_claims,
    sum(f.tot_30day_fills) as total_prescriptions,
    sum(f.tot_drug_cst) as total_drug_cost,
    '/drug/' || d.drug_key::text as link
from dm.fact_part_d_claims f
join dm.dim_drug d on f.drug_key = d.drug_key
group by d.drug_key, d.brnd_name, d.gnrc_name
order by total_drug_cost desc
