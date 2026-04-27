select
    d.brnd_name,
    d.gnrc_name,
    sum(p.tot_clms) as total_claims,
    sum(p.tot_30day_fills) as total_prescriptions,
    sum(p.tot_drug_cst) as total_drug_cost
from dm.fact_part_d_claims p
join dm.dim_drug d
    on p.drug_key = d.drug_key
where p.prscrbr_npi = '${params.npi}'
group by d.brnd_name, d.gnrc_name
order by total_claims desc
