select
    d.brnd_name,
    d.gnrc_name,
    sum(f.tot_drug_cst)     as total_drug_cost,
    sum(f.tot_30day_fills)  as total_prescriptions,
    count(*)                as total_claims
from fact_part_d_claims f
join dim_drug d on f.drug_key = d.drug_key
group by d.brnd_name, d.gnrc_name
order by total_drug_cost desc
limit 20
