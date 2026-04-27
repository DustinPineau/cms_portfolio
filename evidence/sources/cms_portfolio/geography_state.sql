select
    p.location_state,
    count(distinct f.prscrbr_npi) as provider_count,
    sum(f.tot_clms) as total_claims,
    sum(f.tot_30day_fills) as total_prescriptions,
    sum(f.tot_drug_cst) as total_drug_cost
from dm.fact_part_d_claims f
join dm.dim_provider p on f.prscrbr_npi = p.npi
where p.is_current = true
and p.location_state is not null
group by p.location_state
order by total_drug_cost desc
