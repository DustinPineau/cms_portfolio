select
    f.prscrbr_npi,
    p.prscrbr_last_org_name,
    p.prscrbr_first_name,
    p.prscrbr_type,
    p.location_city,
    p.location_state,
    sum(f.tot_clms)            as total_claims,
    sum(f.tot_30day_fills)     as total_prescriptions,
    sum(f.tot_drug_cst)        as total_drug_cost,
    '/providers/' || f.prscrbr_npi as link
from dm.fact_part_d_claims f
join dm.dim_provider p on f.prscrbr_npi = p.npi
where p.is_current = true
and p.status = 'ACTIVE'
group by
    f.prscrbr_npi,
    p.prscrbr_last_org_name,
    p.prscrbr_first_name,
    p.prscrbr_type,
    p.location_city,
    p.location_state
order by total_claims desc
limit 1000
