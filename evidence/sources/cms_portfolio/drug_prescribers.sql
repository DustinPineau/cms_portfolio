select
    f.drug_key::text,
    p.npi,
    p.prscrbr_last_org_name,
    p.prscrbr_first_name,
    p.prscrbr_type,
    p.location_city,
    p.location_state,
    sum(f.tot_clms) as total_claims,
    sum(f.tot_30day_fills) as total_prescriptions,
    sum(f.tot_drug_cst) as total_drug_cost
from dm.fact_part_d_claims f
join dm.dim_provider p on f.prscrbr_npi = p.npi
where p.is_current = true
and f.drug_key in (
    select drug_key
    from dm.fact_part_d_claims
    group by drug_key
    order by sum(tot_drug_cst) desc
    limit 500
)
group by f.drug_key, p.npi, p.prscrbr_last_org_name, p.prscrbr_first_name,
    p.prscrbr_type, p.location_city, p.location_state
order by f.drug_key, total_claims desc
