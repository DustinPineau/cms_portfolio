select
    f.drug_key::text,
    p.npi,
    p.prscrbr_last_org_name,
    p.prscrbr_first_name,
    p.prscrbr_type,
    p.location_city,
    p.location_state,
    f.total_claims,
    f.total_prescriptions,
    f.total_drug_cost
from (
    select drug_key, prscrbr_npi,
        sum(tot_clms) as total_claims,
        sum(tot_30day_fills) as total_prescriptions,
        sum(tot_drug_cst) as total_drug_cost,
        row_number() over (
            partition by drug_key
            order by sum(tot_clms) desc
        ) as rn
    from dm.fact_part_d_claims
    where drug_key in (
        select drug_key from dm.fact_part_d_claims
        group by drug_key
        order by sum(tot_drug_cst) desc
        limit 50
    )
    group by drug_key, prscrbr_npi
) f
join dm.dim_provider p on f.prscrbr_npi = p.npi
where f.rn <= 100
and p.is_current = true
order by f.drug_key, f.total_claims desc
