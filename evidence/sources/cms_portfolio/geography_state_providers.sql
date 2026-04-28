select * from (
    select
        p.location_state,
        p.npi,
        p.prscrbr_last_org_name,
        p.prscrbr_first_name,
        p.prscrbr_type,
        p.location_city,
        sum(f.tot_clms) as total_claims,
        sum(f.tot_30day_fills) as total_prescriptions,
        sum(f.tot_drug_cst) as total_drug_cost,
        row_number() over (
            partition by p.location_state
            order by sum(f.tot_clms) desc
        ) as rn
    from dm.fact_part_d_claims f
    join dm.dim_provider p on f.prscrbr_npi = p.npi
    where p.is_current = true
    and p.status = 'ACTIVE'
    group by p.location_state, p.npi, p.prscrbr_last_org_name,
        p.prscrbr_first_name, p.prscrbr_type, p.location_city
) ranked
where rn <= 100
order by location_state, total_claims desc
