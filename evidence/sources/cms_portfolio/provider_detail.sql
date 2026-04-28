select
    p.npi,
    p.prscrbr_last_org_name,
    p.prscrbr_first_name,
    p.prscrbr_type,
    p.prscrbr_city,
    p.prscrbr_state_abrvtn,
    p.credential,
    p.status,
    p.sex,
    p.enumeration_date,
    p.last_updated
from dm.dim_provider p
where p.is_current = true
and p.status = 'ACTIVE'
and p.npi in (
    select prscrbr_npi
    from dm.fact_part_d_claims
    group by prscrbr_npi
    order by sum(tot_clms) desc
    limit 1000
)
