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
