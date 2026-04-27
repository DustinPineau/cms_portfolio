select    
    p.npi,
    p.prscrbr_last_org_name,
    p.prscrbr_first_name,
    p.prscrbr_city,
    p.prscrbr_state_abrvtn,
    p.entity_type_code,
    p.status,
    p.credential,
    p.sex,
    p.enumeration_date,
    p.last_updated
from dm.dim_provider p
where p.npi = ${params.npi}
    and is_current = true
