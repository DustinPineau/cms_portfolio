---
title: Provider Detail
---

```sql provider_detail
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
from dim_provider p
where p.npi = '${params.npi}'
    and p.is_current = true
```

```sql provider_drugs
select
    d.brnd_name,
    d.gnrc_name,
    sum(p.tot_clms) as total_claims,
    sum(p.tot_30day_fills) as total_prescriptions,
    sum(p.tot_drug_cst) as total_drug_cost
from fact_part_d_claims p
join dim_drug d on p.drug_key = d.drug_key
where p.prscrbr_npi = '${params.npi}'
group by d.brnd_name, d.gnrc_name
order by total_claims desc
```

# {provider_detail[0].prscrbr_first_name} {provider_detail[0].prscrbr_last_org_name}

**Specialty:** {provider_detail[0].prscrbr_type}  
**Location:** {provider_detail[0].prscrbr_city}, {provider_detail[0].prscrbr_state_abrvtn}  
**Credential:** {provider_detail[0].credential}  
**Status:** {provider_detail[0].status}  
**NPI:** {provider_detail[0].npi}  

## Prescribing Patterns

<DataTable data={provider_drugs}>
    <Column id=brnd_name title="Brand Name"/>
    <Column id=gnrc_name title="Generic Name"/>
    <Column id=total_claims title="Total Claims" fmt=num0/>
    <Column id=total_prescriptions title="Total Prescriptions" fmt=num0/>
    <Column id=total_drug_cost title="Drug Cost" fmt=usd0/>
</DataTable>
