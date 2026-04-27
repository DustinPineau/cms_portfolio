---
title: Provider Detail
---

```sql provider_detail
select * from cms_portfolio.provider_detail
where npi = '${params.npi}'
```

```sql provider_drugs
select * from cms_portfolio.provider_drugs
where prscrbr_npi = '${params.npi}'
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
