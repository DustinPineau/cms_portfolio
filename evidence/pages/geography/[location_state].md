---
title: State Detail
---

```sql state_summary
select * from cms_portfolio.geography_state
where location_state = '${params.location_state}'
```

```sql state_providers
select * from cms_portfolio.geography_state_providers
where location_state = '${params.location_state}'
```

# {state_summary[0].location_state}

<BigValue data={state_summary} value=total_claims title="Total Claims" fmt=num0/>
<BigValue data={state_summary} value=total_drug_cost title="Total Drug Cost" fmt=usd0/>
<BigValue data={state_summary} value=provider_count title="Provider Count" fmt=num0/>

## Top Providers

<DataTable data={state_providers}>
    <Column id=npi title="NPI"/>
    <Column id=prscrbr_last_org_name title="Last Name / Org"/>
    <Column id=prscrbr_first_name title="First Name"/>
    <Column id=prscrbr_type title="Specialty"/>
    <Column id=location_city title="City"/>
    <Column id=total_claims title="Total Claims" fmt=num0/>
    <Column id=total_drug_cost title="Drug Cost" fmt=usd0/>
</DataTable>
