---
title: State Detail
---

```sql state_summary
select
    location_state,
    format('{:,}', cast(provider_count as integer)) as provider_count_fmt,
    format('{:,}', cast(total_claims as integer)) as total_claims_fmt,
    format('{:,.2f}', total_drug_cost) as total_drug_cost_fmt
from cms_portfolio.geography_state
where location_state = '${params.location_state}'
```

```sql state_providers
select * from cms_portfolio.geography_state_providers
where location_state = '${params.location_state}'
```

# {state_summary[0].location_state}

**Total Claims:** {state_summary[0].total_claims_fmt}  
**Total Drug Cost:** ${state_summary[0].total_drug_cost_fmt}  
**Provider Count:** {state_summary[0].provider_count_fmt}  

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
