---
title: Provider Deep Dive
---

```sql provider_list
select * from cms_portfolio.provider_list
```

## Find a Provider

<DataTable
    data={provider_list}
    search=true
    link=link
>
    <Column id=prscrbr_npi title="NPI"/>
    <Column id=prscrbr_last_org_name title="Last Name / Org"/>
    <Column id=prscrbr_first_name title="First Name"/>
    <Column id=prscrbr_type title="Specialty"/>
    <Column id=location_city title="City"/>
    <Column id=location_state title="State"/>
    <Column id=total_claims title="Total Claims" fmt=num0/>
    <Column id=total_drug_cost title="Drug Cost" fmt=usd0/>
</DataTable>

