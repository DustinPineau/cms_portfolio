---
title: CMS Medicare Part D — Summary
---

```sql kpi_totals
select * from cms_portfolio.kpi_totals
```

<BigValue 
    data={kpi_totals} 
    value=total_claims 
    title="Total Claims"
    fmt=num0
/>
<BigValue 
    data={kpi_totals} 
    value=total_prescriptions 
    title="Total Prescriptions"
    fmt=num0
/>
<BigValue 
    data={kpi_totals} 
    value=total_drug_cost 
    title="Total Drug Cost"
    fmt=usd0
/>
<BigValue 
    data={kpi_totals} 
    value=provider_count 
    title="Providers"
    fmt=num0
/>
<BigValue 
    data={kpi_totals} 
    value=unique_drugs 
    title="Unique Drugs"
    fmt=num0
/>

## Top Drugs by Spend

```sql top_drugs_by_spend
select * from cms_portfolio.top_drugs_by_spend
```

<DataTable data={top_drugs_by_spend} rows=20/>

## Top Providers by Volume
```sql top_providers_by_volume
select * from cms_portfolio.top_providers_by_volume
```
<DataTable data={top_providers_by_volume} rows=20>
    <Column id=prscrbr_npi title="NPI"/>
    <Column id=prscrbr_last_org_name title="Last Name / Org"/>
    <Column id=prscrbr_first_name title="First Name"/>
    <Column id=prscrbr_type title="Specialty"/>
    <Column id=location_city title="City"/>
    <Column id=location_state title="State"/>
    <Column id=total_claims title="Total Claims" fmt=num0/>
    <Column id=total_drug_cost title="Drug Cost" fmt=usd0/>
</DataTable>

