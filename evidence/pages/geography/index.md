---
title: Geography
---

```sql geography_state
select * from cms_portfolio.geography_state
```

## Drug Spend by State

<USMap data={geography_state} state=location_state abbreviations=true value=total_drug_cost/>

## State Summary

<DataTable data={geography_state}>
    <Column id=location_state title="State"/>
    <Column id=provider_count title="Providers" fmt=num0/>
    <Column id=total_claims title="Total Claims" fmt=num0/>
    <Column id=total_prescriptions title="Total Prescriptions" fmt=num0/>
    <Column id=total_drug_cost title="Drug Cost" fmt=usd0/>
</DataTable>
