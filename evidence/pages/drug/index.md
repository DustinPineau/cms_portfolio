---
title: Drug Deep Dive
---

```sql drug_list
select * from cms_portfolio.drug_list
```

## Top Drugs by Spend

<DataTable data={drug_list} search=true link=link>
    <Column id=brnd_name title="Brand Name"/>
    <Column id=gnrc_name title="Generic Name"/>
    <Column id=total_claims title="Total Claims" fmt=num0/>
    <Column id=total_prescriptions title="Total Prescriptions" fmt=num0/>
    <Column id=total_drug_cost title="Drug Cost" fmt=usd0/>
</DataTable>
