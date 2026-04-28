---
title: Drug Detail
---

```sql drug_detail
select
    brnd_name,
    gnrc_name,
    total_claims,
    total_drug_cost
from cms_portfolio.drug_list
where drug_key = '${params.drug_key}'
```

```sql drug_prescribers
select * from cms_portfolio.drug_prescribers
where drug_key = '${params.drug_key}'
order by total_claims desc
```

# {drug_detail[0].brnd_name}

<BigValue data={drug_detail} value=brnd_name title="Brand Name"/>
<BigValue data={drug_detail} value=gnrc_name title="Generic Name"/>
<BigValue data={drug_detail} value=total_claims title="Total Claims" fmt=num0/>
<BigValue data={drug_detail} value=total_drug_cost title="Total Drug Cost" fmt=usd0/>

## Top Prescribers

<DataTable data={drug_prescribers}>
    <Column id=prscrbr_last_org_name title="Last Name / Org"/>
    <Column id=prscrbr_first_name title="First Name"/>
    <Column id=prscrbr_type title="Specialty"/>
    <Column id=location_city title="City"/>
    <Column id=location_state title="State"/>
    <Column id=total_claims title="Total Claims" fmt=num0/>
    <Column id=total_drug_cost title="Drug Cost" fmt=usd0/>
</DataTable>
