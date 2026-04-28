# CMS Medicare Part D Data Pipeline
 
An end-to-end data engineering portfolio project that utilizes a variety of tools common in modern data management workflows to construct a fully functional pipeline which reliably processes data from source to deliverable, touching on extraction, transformation, modelling, and visualization along the way.        
**Live Dashboard:** https://dustinpineau.github.io/cms_portfolio
 
---
 
## Overview
    
This pipeline was intended as a way to demonstrate data engineering proficiency and as a learning exercise to gain hands-on experience with tools that aren't used in my day-to-day work environment. I chose CMS Medicare Part D as the dataset for this project because it's large-scale, public, easily auditable, and was already familiar through my work as an analyst in the pharmaceutical industry.
 
### Data Sources
 
**National Plan and Provider Enumeration System (NPPES)**
NPPES is a CMS-maintained registry of healthcare providers in the United States. It covers any providers issued a National Provider Identifier (NPI), including those who bill Medicare, Medicaid, and most private insurers. The initial load uses the NPPES Data Dissemination V.2 (April 13, 2026) bulk file, comprising approximately 9.5 million records. Weekly NPI updates are ingested on a scheduled basis and used to maintain a Type 2 Slowly Changing Dimension (SCD2) at the data mart layer, providing full auditability of provider attribute changes over time.
 
**Medicare Part D Prescribers by Provider and Drug (CY2023)**
Published by CMS at data.cms.gov, this dataset contains approximately 26.8 million records covering ~1.1 million unique prescribers and ~3,000 drugs for calendar year 2023 — the most recent year available as of April 2026. Data is fetched via the CMS public data API using a custom Python client and ingested into PostgreSQL, where it is used to enrich the NPPES provider dimension.
 
---
 
## Architecture
 
### Infrastructure
- **Host:** Arch Linux, QEMU/KVM hypervisor
- **Guest:** Rocky Linux 9
- **Database:** PostgreSQL 17
### Ingestion
- **Language:** Python 3.14
- **CMS Part D:** Custom API client with offset pagination.
- **NPPES:** Bulk dissemination download script for initial load; separate weekly update download script for incremental ingestion.
### Storage Schema Layout
| Schema | Purpose |
|--------|---------|
| `raw` | JSONB landing zone for CMS Part D API responses |
| `stg` | Unpacked, typed staging tables |
| `dm` | Kimball dimensional model (facts + dimensions) |
| `adm` | Stored procedures for SCD2 updates and maintenance |
| `xf` | Intermediate transforms |
 
### Transformation
- **dbt 1.9** models handle staging, unpacking, and some of the data mart layer.
- `dim_provider` is managed by a PostgreSQL stored procedure rather than dbt due to the complexity of the SCD2 update logic.
### Orchestration
- **Apache Airflow 2**
- Weekly DAG: `download_nppes_weekly` → `refresh_nppes_weekly` → `update_dim_provider` → `dbt_run`
### Visualization
- **Evidence.dev**
  - Summary: KPIs, top drugs by spend, top providers by volume.
  - Provider: searchable provider list with drill-down to individual provider detail and prescribing patterns.
  - Drug: searchable drug list with drill-down to individual drug detail and top prescribers.
  - Geography: US map of drug spend by state with drill-down to state-level provider summary.

---
 
## Dimensional Model
 
The data mart uses Kimball dimensional modeling with the star schema centered on prescription claims.
 
### Fact Table
**`dm.fact_part_d_claims`** — one row per provider per drug per year.
### Dimension Tables
**`dm.dim_provider`** — SCD2, updated by stored procedure.
**`dm.dim_drug`**

---
 
## Key Design Decisions
 
**SCD2 on dim_provider**
NPPES updates weekly. SCD2 preserves a full audit trail of provider attribute changes over time. 

**Custom CMS API pagination**
The ingestion client uses the API's native size/offset pagination with a found_rows termination safeguard. 

**dim_provider scoped to Part D prescribers**
The provider dimension is scoped to ~1.1M prescribers who appear in the Part D claims data rather than the full 9.5M NPPES providers. Keeping data relevant to immediate reporting needs only was a pragmatic decision, a production implementation would include the full dataset.
 
**Rocky Linux VM**
Rocky Linux was chosen to simulate an enterprise RHEL-compatible data server environment, which is common in healthcare data settings.
 
---
 
## Repository Structure
 
```
cms_portfolio/
├── airflow/dags/        # Airflow DAG definitions
├── dbt/cms_medicare/    # dbt project (models, tests, docs)
├── evidence/            # Evidence.dev dashboard
│   ├── pages/           # Dashboard pages (index, providers, drug, geography)
│   └── sources/         # SQL source queries
├── python/              # Ingestion scripts
│   ├── cms_part_d_ingest.py
│   ├── nppes_bulk_download.py
│   └── nppes_weekly_download.py
└── sql/                 # Raw SQL scripts
    ├── 00_admin/        # Schema setup, stored procedures
    ├── 01_ingestion/
    ├── 02_staging/
    ├── 03_transform/
    ├── 04_marts/
    └── 06_maintenance/
```
 
---
 
## Setup
 
Full VM and database setup is documented in `qemu_rocky_linux_setup.docx`. Prerequisites: QEMU/KVM, Rocky Linux 9, PostgreSQL 17, Apache Airflow, Python 3, dbt-postgres. 

---
 
## Dashboard
 
Live: https://dustinpineau.github.io/cms_portfolio
 
Built with Evidence.dev and deployed to GitHub Pages.
