# Financial Fraud Analytics Pipeline — Azure Data Engineering Portfolio Project

An end-to-end Azure data engineering pipeline, built to demonstrate a full
medallion-architecture (Bronze/Silver/Gold) build on Azure: ingestion,
transformation, governance, orchestration, and BI serving — using a public
financial transactions dataset as the demo data source.

Built as a personal portfolio project to demonstrate Azure data engineering
skills (ADF, ADLS Gen2, Databricks + Unity Catalog, Synapse Analytics, Power
BI) in a domain (finance/fraud) outside my core professional background in
healthcare data engineering.

## Architecture

See `architecture-diagram.md` for the full diagram. High level:
Kaggle source files
|
v
ADLS Gen2 (landing/) --[ADF: metadata-driven pipeline]-->
Databricks notebook --[from_json/explode, explicit schemas]--> Bronze (Delta, External, all-String)
|
v
Databricks notebook --[cleansing, masking, dedup, DQ flags]--> Silver (Delta, External, star schema)
|
v
Databricks notebook --[aggregation]--> Gold (Delta, External, pre-aggregated marts)
|
v
Synapse Serverless SQL (views over Gold) --> Power BI (Import mode)
Full detail on every decision and its reasoning: see `docs/design_decisions.md`.

## Tech Stack

- **Azure Data Factory** — orchestration (metadata-driven, config-file-driven pipelines)
- **Azure Data Lake Storage Gen2** — medallion-layered storage (landing/bronze/silver/gold)
- **Azure Databricks** (Premium, Unity Catalog) — transformation, PySpark/Delta Lake
- **Azure Synapse Analytics** (serverless SQL) — serving layer
- **Power BI** — dashboarding
- **Azure Key Vault** — centralized secrets, RBAC-based
- **Azure CLI / PowerShell** — infrastructure as code (`infra/`, `infra-ps/`)

## Repository Structure
finance-fraud-azure-pipeline/
├── infra/ # Azure CLI (bash) provisioning scripts
├── infra-ps/ # PowerShell equivalents (Windows-friendly)
├── notebooks/
│ ├── 01_bronze_ingestion.py
│ ├── 02_silver_transformation.py
│ ├── 03_gold_aggregation.py
│ └── utils/
│ ├── transformation_utils.py
│ └── logging_utils.py
├── pipelines/ # ADF Git-integrated resources
│ ├── linkedService/
│ ├── dataset/
│ └── pipeline/
├── config/ # entity/table config JSON (source of truth; also
│ # deployed to landing/_config/ in ADLS at runtime)
├── sql/
│ └── views/ # Synapse serverless view definitions
├── powerbi/
│ └── fraud_analytics_dashboard.pbix
├── data/
│ └── sample/ # small sample of source data (full data not committed)
├── docs/
│ ├── design_decisions.md
│ └── architecture-diagram.md
├── .gitignore
└── README.md

## Data Source

[`computingvictor/transactions-fraud-datasets`](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets)
on Kaggle — a public synthetic card-transactions dataset (users, cards,
transactions, MCC codes, fraud labels). Not committed in full to this repo
(exceeds GitHub's file size limits and isn't necessary to review the
pipeline code) — see `data/sample/` for a small representative sample, or
download the full dataset directly from Kaggle.

## How This Was Built

Roughly in this order:
1. Domain & dataset selection
2. Azure infrastructure provisioning (resource group, ADLS Gen2, Key Vault, ADF, Databricks)
3. Medallion container/folder structure design
4. Bronze ingestion (ADF + Databricks, metadata-driven)
5. Unity Catalog setup (schemas, external locations, storage credentials)
6. Silver transformation (cleansing, PII handling, data quality flags)
7. Gold aggregation (business-facing marts)
8. Pipeline logging framework
9. Pipeline chaining (master orchestrator)
10. Synapse serverless SQL serving layer
11. Power BI dashboard

## Known Limitations

See the "Known Limitations" and "Explicitly Deferred / Not Built" sections
in `docs/design_decisions.md` for the full, honest list — notably:
data-quality issues in the source `merchant_state` field (left unfixed,
worked around in the dashboard), and email failure/success notifications
(designed but not completed, due to a personal-Microsoft-account OAuth
restriction on the Office 365 connector).

## Author

Dinesh Gautam — Data Engineer, 13+ years in ETL/ELT, and
healthcare data warehousing.