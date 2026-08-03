# Design Decisions & Architecture Notes

This document captures the key design decisions made while building this project,
the reasoning behind them, known limitations, and what a production version of
this pipeline would do differently. Written as a running log rather than a
polished spec, since the reasoning itself is as much the point as the outcome.

## Domain & Data

- Chosen domain: banking/financial transaction fraud analytics. Deliberately
  picked outside my professional background (13+ years in healthcare data) to
  demonstrate breadth, and because finance is a strong Azure data engineering
  market in Australia.
- Dataset: a public Kaggle dataset (`computingvictor/transactions-fraud-datasets`),
  a well-known synthetic/anonymised card-transactions dataset with 5 source
  files: `transactions_data.csv`, `users_data.csv`, `cards_data.csv`,
  `mcc_codes.json`, `train_fraud_labels.json`.
- Transactions data trimmed to ~1M rows for local dev/demo purposes (full
  source file is ~5M+ rows) to keep free-tier Azure costs and iteration time
  manageable.
- The dataset is US-based (addresses, MCC/merchant data). Left as-is rather
  than artificially remapped to Australian geography, since forcing fake local
  data onto a real dataset would look worse under scrutiny than being upfront
  that it's a US-sourced public dataset.

## Architecture Overview

Medallion architecture (Landing -> Bronze -> Silver -> Gold) on Azure, using:
- **ADLS Gen2** for storage, with separate containers per layer
  (`landing`, `bronze`, `silver`, `gold`, `checkpoints`)
- **Azure Data Factory** for orchestration
- **Azure Databricks** (Premium tier, Unity Catalog enabled) for transformation
- **Azure Synapse Analytics (serverless SQL)** as the serving layer
- **Power BI** for the dashboard
- **Azure Key Vault** for centralized secret management

## Infrastructure & IaC

- Provisioned via a mix of Azure CLI scripts (in `infra/`) and manual portal
  steps (where CLI/UI naming-uniqueness constraints made the portal easier,
  e.g. Key Vault). Scripts are kept even for resources created manually, as a
  reproducibility record.
- Two script sets maintained in the repo: `infra/` (bash, for Linux/macOS/Git
  Bash users) and `infra-ps/` (PowerShell, since primary dev environment was
  Windows without WSL/Git Bash initially available).
- Region: `australiaeast` throughout, both for latency and as a deliberate
  data-residency talking point for an AU-market portfolio piece.
- Cost-control decisions made deliberately, not incidentally:
  - Synapse: serverless SQL only, no dedicated SQL pool (avoids flat hourly
    billing regardless of usage).
  - Databricks: Standard tier was the original plan, but Microsoft retired
    Standard tier for new workspaces (forced Premium-only, with mandatory
    Unity Catalog) partway through the build - Premium was adopted instead,
    since it wasn't optional.
  - Databricks clusters: single-node where possible, short auto-termination,
    "New job cluster" (ephemeral, per-run) for ADF-triggered production runs,
    "Existing interactive cluster" only during active development/debugging
    to avoid repeated cluster-startup overhead while iterating.
  - A NAT Gateway is auto-deployed by default when creating an Azure
    Databricks workspace with default VNet + secure cluster connectivity
    (a newer Azure networking default). This has an ongoing hourly cost and
    is not obviously visible unless you inspect the auto-created "managed
    resource group." Fixed by explicitly passing `--enable-no-public-ip false`
    at workspace creation to skip it.
  - Free-tier Azure subscription has a small regional vCPU quota (4 cores in
    this case). Combined with running 5 entities in true parallel, this
    caused cluster provisioning failures (`AZURE_QUOTA_EXCEEDED_EXCEPTION`).
    Resolved by switching the ADF ForEach loop to sequential execution rather
    than requesting a quota increase (which free/trial subscriptions
    frequently decline anyway).

## Storage Layout
landing/
transactions/ingestion_date=YYYY-MM-DD/transactions_data.csv
users/ingestion_date=YYYY-MM-DD/users_data.csv
cards/ingestion_date=YYYY-MM-DD/cards_data.csv
mcc_codes/ingestion_date=YYYY-MM-DD/mcc_codes.json
fraud_labels/ingestion_date=YYYY-MM-DD/train_fraud_labels.json
_config/entity_config.json
_config/silver_table_config.json
_config/gold_table_config.json
bronze/<entity>/ (External Delta tables)
silver/<table>/ (External Delta tables)
gold/<table>/ (External Delta tables)
- `ingestion_date=YYYY-MM-DD` partitioning applied even though this is a
  one-time historical load, specifically so the ADF pipeline is already
  written the way it would need to be for a genuine recurring feed. In a real
  production setup, this partition is written automatically by whichever
  process lands the file (the ingestion tool stamping the run date, or the
  source system's own delivery convention) - never manually chosen by an
  engineer, which is what I simulated by hand here in the absence of a live
  upstream source system.

## Medallion-Driven, Metadata-Driven Pipeline Design

Rather than hardcoding 5 (then 4, then 4) separate pipeline branches per
layer, each layer uses:
- **One generic Databricks notebook per layer** (`01_bronze_ingestion.py`,
  `02_silver_transformation.py`, `03_gold_aggregation.py`), parameterized by
  entity/table name, with dispatch logic routing to the right
  transform/aggregate function.
- **One JSON config file per layer** (`entity_config.json`,
  `silver_table_config.json`, `gold_table_config.json`) listing what to
  process, each with an `is_active` flag to allow disabling one entity
  without a pipeline change.
- **ADF Lookup activity** reads the config file and feeds a **ForEach**
  loop, rather than a hardcoded pipeline parameter array. This scales cleanly
  to many entities - adding entity #51 means editing one config file, not
  editing/republishing the pipeline.
- At larger real-world scale, this config would move from a JSON file to a
  proper control table (e.g. an Azure SQL or Synapse table), since a table
  supports things a flat file can't (an admin UI, `last_successful_run`
  tracking for incremental loads, foreign keys). A JSON file was the
  right-sized choice for this project's scale.

## Bronze Layer

- All columns kept as `String` type deliberately (a common "raw vault"
  pattern) - defers all real type casting to Silver, so a single malformed
  value doesn't break ingestion for an entire file.
- CSV entities read with explicit schemas, not `inferSchema` - avoids an
  extra full-file scan pass and protects against silent schema drift between
  runs.
- Audit/lineage columns on every table: `_ingested_at`, `_ingestion_date`,
  `_source_file`, `_batch_id`.
- The two JSON source files (`mcc_codes.json`, `train_fraud_labels.json`) are
  not row-oriented JSON - they're a flat dict and a nested dict respectively,
  where dict *keys* are data values, not column names. Spark's native
  `spark.read.json()` can't parse this shape correctly (it would try to turn
  every key into a column). Solved with Spark-native `from_json()` +
  `explode()` against an explicit `MapType` schema - fully distributed,
  avoids driver-side Python parsing entirely. (This was not the first
  approach tried - see "What Didn't Work" below.)

## Silver Layer

- Schema-qualified naming: `bronze.*`, `silver.*`, `gold.*` (Unity Catalog
  schemas), rather than distinguishing layers by folder/table name prefixes
  alone.
- Target tables: `dim_customer`, `dim_card`, `dim_mcc`, `fact_transactions`
  (star-schema style), each with a surrogate key (`_sk`) alongside the
  natural/business key, though joins within Silver still use business keys -
  surrogate-key joins were deferred to Gold to keep Silver's logic simpler.
- PII handling, decided explicitly rather than applying a blanket rule:
  - `cvv` - dropped entirely, not masked. This isn't a judgment call like the
    others: PCI-DSS prohibits storing CVV under any circumstances past
    initial authorization, even encrypted/hashed.
  - `card_number` - masked to last 4 digits only (`************1234`), with
    `card_number_last4` also kept as a separate, directly usable column.
  - `address` - originally planned to mask down to city/state/postcode for
    location-pattern fraud analysis, but the actual source data's `address`
    field turned out to be street-only with no embedded city/state/zip to
    extract, making that masking plan inapplicable. Decision reversed: kept
    unmasked, since latitude/longitude (already present as separate,
    rounded-precision columns) provide the actual location signal needed for
    fraud analysis, and a bare street address alone reveals less than
    originally assumed.
  - `latitude`/`longitude` - rounded to 2 decimal places (~1km precision) as
    a deliberate generalization, balancing usefulness for
    distance-from-home fraud features against precise-residence exposure.
- Currency-style string fields (`amount`, `yearly_income`, `credit_limit`,
  etc., stored as `"$xx.xx"` strings in the source) cleaned via a shared
  `clean_currency()` utility function rather than repeated inline logic.
- `_dq_flags` column (array of strings) added per table to hold multiple data
  quality issues per row (e.g. `missing_customer_id`, `invalid_amount`) -
  flags rather than silently drops rows, so bad data stays visible/auditable
  instead of being lost.
- Deduplication via `ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY
  _ingested_at DESC)`, keeping the latest ingested row per key.
- `fact_transactions` partitioned by `txn_year`/`txn_month` (derived from the
  transaction date), not by ingestion date - matches how this table would
  actually be queried downstream.
- `is_fraud` on `fact_transactions` is left `NULL` for transactions with no
  matching fraud label, rather than defaulting to `false` - an unlabelled
  transaction is a different thing from a confirmed non-fraud transaction,
  and defaulting to false would silently misrepresent that.
- Lineage columns: each Silver table carries forward the source Bronze
  table's `_batch_id` (as `_bronze_batch_id`), plus its own
  `_silver_batch_id`/`_silver_loaded_at`/etc, so a Silver row can be traced
  back to the exact Bronze ingestion run that produced it.
- Utility functions (`clean_currency`, `mask_card_number`, `round_coordinates`,
  `create_dq_flag`, `build_dq_flags`, `add_ingestion_metadata`) live in a
  shared, importable module (`notebooks/utils/transformation_utils.py`),
  synced via Databricks Repos (Git Folders) rather than pasted inline or
  sourced via `%run` - makes the logic testable and avoids re-execution
  overhead.

## Gold Layer

- Sources Silver tables only, never Bronze directly - a deliberate, strict
  medallion separation rule.
- 4 tables: `fraud_summary_by_state`, `fraud_summary_by_mcc`,
  `monthly_transaction_trends`, `customer_risk_profile`.
- `risk_tier` bucketing (Low/Medium/High based on fraud rate thresholds) uses
  simple, illustrative thresholds rather than researched business rules,
  since this is a demo project without a real business stakeholder defining
  risk appetite.
- Surrogate keys (`customer_sk`) are actually used for joins at this layer,
  unlike Silver, which stuck to business keys.

## Unity Catalog: External vs. Managed Tables

- All layers (Bronze, Silver, Gold) use **External** tables/locations, not
  Managed tables - a deliberate, consistent choice across the whole pipeline.
- Reasoning: Synapse serverless SQL (the serving layer for Power BI) reads
  Delta files directly by storage path - it doesn't go through Unity
  Catalog at all. Managed tables would mean an extra step to look up UC's
  internally-chosen storage path before Synapse could reach them. Staying
  External keeps one consistent access pattern across every layer and every
  downstream consumer.
- Requires: an Access Connector (Azure managed identity) with "Storage Blob
  Data Contributor" on the storage account, a Unity Catalog Storage
  Credential referencing it, and an External Location per container -
  standard UC governance model for pointing at self-managed storage paths.
- A separate `control` schema (Managed, not External - no outside consumer
  needs a direct file path to it) holds pipeline logging.

## Logging & Observability

- A `control.pipeline_logs` Delta table records one row per table per
  notebook run (not one row per internal code block - considered, but scaled
  back since Databricks' own job run history and stack traces already cover
  block-level detail; the log table's job is durable, cross-run, queryable
  history that native tooling doesn't retain long-term).
- Columns: `run_id`, `pipeline_layer`, `table_name`, `step_name`, `status`,
  `logged_at`, `row_count`, `error_message`, `notebook_name`, `triggered_by`,
  `load_type`, `job_id`, `databricks_run_id`.
- Row count sourced from the Delta write's own commit metadata
  (`DESCRIBE HISTORY ... numOutputRows`) rather than a separate `.count()`
  call, to avoid an unnecessary extra Spark action on large tables.
- Writes happen immediately per log call (not buffered and flushed once at
  the end), so a log entry survives even if the notebook crashes right
  after - a deliberate tradeoff of small-file/commit overhead in exchange for
  real-time visibility while debugging, considered acceptable at this
  project's actual run volume. `delta.autoOptimize.optimizeWrite` and
  `autoCompact` are enabled on the log table as a background safety net
  against small-file accumulation, without needing to change the write
  pattern itself.
- `job_id`/`databricks_run_id` sourced via `spark.conf.get(...)`, not the
  Databricks internal `CommandContext.tags()` API - the latter is blocked by
  Unity Catalog's process isolation security model on Assigned/Shared-access
  clusters and throws a `Py4JSecurityException`.
- Logging implemented on Bronze and Silver; not added to Gold for this
  iteration (a scope decision, not an oversight).

## Orchestration

- Three independently testable/debuggable ADF pipelines
  (`pl_landing_to_bronze`, `pl_bronze_to_silver`, `pl_silver_to_gold`), each
  runnable and debuggable on its own.
- A master orchestrator pipeline (`pl_master_orchestration`) chains all
  three via `Execute Pipeline` activities (with "wait on completion"
  enabled), driven by a single shared `p_run_date` parameter - keeps each
  layer's internal logic isolated while still giving a one-click full-chain
  run option.
- ADF linked services authenticate to Key Vault via ADF's system-assigned
  managed identity (RBAC role: Key Vault Secrets User) - no credentials
  stored in ADF itself. Storage and Databricks linked services in turn pull
  their actual secrets (storage account key, Databricks PAT) from Key Vault
  by reference, not by value.

## Secrets & Key Vault

- Key Vault uses the RBAC permission model (not the legacy Access Policies
  model) - the current Microsoft-recommended approach.
- Databricks reads secrets via a Key Vault-backed secret scope
  (`kv-finance-fraud-scope`). On an RBAC-model vault, this requires an
  explicit role assignment ("Key Vault Secrets User") granted to
  Databricks' own fixed, global Enterprise Application identity
  ("AzureDatabricks", App ID `2ff814a6-3304-4ab8-85cb-cd0e6f879c1d`, same
  across every Azure tenant) - this step is not automatic on RBAC vaults the
  way it is on legacy Access Policy vaults, and is easy to miss.
- Databricks Personal Access Token (PAT) used for the ADF-to-Databricks
  connection, generated with a 90-day expiry and "All APIs" scope (narrower
  scoping caused confusing partial failures, since ADF's Notebook activity
  touches the Jobs, Clusters, and Workspace APIs together).
- Known limitation, called out deliberately rather than hidden: PAT
  authentication ties the pipeline's identity to a specific human user and
  requires manual rotation. Production would use a Service Principal with
  OAuth (client credentials flow) instead - a non-human identity, shorter-
  lived tokens issued automatically, and support for automated secret
  rotation via Key Vault + Event Grid, none of which a human-tied PAT
  supports well.

## Serving Layer (Synapse) & Power BI

- Synapse serverless SQL only (no dedicated pool), consistent with the
  earlier cost-control decision.
- A `finance_fraud_serving` database with one view per Gold table, each
  wrapping an `OPENROWSET(..., FORMAT='DELTA')` query - gives Power BI a
  stable, named object to connect to instead of a raw storage path pasted
  into every query.
- Views authenticate to storage via a database-scoped credential backed by
  the Synapse workspace's own managed identity (granted "Storage Blob Data
  Reader" on the storage account) - required specifically because the
  Synapse SQL admin login (username/password) has no Azure AD identity of
  its own to pass through to storage; Entra ID login would have worked
  without this extra credential, but was not usable here (see Known
  Limitations).
- Power BI connects via SQL (Database) authentication against the
  serverless endpoint, with **Import** mode (not DirectQuery) - the Gold
  tables are small, pre-aggregated summaries, so Import gives faster report
  interaction with no ongoing dependency on Synapse being reachable every
  time the report opens.

## Known Limitations / Data Quality Issues (Not Fixed, Deliberately)

- `merchant_state` in the transactions source data is inconsistent -
  sometimes country-level values, sometimes state abbreviations - which made
  a state-level map visual in Power BI unusable (looked like transactions
  spanning the globe rather than a single country). A bar chart was used
  instead for the state-level view. Worth a `_dq_flags` entry
  (`inconsistent_state_format`) on `fact_transactions` in a future pass.
- `customer_risk_profile`'s null-handling for customers with zero
  transactions (their `fraud_rate_pct` comes back null after a left join,
  and the current `risk_tier` bucketing logic falls through to "High" on a
  null input rather than an explicit "No Activity" tier) was identified but
  not fixed - a known edge case, not an oversight.
- A similar unresolved null case exists on `fraud_summary_by_mcc` if an
  `mcc_code` appears in transactions but is missing from the `mcc_codes`
  reference file.

## Explicitly Deferred / Not Built

These weren't oversights - each was a scoped decision to stop at a
reasonable point for a portfolio project rather than gold-plate every layer:

- **Error/exception alerting via email**: designed (ADF Web Activity ->
  Azure Logic App -> Office 365 "Send an email" connector, branching on a
  `status` field in the payload, with separate success/failure email
  bodies), but not completed. The Office 365 connector's OAuth sign-in
  requires a work/school Microsoft account; the personal Microsoft account
  behind this Azure subscription hit an "unauthorized"/"can't sign in with
  personal account" error, the same limitation encountered with Synapse's
  Entra ID login and this account's Guest status in its own auto-created
  tenant. A production build (or a build under an organizational account)
  would complete this as designed; a personal-account fallback (e.g.
  SendGrid) was identified but not implemented.
- **Managed vs. External table tradeoff at Gold**: kept External for
  consistency, as noted above, though Managed tables (with UC's automatic
  Predictive Optimization) are increasingly the Microsoft-recommended
  default for layers with no external, non-UC consumer.
- **Serverless Databricks compute** for job execution: identified as a way
  to sidestep the VM-size/regional-stockout/vCPU-quota issues hit repeatedly
  during Bronze development, but not adopted mid-build given the
  in-progress cluster configuration already working; worth a future
  migration.
- **Unity Catalog storage credential model for Databricks-to-storage
  access**: the working setup uses the legacy `fs.azure.account.key...`
  Spark-conf approach for cluster-level file access (separate from the UC
  External Location/Storage Credential setup used for table registration
  itself) - a hybrid rather than a fully UC-native single mechanism. A
  cleaner setup would route cluster storage access through the same UC
  Storage Credential used for table registration.

## What Didn't Work (Worth Recording, Not Just the Final Answer)

A few dead ends during the build, kept here since the reasoning for avoiding
them again is as useful as the eventual fix:

- Parsing the two dict-shaped JSON source files via `.collect()` + Python
  `json.loads()` on the driver repeatedly hit Spark Connect's ~128MB gRPC
  message-size limit on the larger fraud-labels file, and separately caused
  driver out-of-memory crashes once the in-memory Python dict/list grew large
  enough. A pandas-based staging-file workaround was also tried and abandoned
  after hitting a Unity Catalog restriction on direct `/dbfs/...` FUSE-mount
  access from Shared-mode clusters. The eventual fix (Spark-native
  `from_json`+`explode`, see Bronze Layer notes above) avoids all of these
  because it never materializes the full dataset outside of Spark's own
  distributed engine.
- `spark.databricks.cluster.profile=singleNode` / `spark.master=local[*]` -
  the legacy way to declare a single-node cluster - actively conflicts with
  Unity Catalog's newer Access Mode setting and causes a cluster startup
  failure. Single-node clusters on UC-enabled workspaces should be
  configured via `Workers: 0` + an explicit Access Mode instead.