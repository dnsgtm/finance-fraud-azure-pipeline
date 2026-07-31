# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Setup
# MAGIC One-time setup: creates bronze/silver/gold schemas, an external location per
# MAGIC medallion container backed by the workspace's auto-provisioned Access Connector,
# MAGIC and registers the 5 existing Bronze Delta paths as named tables.
# MAGIC Run manually, once. Not called by ADF. Safe to re-run - everything is IF NOT EXISTS.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "cl_finance_fraud_dev")
dbutils.widgets.text("storage_account", "storagefinancialfraud")
dbutils.widgets.text("access_connector_resource_id", "/subscriptions/f020a269-b819-4681-b75e-0da216b23434/resourceGroups/databricks-rg-dbw-finance-fraud-dev-1jp4e8or5fryt/providers/Microsoft.Databricks/accessConnectors/unity-catalog-access-connector")

CATALOG = dbutils.widgets.get("catalog_name")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
ACCESS_CONNECTOR_ID = dbutils.widgets.get("access_connector_resource_id")

if not ACCESS_CONNECTOR_ID:
    raise ValueError(
        "access_connector_resource_id widget is empty. "
        "Get it via: az databricks access-connector list --resource-group <managed-rg> -o table"
    )

print(f"catalog={CATALOG} | storage_account={STORAGE_ACCOUNT}")

# COMMAND ----------

# MAGIC %md ### 1. Create schemas

# COMMAND ----------

for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"Schema ready: {CATALOG}.{schema}")

# COMMAND ----------

# MAGIC %md ### 2. Create storage credential (backed by the Access Connector)

# COMMAND ----------

spark.sql(f"""
CREATE STORAGE CREDENTIAL IF NOT EXISTS cred_finance_fraud_storage
WITH (AZURE_MANAGED_IDENTITY '{ACCESS_CONNECTOR_ID}')
""")
print("Storage credential ready: cred_finance_fraud_storage")

# COMMAND ----------

# MAGIC %md ### 3. Create external locations (one per medallion container)

# COMMAND ----------

CONTAINERS = ["landing", "bronze", "silver", "gold"]

for container in CONTAINERS:
    location_url = f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    spark.sql(f"""
    CREATE EXTERNAL LOCATION IF NOT EXISTS loc_{container}
    URL '{location_url}'
    WITH (STORAGE CREDENTIAL cred_finance_fraud_storage)
    """)
    print(f"External location ready: loc_{container} -> {location_url}")

# COMMAND ----------

# MAGIC %md ### 4. Register existing Bronze Delta paths as named tables

# COMMAND ----------

BRONZE_ENTITIES = ["transactions", "users", "cards", "mcc_codes", "fraud_labels"]

for entity in BRONZE_ENTITIES:
    location_url = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/{entity}/"
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.{entity}
    USING DELTA
    LOCATION '{location_url}'
    """)
    print(f"Table ready: {CATALOG}.bronze.{entity}")

# COMMAND ----------

# MAGIC %md ### 5. Verify

# COMMAND ----------

spark.sql(f"SHOW TABLES IN {CATALOG}.bronze").show(truncate=False)

for entity in BRONZE_ENTITIES:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.bronze.{entity}").collect()[0]["cnt"]
    print(f"{entity}: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Create control schema under cl_finance_fraud_dev catalog to store log details

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS cl_finance_fraud_dev.control;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS cl_finance_fraud_dev.control.pipeline_logs (
# MAGIC     run_id STRING,
# MAGIC     pipeline_layer STRING,
# MAGIC     table_name STRING,
# MAGIC     step_name STRING,
# MAGIC     status STRING,
# MAGIC     logged_at TIMESTAMP,
# MAGIC     row_count INT,
# MAGIC     error_message STRING,
# MAGIC     notebook_name STRING,
# MAGIC     triggered_by STRING,
# MAGIC     load_type STRING,
# MAGIC     job_id STRING,
# MAGIC     databricks_run_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );