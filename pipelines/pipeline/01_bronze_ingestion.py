# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion Notebook
# MAGIC Generic ingestion notebook - handles all 5 source entities via widget
# MAGIC params (entity, ingestion_date, source_format). Called once per entity
# MAGIC by the ForEach loop in pl_landing_to_bronze.

# COMMAND ----------
from datetime import date

dbutils.widgets.text("entity", "transactions")
dbutils.widgets.text("ingestion_date", str(date.today()))
dbutils.widgets.text("source_format", "csv")

entity = dbutils.widgets.get("entity")
ingestion_date = dbutils.widgets.get("ingestion_date")
source_format = dbutils.widgets.get("source_format")

storage_account = "storagefinancialfraud"
landing_path = f"abfss://landing@{storage_account}.dfs.core.windows.net/{entity}/ingestion_date={ingestion_date}/"
bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/{entity}/"

print(f"entity={entity} | source_format={source_format}")
print(f"landing_path={landing_path}")
print(f"bronze_path={bronze_path}")

# COMMAND ----------

# MAGIC %md ### CSV entities: transactions, users, cards

# COMMAND ----------

from pyspark.sql import functions as F

def ingest_csv(entity_name: str):
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(landing_path)
    )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### JSON entities: mcc_codes, fraud_labels
# MAGIC mcc_codes.json -> flat dict, {mcc_code: description}
# MAGIC train_fraud_labels.json -> nested dict, {"target": {transaction_id: label}}

# COMMAND ----------

import json as pyjson

def ingest_mcc_codes():
    raw_files = dbutils.fs.ls(landing_path)
    json_file = [f.path for f in raw_files if f.path.endswith(".json")][0]
    content = "".join([row.value for row in spark.read.text(json_file).collect()])
    data = pyjson.loads(content)
    rows = [(str(code), str(description)) for code, description in data.items()]
    df = spark.createDataFrame(rows, schema=["mcc_code", "mcc_description"])
    return df

def ingest_fraud_labels():
    raw_files = dbutils.fs.ls(landing_path)
    json_file = [f.path for f in raw_files if f.path.endswith(".json")][0]
    content = "".join([row.value for row in spark.read.text(json_file).collect()])
    data = pyjson.loads(content)
    label_dict = data["target"]
    rows = [(str(txn_id), str(label)) for txn_id, label in label_dict.items()]
    df = spark.createDataFrame(rows, schema=["transaction_id", "is_fraud_label"])
    return df

# COMMAND ----------

# MAGIC %md ### Dispatch by entity + write to Bronze as Delta

# COMMAND ----------

if entity == "mcc_codes":
    df = ingest_mcc_codes()
elif entity == "fraud_labels":
    df = ingest_fraud_labels()
else:
    df = ingest_csv(entity)

df_bronze = (
    df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_ingestion_date", F.lit(ingestion_date))
    .withColumn("_source_file", F.lit(landing_path))
    .withColumn("_batch_id", F.lit(f"{entity}_{ingestion_date}"))
)

row_count = df_bronze.count()
print(f"Rows read for {entity}: {row_count:,}")

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(bronze_path)
)

print(f"Bronze write complete: {bronze_path}")

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: {entity} | rows={row_count}")