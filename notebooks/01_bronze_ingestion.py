# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion Notebook
# MAGIC Generic ingestion notebook - handles all 5 source entities via widget params (entity, ingestion_date, source_format). Called once per entity by the ForEach loop in pl_landing_to_bronze.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read Widgets (ADF Parameters)

# COMMAND ----------

from datetime import date

dbutils.widgets.text("entity", "transactions")
dbutils.widgets.text("ingestion_date", str(date.today()))
dbutils.widgets.text("source_format", "csv")

entity = dbutils.widgets.get("entity")
ingestion_date = dbutils.widgets.get("ingestion_date")
source_format = dbutils.widgets.get("source_format")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Validate Parameters

# COMMAND ----------

VALID_ENTITIES = {
    "transactions",
    "users",
    "cards",
    "mcc_codes",
    "fraud_labels"
}

if entity not in VALID_ENTITIES:
    raise ValueError(f"Unsupported entity: {entity}")

VALID_FORMATS = {"csv", "json"}

if source_format not in VALID_FORMATS:
    raise ValueError(f"Unsupported source format: {source_format}")

print(f"""
Entity           : {entity}
Ingestion Date   : {ingestion_date}
Source Format    : {source_format}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Build ADLS Gen 2 Paths

# COMMAND ----------

storage_account = "storagefinancialfraud"

landing_path = (
    f"abfss://landing@{storage_account}.dfs.core.windows.net/"
    f"{entity}/ingestion_date={ingestion_date}/"
)

bronze_path = (
    f"abfss://bronze@{storage_account}.dfs.core.windows.net/{entity}/"
)

print(landing_path)
print(bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating JSON structure to hold the config needed for dictonary related flat JSON file

# COMMAND ----------

from pyspark.sql.types import (
    MapType,
    StringType,
    StructType,
    StructField
)

JSON_CONFIG = {

    "fraud_labels": {
        "schema": StructType([
            StructField(
                "target",
                MapType(
                    StringType(),
                    StringType()
                ),
                True
            )
        ]),
        "root_column": "target",
        "key_column": "transaction_id",
        "value_column": "is_fraud_label"
    },


    "mcc_codes": {
        "schema": MapType(
            StringType(),
            StringType()
        ),
        "root_column": None,
        "key_column": "mcc_code",
        "value_column": "mcc_description"
    }
}

# COMMAND ----------

# MAGIC %md #### CSV entities: transactions, users, cards

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType

transactions_schema = StructType([
    StructField("id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("card_id", StringType(), True),
    StructField("amount", StringType(), True),        # "$xx.xx" format
    StructField("use_chip", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_city", StringType(), True),
    StructField("merchant_state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("mcc", StringType(), True),
    StructField("errors", StringType(), True),
])

users_schema = StructType([
    StructField("id", StringType(), True),
    StructField("current_age", StringType(), True),
    StructField("retirement_age", StringType(), True),
    StructField("birth_year", StringType(), True),
    StructField("birth_month", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("per_capita_income", StringType(), True),  # "$xx.xx" format
    StructField("yearly_income", StringType(), True),       # "$xx.xx" format
    StructField("total_debt", StringType(), True),          # "$xx.xx" format
    StructField("credit_score", StringType(), True),
    StructField("num_credit_cards", StringType(), True),
])

cards_schema = StructType([
    StructField("id", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("card_brand", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("card_number", StringType(), True),
    StructField("expires", StringType(), True),
    StructField("cvv", StringType(), True),
    StructField("has_chip", StringType(), True),
    StructField("num_cards_issued", StringType(), True),
    StructField("credit_limit", StringType(), True),        # "$xx.xx" format
    StructField("acct_open_date", StringType(), True),
    StructField("year_pin_last_changed", StringType(), True),
    StructField("card_on_dark_web", StringType(), True),
])

SCHEMA_MAP = {
    "transactions": transactions_schema,
    "users": users_schema,
    "cards": cards_schema,
}

def ingest_csv(entity_name: str):
    df = (
        spark.read
        .option("header", "true")
        .schema(SCHEMA_MAP[entity_name])
        .csv(landing_path)
    )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### JSON entities: mcc_codes, fraud_labels
# MAGIC mcc_codes.json -> flat dict, {mcc_code: description}
# MAGIC train_fraud_labels.json -> nested dict, {"target": {transaction_id: label}}

# COMMAND ----------

from pyspark.sql import functions as F


def ingest_json_dictionary(entity_name):

    json_file = [
        f.path
        for f in dbutils.fs.ls(landing_path)
        if f.path.endswith(".json")
    ][0]

    config = JSON_CONFIG[entity_name]

    raw_df = (
        spark.read
        .option("wholetext", "true")
        .text(json_file)
    )


    if config["root_column"]:

        # Nested JSON (fraud_labels)

        df = (
            raw_df
            .select(
                F.from_json(
                    F.col("value"),
                    config["schema"]
                ).alias("parsed")
            )
            .select(
                F.explode(
                    F.col(f"parsed.{config['root_column']}")
                )
                .alias(
                    config["key_column"],
                    config["value_column"]
                )
            )
        )


    else:

        # Flat JSON (mcc_codes)

        df = (
            raw_df
            .select(
                F.from_json(
                    F.col("value"),
                    config["schema"]
                ).alias("json_map")
            )
            .select(
                F.explode("json_map")
                .alias(
                    config["key_column"],
                    config["value_column"]
                )
            )
        )


    return df, json_file

# COMMAND ----------

# MAGIC %md #### Dispatch by entity + write to Bronze as Delta

# COMMAND ----------

if entity in JSON_CONFIG:
    df, json_file  = ingest_json_dictionary(entity)
    source_file_col = F.lit(json_file)
else:
    df = ingest_csv(entity)
    source_file_col = F.col("_metadata.file_path")

df_bronze = (
    df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_ingestion_date", F.lit(ingestion_date))
    .withColumn("_source_file", source_file_col)
    .withColumn("_batch_id", F.lit(f"{entity}_{ingestion_date}"))
)

# COMMAND ----------

row_count = df_bronze.count()

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(bronze_path)
)

print(f"Bronze write complete: {bronze_path}")
print(f"Rows written: {row_count}")

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: {entity} | rows={row_count}")