# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transformation Notebook
# MAGIC Generic transformation notebook - handles all 4 silver tables via widget
# MAGIC params (silver_table, load_date). Called once per table by the ForEach
# MAGIC loop in pl_bronze_to_silver. Sources shared logic from utils/transformation_utils.py.

# COMMAND ----------

# MAGIC %md #### 1. Parameters

# COMMAND ----------

dbutils.widgets.text("silver_table", "dim_customer")
dbutils.widgets.text("load_date", "2026-07-16")

silver_table = dbutils.widgets.get("silver_table")
load_date = dbutils.widgets.get("load_date")

CATALOG = "cl_finance_fraud_dev"

print(f"silver_table={silver_table} | load_date={load_date}")

# COMMAND ----------

# MAGIC %md #### 2. Imports / utils

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.transformation_utils import (
    clean_currency,
    mask_card_number,
    round_coordinates,
    create_dq_flag,
    build_dq_flags,
    add_ingestion_metadata,
)

# COMMAND ----------

# MAGIC %md #### 3. Shared helper - dedup on business key

# COMMAND ----------

def dedup_on_key(df, key_column):
    window = Window.partitionBy(key_column).orderBy(F.col("_ingested_at").desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

# COMMAND ----------

# MAGIC %md #### 4. Transform functions

# COMMAND ----------

def transform_customer():
    bronze = spark.table(f"{CATALOG}.bronze.users")
    bronze = dedup_on_key(bronze, "id")

    df = bronze.select(
        F.monotonically_increasing_id().alias("customer_sk"),
        F.col("id").alias("customer_id"),
        F.col("current_age").cast("int"),
        F.col("retirement_age").cast("int"),
        F.col("birth_year").cast("int"),
        F.col("birth_month").cast("int"),
        F.col("gender"),
        F.concat_ws(
            " ",
            F.regexp_extract(F.col("address"), r",\s*(.*)$", 1)
        ).alias("city_state_postcode"),
        round_coordinates("latitude").alias("latitude"),
        round_coordinates("longitude").alias("longitude"),
        clean_currency("per_capita_income").alias("per_capita_income"),
        clean_currency("yearly_income").alias("yearly_income"),
        clean_currency("total_debt").alias("total_debt"),
        F.col("credit_score").cast("int"),
        F.col("num_credit_cards").cast("int"),
        F.col("_batch_id").alias("_bronze_batch_id"),
    )

    df = df.withColumn(
        "_dq_flags",
        build_dq_flags(
            create_dq_flag(F.col("customer_id").isNull(), F.lit("missing_customer_id")),
            create_dq_flag(F.col("yearly_income").isNull(), F.lit("invalid_yearly_income")),
            create_dq_flag(F.col("credit_score").isNull(), F.lit("missing_credit_score")),
        )
    )

    return add_ingestion_metadata(df, load_date, "dim_customer")

# COMMAND ----------

def transform_card():
    bronze = spark.table(f"{CATALOG}.bronze.cards")
    bronze = dedup_on_key(bronze, "id")

    df = bronze.select(
        F.monotonically_increasing_id().alias("card_sk"),
        F.col("id").alias("card_id"),
        F.col("client_id").alias("customer_id"),
        F.col("card_brand"),
        F.col("card_type"),
        mask_card_number("card_number").alias("card_number_masked"),
        F.substring(F.col("card_number"), -4, 4).alias("card_number_last4"),
        F.col("has_chip").cast("boolean"),
        F.col("card_on_dark_web").cast("boolean"),
        F.col("num_cards_issued").cast("int"),
        clean_currency("credit_limit").alias("credit_limit"),
        F.to_date(F.col("acct_open_date")).alias("acct_open_date"),
        F.col("year_pin_last_changed").cast("int"),
        F.col("expires"),
        F.col("_batch_id").alias("_bronze_batch_id"),
    )

    df = df.withColumn(
        "_dq_flags",
        build_dq_flags(
            create_dq_flag(F.col("card_id").isNull(), F.lit("missing_card_id")),
            create_dq_flag(F.col("credit_limit").isNull(), F.lit("invalid_credit_limit")),
        )
    )

    return add_ingestion_metadata(df, load_date, "dim_card")

# COMMAND ----------

def transform_mcc():
    bronze = spark.table(f"{CATALOG}.bronze.mcc_codes")
    bronze = dedup_on_key(bronze, "mcc_code")

    df = bronze.select(
        F.monotonically_increasing_id().alias("mcc_sk"),
        F.col("mcc_code"),
        F.col("mcc_description"),
        F.col("_batch_id").alias("_bronze_batch_id"),
    )

    return add_ingestion_metadata(df, load_date, "dim_mcc")

# COMMAND ----------

def transform_transactions():
    txn = spark.table(f"{CATALOG}.bronze.transactions")
    txn = dedup_on_key(txn, "id")

    labels = spark.table(f"{CATALOG}.bronze.fraud_labels")

    df = txn.select(
        F.monotonically_increasing_id().alias("transaction_sk"),
        F.col("id").alias("transaction_id"),
        F.to_timestamp(F.col("date")).alias("transaction_datetime"),
        F.col("client_id").alias("customer_id"),
        F.col("card_id"),
        clean_currency("amount").alias("amount"),
        F.col("use_chip").alias("channel"),
        F.col("merchant_id"),
        F.col("merchant_city"),
        F.col("merchant_state"),
        F.col("zip"),
        F.col("mcc").alias("mcc_code"),
        F.when(F.col("errors").isNotNull(), F.lit(True)).otherwise(F.lit(False)).alias("has_error"),
        F.col("errors").alias("error_detail"),
        F.col("_batch_id").alias("_bronze_batch_id_transactions"),
    )

    df = df.join(
        labels.select(
            F.col("transaction_id"),
            F.when(F.col("is_fraud_label") == "Yes", True)
             .when(F.col("is_fraud_label") == "No", False)
             .otherwise(F.lit(None)).alias("is_fraud")
            F.col("_batch_id").alias("_bronze_batch_id_fraud_labels"),
        ),
        on="transaction_id",
        how="left",
    )

    df = df.withColumn(
        "_dq_flags",
        build_dq_flags(
            create_dq_flag(F.col("amount").isNull(), F.lit("invalid_amount")),
            create_dq_flag(F.col("transaction_datetime").isNull(), F.lit("invalid_datetime")),
            create_dq_flag(F.col("customer_id").isNull(), F.lit("missing_customer_id")),
        )
    )

    return add_ingestion_metadata(df, load_date, "fact_transactions")

# COMMAND ----------

# MAGIC %md #### 5. Dispatch

# COMMAND ----------

TRANSFORM_MAP = {
    "dim_customer": transform_customer,
    "dim_card": transform_card,
    "dim_mcc": transform_mcc,
    "fact_transactions": transform_transactions,
}

if silver_table not in TRANSFORM_MAP:
    raise ValueError(f"Unsupported silver_table: {silver_table}")

df_silver = TRANSFORM_MAP[silver_table]()

row_count = df_silver.count()
print(f"{silver_table}: {row_count:,} rows")

# COMMAND ----------

# MAGIC %md #### 6. Write

# COMMAND ----------

silver_path = f"abfss://silver@storagefinancialfraud.dfs.core.windows.net/{silver_table}/"

write = df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true")

if silver_table == "fact_transactions":
    write = (
        df_silver
        .withColumn("txn_year", F.year("transaction_datetime"))
        .withColumn("txn_month", F.month("transaction_datetime"))
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .partitionBy("txn_year", "txn_month")
    )

write.save(silver_path)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.{silver_table}
USING DELTA
LOCATION '{silver_path}'
""")

spark.sql(f"OPTIMIZE {CATALOG}.silver.{silver_table}")

print(f"Silver write complete: {silver_path}")

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: {silver_table} | rows={row_count}")