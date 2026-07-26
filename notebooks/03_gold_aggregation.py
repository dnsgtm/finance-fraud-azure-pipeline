# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Aggregation Notebook
# MAGIC Generic aggregation notebook - handles all 4 gold tables via widget
# MAGIC params (gold_table, load_date). Called once per table by the ForEach
# MAGIC loop in pl_silver_to_gold. Sources only from Silver, never Bronze.

# COMMAND ----------

# MAGIC %md #### 1. Parameters

# COMMAND ----------

dbutils.widgets.text("gold_table", "fraud_summary_by_state")
dbutils.widgets.text("load_date", "2026-07-16")

gold_table = dbutils.widgets.get("gold_table")
load_date = dbutils.widgets.get("load_date")

CATALOG = "cl_finance_fraud_dev"

print(f"gold_table={gold_table} | load_date={load_date}")

# COMMAND ----------

# MAGIC %md #### 2. Imports

# COMMAND ----------

from pyspark.sql import functions as F

from utils.transformation_utils import add_ingestion_metadata

# COMMAND ----------

# MAGIC %md #### 3. Shared helper - risk tier bucketing

# COMMAND ----------

def risk_tier_expr(fraud_rate_col):
    return (
        F.when(fraud_rate_col == 0, F.lit("Low"))
        .when(fraud_rate_col <= 5, F.lit("Medium"))
        .otherwise(F.lit("High"))
    )

# COMMAND ----------

# MAGIC %md #### 4. Aggregation functions

# COMMAND ----------

def build_fraud_summary_by_state():
    txn = spark.table(f"{CATALOG}.silver.fact_transactions")

    df = (
        txn.groupBy("merchant_state")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0)).alias("total_fraud_transactions"),
            F.sum("amount").alias("total_amount"),
            F.sum(F.when(F.col("is_fraud") == True, F.col("amount")).otherwise(0.0)).alias("total_fraud_amount"),
        )
        .withColumn(
            "fraud_rate_pct",
            F.round(F.col("total_fraud_transactions") / F.col("total_transactions") * 100, 2)
        )
    )

    return add_ingestion_metadata(df, load_date, gold_table, "gold")

# COMMAND ----------

def build_fraud_summary_by_mcc():
    txn = spark.table(f"{CATALOG}.silver.fact_transactions")
    mcc = spark.table(f"{CATALOG}.silver.dim_mcc")

    df = (
        txn.groupBy("mcc_code")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0)).alias("total_fraud_transactions"),
            F.sum("amount").alias("total_amount"),
            F.sum(F.when(F.col("is_fraud") == True, F.col("amount")).otherwise(0.0)).alias("total_fraud_amount"),
        )
        .withColumn(
            "fraud_rate_pct",
            F.round(F.col("total_fraud_transactions") / F.col("total_transactions") * 100, 2)
        )
        .join(mcc.select("mcc_code", "mcc_description"), on="mcc_code", how="left")
    )

    return add_ingestion_metadata(df, load_date, gold_table, "gold")

# COMMAND ----------

def build_monthly_transaction_trends():
    txn = spark.table(f"{CATALOG}.silver.fact_transactions")

    df = (
        txn.groupBy("txn_year", "txn_month")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0)).alias("total_fraud_transactions"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_transaction_amount"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn(
            "fraud_rate_pct",
            F.round(F.col("total_fraud_transactions") / F.col("total_transactions") * 100, 2)
        )
        .withColumn("avg_transaction_amount", F.round(F.col("avg_transaction_amount"), 2))
    )

    return add_ingestion_metadata(df, load_date, gold_table, "gold")

# COMMAND ----------

def build_customer_risk_profile():
    txn = spark.table(f"{CATALOG}.silver.fact_transactions")
    cust = spark.table(f"{CATALOG}.silver.dim_customer")
    card = spark.table(f"{CATALOG}.silver.dim_card")

    txn_agg = txn.groupBy("customer_id").agg(
        F.count("*").alias("total_transactions"),
        F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0)).alias("total_fraud_transactions"),
        F.sum("amount").alias("total_spend"),
    ).withColumn(
        "fraud_rate_pct",
        F.round(F.col("total_fraud_transactions") / F.col("total_transactions") * 100, 2)
    )

    card_agg = card.groupBy("customer_id").agg(
        F.count("*").alias("num_cards")
    )

    df = (
        cust.select("customer_sk", "customer_id", "current_age", "credit_score", "yearly_income")
        .join(txn_agg, on="customer_id", how="left")
        .join(card_agg, on="customer_id", how="left")
        .withColumn("risk_tier", risk_tier_expr(F.col("fraud_rate_pct")))
    )

    return add_ingestion_metadata(df, load_date, gold_table, "gold")

# COMMAND ----------

# MAGIC %md #### 5. Dispatch

# COMMAND ----------

GOLD_MAP = {
    "fraud_summary_by_state": build_fraud_summary_by_state,
    "fraud_summary_by_mcc": build_fraud_summary_by_mcc,
    "monthly_transaction_trends": build_monthly_transaction_trends,
    "customer_risk_profile": build_customer_risk_profile,
}

if gold_table not in GOLD_MAP:
    raise ValueError(f"Unsupported gold_table: {gold_table}")

df_gold = GOLD_MAP[gold_table]()

row_count = df_gold.count()
print(f"{gold_table}: {row_count:,} rows")

# COMMAND ----------

# MAGIC %md #### 6. Write

# COMMAND ----------

gold_path = f"abfss://gold@storagefinancialfraud.dfs.core.windows.net/{gold_table}/"

(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_path)
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.gold.{gold_table}
USING DELTA
LOCATION '{gold_path}'
""")

spark.sql(f"OPTIMIZE {CATALOG}.gold.{gold_table}")

print(f"Gold write complete: {gold_path}")

# COMMAND ----------

dbutils.notebook.exit(f"SUCCESS: {gold_table} | rows={row_count}")