from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F

CATALOG = "cl_finance_fraud_dev"
LOG_TABLE = f"{CATALOG}.control.pipeline_logs"

LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), True),
    StructField("pipeline_layer", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("step_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("row_count", IntegerType(), True),
    StructField("error_message", StringType(), True),
    StructField("notebook_name", StringType(), True),
    StructField("triggered_by", StringType(), True),
    StructField("load_type", StringType(), True),
    StructField("job_id", StringType(), True),
    StructField("databricks_run_id", StringType(), True),
])


def get_run_context(spark):
    """
    Pulls Databricks job/run context via Spark conf - safe on Unity Catalog
    process-isolated clusters, unlike the internal CommandContext API.
    """

    job_id = spark.conf.get("spark.databricks.job.id", None)
    run_id = spark.conf.get("spark.databricks.job.runId", None)

    return {
        "job_id": job_id if job_id else "MANUAL",
        "databricks_run_id": run_id if run_id else "MANUAL",
        "triggered_by": "ADF" if job_id else "MANUAL",
    }


def get_last_write_row_count(spark, full_table_name):
    """
    Reads row count from the Delta transaction log's own commit metadata,
    instead of running a separate .count() action.
    """

    history = spark.sql(f"DESCRIBE HISTORY {full_table_name} LIMIT 1").collect()[0]
    metrics = history["operationMetrics"] or {}
    count = metrics.get("numOutputRows")
    return int(count) if count is not None else None


def log_step(spark, run_id, pipeline_layer, table_name, step_name, status,
             notebook_name, load_type="overwrite", error_message=None, row_count=None):

    ctx = get_run_context(spark)

    log_df = spark.createDataFrame(
        [(
            run_id, pipeline_layer, table_name, step_name, status, row_count,
            error_message, notebook_name, ctx["triggered_by"], load_type,
            ctx["job_id"], ctx["databricks_run_id"],
        )],
        schema=LOG_SCHEMA,
    ).withColumn("logged_at", F.current_timestamp())

    log_df.write.format("delta").mode("append").saveAsTable(LOG_TABLE)