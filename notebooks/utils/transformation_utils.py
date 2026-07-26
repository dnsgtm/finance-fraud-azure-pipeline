from pyspark.sql import functions as F


def clean_currency(column_name):
    """
    Removes currency symbols and commas
    Converts value to double
    """

    return F.regexp_replace(F.col(column_name), r"[$,]", "").cast("double")


def mask_card_number(column_name):
    """
    Keeps only last 4 digits. Returns null if input is null.
    """

    return F.when(F.col(column_name).isNull(), F.lit(None)).otherwise(
        F.concat(F.lit("************"), F.substring(F.col(column_name), -4, 4))
    )


def round_coordinates(column_name):
    """
    Reduces latitude/longitude precision
    """

    return F.round(F.col(column_name).cast("double"), 2)


def create_dq_flag(condition, flag_name):

    return F.when(condition, flag_name)


def build_dq_flags(*flag_expressions):
    """
    Takes multiple F.when(...) expressions (each returning a flag name or null)
    and combines them into a single array column, dropping nulls.
    """
    return F.array_compact(F.array(*flag_expressions))


def add_ingestion_metadata(df, load_date, table_name):
    return (
        df
        .withColumn("_silver_loaded_at", F.current_timestamp())
        .withColumn("_silver_load_date", F.lit(load_date))
        .withColumn("_silver_table", F.lit(table_name))
        .withColumn("_silver_batch_id", F.lit(f"{table_name}_{load_date}"))
    )