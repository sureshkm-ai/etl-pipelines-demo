"""
revenue_report_for_completed_transactions: Filter the Olist orders dataset to include
only delivered orders, ensuring downstream revenue reports reflect completed transactions only.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from delta import configure_spark_with_delta_pip


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SOURCE_PATH: str = "s3://etl-agent-raw-prod/olist/orders/"
TARGET_PATH: str = "s3://etl-agent-processed-production/revenue_report_for_completed_transactions/"
PIPELINE_NAME: str = "revenue_report_for_completed_transactions"
PARTITION_COLS: list[str] = ["order_purchase_year", "order_purchase_month"]

RENAME_MAP: dict[str, str] = {
    "col0": "order_id",
    "col1": "customer_id",
    "col2": "order_status",
    "col3": "order_purchase_timestamp",
    "col4": "order_approved_at",
    "col5": "order_delivered_carrier_date",
    "col6": "order_delivered_customer_date",
    "col7": "order_estimated_delivery_date",
}

TIMESTAMP_COLUMNS: list[str] = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------
def create_spark_session() -> SparkSession:
    """
    Create and return a SparkSession configured for Delta Lake.

    Returns
    -------
    SparkSession
        A fully configured SparkSession with Delta Lake extensions enabled.
    """
    builder = (
        SparkSession.builder.appName(PIPELINE_NAME)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Optimise small-file writes for partitioned Delta tables
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        # Shuffle partitions tuned for moderate dataset size
        .config("spark.sql.shuffle.partitions", "200")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Transformation helpers
# ---------------------------------------------------------------------------
def read_source(spark: SparkSession) -> DataFrame:
    """
    Read raw CSV data from S3 using the inferred schema (col0–col7).

    Parameters
    ----------
    spark : SparkSession
        Active SparkSession.

    Returns
    -------
    DataFrame
        Raw DataFrame with columns col0 through col7 as strings.
    """
    print(f"[{PIPELINE_NAME}] Reading source CSV from: {SOURCE_PATH}")
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")   # keep all columns as string; we cast explicitly
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "false")
        .csv(SOURCE_PATH)
    )
    print(f"[{PIPELINE_NAME}] Source rows read: {df.count():,}")
    print(f"[{PIPELINE_NAME}] Source schema:")
    df.printSchema()
    return df


def rename_columns(df: DataFrame) -> DataFrame:
    """
    Rename generic column names (col0–col7) to their semantic equivalents
    based on the known Olist orders schema.

    Parameters
    ----------
    df : DataFrame
        DataFrame with raw column names.

    Returns
    -------
    DataFrame
        DataFrame with semantically meaningful column names.
    """
    print(f"[{PIPELINE_NAME}] Renaming columns: {RENAME_MAP}")
    for old_name, new_name in RENAME_MAP.items():
        df = df.withColumnRenamed(old_name, new_name)
    print(f"[{PIPELINE_NAME}] Columns after rename: {df.columns}")
    return df


def cast_timestamp_columns(df: DataFrame) -> DataFrame:
    """
    Cast date/timestamp columns from string to TimestampType.

    Null-safe: rows with unparseable timestamps will produce null values
    rather than raising exceptions, preserving all rows for downstream
    null-handling steps.

    Parameters
    ----------
    df : DataFrame
        DataFrame with renamed string columns.

    Returns
    -------
    DataFrame
        DataFrame with timestamp columns cast to TimestampType.
    """
    print(f"[{PIPELINE_NAME}] Casting timestamp columns: {TIMESTAMP_COLUMNS}")
    for col_name in TIMESTAMP_COLUMNS:
        df = df.withColumn(col_name, F.col(col_name).cast(TimestampType()))
    print(f"[{PIPELINE_NAME}] Schema after cast:")
    df.printSchema()
    return df


def filter_delivered_orders(df: DataFrame) -> DataFrame:
    """
    Keep only rows where order_status equals 'delivered'.

    Parameters
    ----------
    df : DataFrame
        DataFrame with all order statuses.

    Returns
    -------
    DataFrame
        DataFrame containing only delivered orders.
    """
    print(f"[{PIPELINE_NAME}] Filtering rows where order_status = 'delivered'")
    df_filtered = df.filter(F.col("order_status") == "delivered")
    delivered_count = df_filtered.count()
    print(f"[{PIPELINE_NAME}] Rows after filter (delivered only): {delivered_count:,}")
    return df_filtered


def drop_null_order_ids(df: DataFrame) -> DataFrame:
    """
    Drop rows where order_id is null to ensure referential integrity
    in downstream revenue reports.

    Parameters
    ----------
    df : DataFrame
        DataFrame that may contain null order_id values.

    Returns
    -------
    DataFrame
        DataFrame with no null order_id values.
    """
    before_count = df.count()
    print(f"[{PIPELINE_NAME}] Dropping rows with null order_id (rows before: {before_count:,})")
    df_clean = df.dropna(subset=["order_id"])
    after_count = df_clean.count()
    dropped = before_count - after_count
    print(f"[{PIPELINE_NAME}] Rows dropped due to null order_id: {dropped:,}")
    print(f"[{PIPELINE_NAME}] Rows after null drop: {after_count:,}")
    return df_clean


def enrich_partition_columns(df: DataFrame) -> DataFrame:
    """
    Extract year and month from order_purchase_timestamp to support
    efficient partitioned writes and downstream partition pruning.

    Parameters
    ----------
    df : DataFrame
        DataFrame with order_purchase_timestamp as TimestampType.

    Returns
    -------
    DataFrame
        DataFrame enriched with order_purchase_year and order_purchase_month columns.
    """
    print(f"[{PIPELINE_NAME}] Enriching DataFrame with partition columns: {PARTITION_COLS}")
    df_enriched = (
        df.withColumn("order_purchase_year", F.year(F.col("order_purchase_timestamp")))
          .withColumn("order_purchase_month", F.month(F.col("order_purchase_timestamp")))
    )
    print(f"[{PIPELINE_NAME}] Partition column value distribution (year/month):")
    (
        df_enriched.groupBy("order_purchase_year", "order_purchase_month")
        .count()
        .orderBy("order_purchase_year", "order_purchase_month")
        .show(50, truncate=False)
    )
    return df_enriched


def sort_by_purchase_timestamp(df: DataFrame) -> DataFrame:
    """
    Sort the DataFrame by order_purchase_timestamp in ascending order
    for consistent, deterministic output ordering.

    Parameters
    ----------
    df : DataFrame
        Enriched and filtered DataFrame.

    Returns
    -------
    DataFrame
        DataFrame sorted by order_purchase_timestamp ascending.
    """
    print(f"[{PIPELINE_NAME}] Sorting by order_purchase_timestamp ASC")
    return df.orderBy(F.col("order_purchase_timestamp").asc())


def write_delta(df: DataFrame) -> None:
    """
    Write the transformed DataFrame to Delta Lake in overwrite mode,
    partitioned by order_purchase_year and order_purchase_month.

    Parameters
    ----------
    df : DataFrame
        Final transformed DataFrame ready for persistence.
    """
    print(f"[{PIPELINE_NAME}] Writing output to Delta Lake: {TARGET_PATH}")
    print(f"[{PIPELINE_NAME}] Partition columns: {PARTITION_COLS}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(*PARTITION_COLS)
        .save(TARGET_PATH)
    )
    print(f"[{PIPELINE_NAME}] Delta write complete.")


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------
def run() -> None:
    """
    Execute the revenue_report_for_completed_transactions ETL pipeline.

    Pipeline steps
    --------------
    1. Read raw CSV from S3 (col0–col7 schema).
    2. Rename columns to semantic Olist orders schema.
    3. Cast string date columns to TimestampType.
    4. Filter to delivered orders only.
    5. Drop rows with null order_id.
    6. Enrich with year/month partition columns.
    7. Sort by order_purchase_timestamp ascending.
    8. Write partitioned Delta table to S3 (overwrite).
    """
    print(f"[{PIPELINE_NAME}] ========== Pipeline START ==========")

    spark = create_spark_session()
    print(f"[{PIPELINE_NAME}] SparkSession created. Spark version: {spark.version}")

    try:
        # Step 1 – Read
        df_raw = read_source(spark)

        # Step 2 – Rename
        df_renamed = rename_columns(df_raw)

        # Step 3 – Cast
        df_cast = cast_timestamp_columns(df_renamed)

        # Step 4 – Filter
        df_filtered = filter_delivered_orders(df_cast)

        # Step 5 – Fill / drop nulls
        df_no_nulls = drop_null_order_ids(df_filtered)

        # Step 6 – Enrich (partition columns)
        df_enriched = enrich_partition_columns(df_no_nulls)

        # Step 7 – Sort
        df_sorted = sort_by_purchase_timestamp(df_enriched)

        # Step 8 – Write
        write_delta(df_sorted)

        print(f"[{PIPELINE_NAME}] ========== Pipeline COMPLETE ==========")

    except Exception as exc:  # noqa: BLE001
        print(f"[{PIPELINE_NAME}] PIPELINE FAILED with error: {exc}")
        raise

    finally:
        spark.stop()
        print(f"[{PIPELINE_NAME}] SparkSession stopped.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run()