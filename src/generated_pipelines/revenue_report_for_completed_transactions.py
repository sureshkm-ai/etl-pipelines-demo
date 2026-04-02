"""
revenue_report_for_completed_transactions: Filter the Olist orders dataset to include only
delivered orders, ensuring no null order_ids, partitioned by year and month of
order_purchase_timestamp.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from delta import configure_spark_with_delta_pip


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PIPELINE_NAME: str = "revenue_report_for_completed_transactions"
SOURCE_PATH: str = "s3://etl-agent-raw-prod/olist/orders/"
TARGET_PATH: str = "s3://etl-agent-processed-production/revenue_report_for_completed_transactions/"
PARTITION_COLS: list[str] = ["order_purchase_year", "order_purchase_month"]

COLUMN_RENAME_MAP: dict[str, str] = {
    "col0": "order_id",
    "col1": "customer_id",
    "col2": "order_status",
    "col3": "order_purchase_timestamp",
    "col4": "order_approved_at",
    "col5": "order_delivered_carrier_date",
    "col6": "order_delivered_customer_date",
    "col7": "order_estimated_delivery_date",
}


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
        # Recommended Delta / S3 performance settings
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Transformation helpers
# ---------------------------------------------------------------------------
def read_source(spark: SparkSession, path: str) -> DataFrame:
    """
    Read raw CSV data from S3.

    Parameters
    ----------
    spark : SparkSession
        Active SparkSession.
    path : str
        S3 URI pointing to the source CSV files.

    Returns
    -------
    DataFrame
        Raw DataFrame with inferred schema (all string columns col0–col7).
    """
    print(f"[{PIPELINE_NAME}] Reading source CSV from: {path}")
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")   # keep all cols as string; cast explicitly later
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .csv(path)
    )
    print(f"[{PIPELINE_NAME}] Source rows loaded: {df.count()}")
    print(f"[{PIPELINE_NAME}] Source schema:")
    df.printSchema()
    return df


def rename_columns(df: DataFrame, rename_map: dict[str, str]) -> DataFrame:
    """
    Rename generic column names (col0–col7) to their semantic equivalents
    based on the known Olist orders schema.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with generic column names.
    rename_map : dict[str, str]
        Mapping of {old_name: new_name}.

    Returns
    -------
    DataFrame
        DataFrame with semantically named columns.
    """
    print(f"[{PIPELINE_NAME}] Renaming columns: {rename_map}")
    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
        else:
            print(
                f"[{PIPELINE_NAME}] WARNING: Column '{old_name}' not found in source; "
                "skipping rename."
            )
    return df


def filter_non_null_order_ids(df: DataFrame) -> DataFrame:
    """
    Remove rows where order_id is null or empty to ensure data quality.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        DataFrame with non-null order_id rows only.
    """
    print(f"[{PIPELINE_NAME}] Filtering: removing rows with null order_id...")
    before = df.count()
    # Guard against both SQL NULL and empty-string values
    df_filtered = df.filter(
        F.col("order_id").isNotNull() & (F.trim(F.col("order_id")) != "")
    )
    after = df_filtered.count()
    print(f"[{PIPELINE_NAME}] Rows removed (null order_id): {before - after}")
    return df_filtered


def filter_delivered_orders(df: DataFrame) -> DataFrame:
    """
    Keep only orders with order_status = 'delivered' to reflect completed transactions.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        DataFrame containing only delivered orders.
    """
    print(f"[{PIPELINE_NAME}] Filtering: keeping only order_status = 'delivered'...")
    before = df.count()
    df_filtered = df.filter(F.col("order_status") == "delivered")
    after = df_filtered.count()
    print(f"[{PIPELINE_NAME}] Rows removed (non-delivered): {before - after}")
    print(f"[{PIPELINE_NAME}] Delivered orders retained: {after}")
    return df_filtered


def cast_timestamp_columns(df: DataFrame) -> DataFrame:
    """
    Cast order_purchase_timestamp from string to TimestampType to enable
    year/month partitioning.  Rows where the cast fails will produce NULL
    (safe cast behaviour via try_cast equivalent using F.to_timestamp).

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        DataFrame with order_purchase_timestamp as TimestampType.
    """
    print(
        f"[{PIPELINE_NAME}] Casting 'order_purchase_timestamp' to TimestampType..."
    )
    # to_timestamp returns NULL on parse failure — safer than direct cast()
    df = df.withColumn(
        "order_purchase_timestamp",
        F.to_timestamp(F.col("order_purchase_timestamp")),
    )
    null_ts_count = df.filter(F.col("order_purchase_timestamp").isNull()).count()
    if null_ts_count > 0:
        print(
            f"[{PIPELINE_NAME}] WARNING: {null_ts_count} rows have unparseable "
            "'order_purchase_timestamp' and will be NULL after cast."
        )
    return df


def fill_null_timestamps(df: DataFrame) -> DataFrame:
    """
    Handle null order_purchase_timestamp values gracefully by filling with a
    sentinel timestamp (1970-01-01 00:00:00) so partitioning does not fail.
    These rows can be investigated downstream.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        DataFrame with no null order_purchase_timestamp values.
    """
    sentinel = "1970-01-01 00:00:00"
    print(
        f"[{PIPELINE_NAME}] Filling null 'order_purchase_timestamp' with sentinel: {sentinel}"
    )
    df = df.withColumn(
        "order_purchase_timestamp",
        F.coalesce(
            F.col("order_purchase_timestamp"),
            F.to_timestamp(F.lit(sentinel)),
        ),
    )
    return df


def enrich_partition_columns(df: DataFrame) -> DataFrame:
    """
    Derive order_purchase_year and order_purchase_month from
    order_purchase_timestamp for partitioning purposes.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with order_purchase_timestamp as TimestampType.

    Returns
    -------
    DataFrame
        DataFrame enriched with order_purchase_year and order_purchase_month columns.
    """
    print(
        f"[{PIPELINE_NAME}] Deriving partition columns: "
        "'order_purchase_year', 'order_purchase_month'..."
    )
    df = df.withColumn(
        "order_purchase_year", F.year(F.col("order_purchase_timestamp"))
    ).withColumn(
        "order_purchase_month", F.month(F.col("order_purchase_timestamp"))
    )
    return df


def sort_output(df: DataFrame) -> DataFrame:
    """
    Sort the output DataFrame by order_purchase_year ASC, order_purchase_month ASC
    for readability and deterministic output ordering.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        Sorted DataFrame.
    """
    print(
        f"[{PIPELINE_NAME}] Sorting by 'order_purchase_year' ASC, "
        "'order_purchase_month' ASC..."
    )
    df = df.orderBy(
        F.col("order_purchase_year").asc(),
        F.col("order_purchase_month").asc(),
    )
    return df


def write_output(df: DataFrame, path: str, partition_cols: list[str]) -> None:
    """
    Write the transformed DataFrame to the target S3 path in Delta format,
    partitioned by the specified columns using overwrite mode.

    Parameters
    ----------
    df : DataFrame
        Transformed DataFrame ready for persistence.
    path : str
        Target S3 URI.
    partition_cols : list[str]
        List of column names to partition the output by.
    """
    print(f"[{PIPELINE_NAME}] Writing output to: {path}")
    print(f"[{PIPELINE_NAME}] Partition columns: {partition_cols}")
    print(f"[{PIPELINE_NAME}] Final row count: {df.count()}")

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(*partition_cols)
        .save(path)
    )
    print(f"[{PIPELINE_NAME}] Write complete.")


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------
def run() -> None:
    """
    Execute the revenue_report_for_completed_transactions ETL pipeline.

    Steps
    -----
    1. Create SparkSession with Delta Lake configuration.
    2. Read raw CSV source data from S3.
    3. Rename generic columns to semantic names.
    4. Filter out null / empty order_ids.
    5. Filter to delivered orders only.
    6. Cast order_purchase_timestamp to TimestampType.
    7. Fill any remaining null timestamps with a sentinel value.
    8. Derive year/month partition columns.
    9. Sort output for readability.
    10. Write partitioned Delta table to S3.
    """
    print(f"[{PIPELINE_NAME}] ========== Pipeline START ==========")

    # Step 1 — SparkSession
    print(f"[{PIPELINE_NAME}] Step 1: Creating SparkSession...")
    spark = create_spark_session()
    print(f"[{PIPELINE_NAME}] SparkSession created. App ID: {spark.sparkContext.applicationId}")

    # Step 2 — Read source
    print(f"[{PIPELINE_NAME}] Step 2: Reading source data...")
    df_raw = read_source(spark, SOURCE_PATH)

    # Step 3 — Rename columns
    print(f"[{PIPELINE_NAME}] Step 3: Renaming columns...")
    df_renamed = rename_columns(df_raw, COLUMN_RENAME_MAP)

    # Step 4 — Filter null order_ids
    print(f"[{PIPELINE_NAME}] Step 4: Filtering null order_ids...")
    df_no_nulls = filter_non_null_order_ids(df_renamed)

    # Step 5 — Filter delivered orders
    print(f"[{PIPELINE_NAME}] Step 5: Filtering delivered orders...")
    df_delivered = filter_delivered_orders(df_no_nulls)

    # Step 6 — Cast timestamp
    print(f"[{PIPELINE_NAME}] Step 6: Casting timestamp column...")
    df_cast = cast_timestamp_columns(df_delivered)

    # Step 7 — Fill null timestamps
    print(f"[{PIPELINE_NAME}] Step 7: Filling null timestamps...")
    df_filled = fill_null_timestamps(df_cast)

    # Step 8 — Enrich with partition columns
    print(f"[{PIPELINE_NAME}] Step 8: Enriching with partition columns...")
    df_enriched = enrich_partition_columns(df_filled)

    # Step 9 — Sort output
    print(f"[{PIPELINE_NAME}] Step 9: Sorting output...")
    df_sorted = sort_output(df_enriched)

    # Step 10 — Write output
    print(f"[{PIPELINE_NAME}] Step 10: Writing output...")
    write_output(df_sorted, TARGET_PATH, PARTITION_COLS)

    print(f"[{PIPELINE_NAME}] ========== Pipeline COMPLETE ==========")
    spark.stop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run()