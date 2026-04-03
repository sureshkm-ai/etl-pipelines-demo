"""
rfm_customer_segmentation_iphone17_campaign: Filter the Olist orders dataset to include
only delivered orders, removing nulls in order_id, and partition output by year and month
of order_purchase_timestamp.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from delta import configure_spark_with_delta_pip


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PIPELINE_NAME: str = "rfm_customer_segmentation_iphone17_campaign"
SOURCE_PATH: str = "s3://etl-agent-raw-prod/olist/orders/"
TARGET_PATH: str = "s3://etl-agent-processed-production/rfm_customer_segmentation_iphone17_campaign/"
PARTITION_COLS: list[str] = ["year", "month"]

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
    spark: SparkSession = (
        configure_spark_with_delta_pip(
            SparkSession.builder
            .appName(PIPELINE_NAME)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            # Optimise small-file writes when partitioning
            .config("spark.databricks.delta.optimizeWrite.enabled", "true")
            .config("spark.databricks.delta.autoCompact.enabled", "true")
            # Shuffle partitions tuned for moderate dataset size
            .config("spark.sql.shuffle.partitions", "200")
        ).getOrCreate()
    )
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
        S3 URI of the source CSV directory.

    Returns
    -------
    DataFrame
        Raw DataFrame with inferred schema (all strings per source schema).
    """
    print(f"[{PIPELINE_NAME}] Reading source CSV from: {path}")
    df: DataFrame = (
        spark.read
        .option("header", "false")   # Source columns are col0–col7 (no header)
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(path)
    )
    # Assign generic Spark-generated column names to match the inferred schema
    # The source has no header; Spark names them _c0–_c7 by default.
    # We rename _c0–_c7 → col0–col7 to align with the spec before semantic rename.
    generic_rename: dict[str, str] = {f"_c{i}": f"col{i}" for i in range(8)}
    for old_name, new_name in generic_rename.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    print(f"[{PIPELINE_NAME}] Source schema: {df.dtypes}")
    print(f"[{PIPELINE_NAME}] Source row count: {df.count()}")
    return df


def rename_columns(df: DataFrame, rename_map: dict[str, str]) -> DataFrame:
    """
    Rename generic column names to their semantic equivalents.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with generic column names (col0–col7).
    rename_map : dict[str, str]
        Mapping of {old_name: new_name}.

    Returns
    -------
    DataFrame
        DataFrame with semantically named columns.
    """
    print(f"[{PIPELINE_NAME}] Renaming columns: {rename_map}")
    for old_col, new_col in rename_map.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)
        else:
            print(
                f"[{PIPELINE_NAME}] WARNING: Column '{old_col}' not found in DataFrame "
                f"— skipping rename to '{new_col}'."
            )
    print(f"[{PIPELINE_NAME}] Columns after rename: {df.columns}")
    return df


def filter_non_null_order_id(df: DataFrame) -> DataFrame:
    """
    Remove rows where order_id is null or empty.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        DataFrame with null/empty order_id rows removed.
    """
    print(f"[{PIPELINE_NAME}] Filtering: removing rows with null order_id...")
    before: int = df.count()
    df_filtered: DataFrame = df.filter(
        F.col("order_id").isNotNull() & (F.trim(F.col("order_id")) != "")
    )
    after: int = df_filtered.count()
    print(f"[{PIPELINE_NAME}] Rows removed (null order_id): {before - after} | Remaining: {after}")
    return df_filtered


def filter_delivered_orders(df: DataFrame) -> DataFrame:
    """
    Keep only orders with order_status = 'delivered'.

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
    before: int = df.count()
    df_filtered: DataFrame = df.filter(F.col("order_status") == "delivered")
    after: int = df_filtered.count()
    print(
        f"[{PIPELINE_NAME}] Rows removed (non-delivered): {before - after} | Remaining: {after}"
    )
    return df_filtered


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Remove duplicate rows based on order_id (primary key deduplication).

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        DataFrame with duplicate order_id rows removed, keeping first occurrence.
    """
    print(f"[{PIPELINE_NAME}] Deduplicating on order_id...")
    before: int = df.count()
    df_deduped: DataFrame = df.dropDuplicates(["order_id"])
    after: int = df_deduped.count()
    print(f"[{PIPELINE_NAME}] Duplicate rows removed: {before - after} | Remaining: {after}")
    return df_deduped


def fill_nulls(df: DataFrame) -> DataFrame:
    """
    Fill null values in non-critical string columns with a safe default.

    Columns order_id and order_status are already guaranteed non-null by
    upstream filters. Timestamp columns are left as null intentionally to
    avoid fabricating business data.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        DataFrame with null string columns filled.
    """
    print(f"[{PIPELINE_NAME}] Filling null values in string columns...")
    nullable_string_cols: list[str] = [
        "customer_id",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    fill_map: dict[str, str] = {col: "UNKNOWN" for col in nullable_string_cols if col in df.columns}
    df_filled: DataFrame = df.fillna(fill_map)
    print(f"[{PIPELINE_NAME}] Null fill applied to columns: {list(fill_map.keys())}")
    return df_filled


def cast_timestamp_columns(df: DataFrame) -> DataFrame:
    """
    Cast order_purchase_timestamp from string to TimestampType.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        DataFrame with order_purchase_timestamp cast to TimestampType.
    """
    print(f"[{PIPELINE_NAME}] Casting order_purchase_timestamp to TimestampType...")
    df_cast: DataFrame = df.withColumn(
        "order_purchase_timestamp",
        F.to_timestamp(F.col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss"),
    )
    null_ts_count: int = df_cast.filter(F.col("order_purchase_timestamp").isNull()).count()
    if null_ts_count > 0:
        print(
            f"[{PIPELINE_NAME}] WARNING: {null_ts_count} rows have unparseable "
            f"order_purchase_timestamp — these will produce null year/month partition values."
        )
    return df_cast


def enrich_partition_columns(df: DataFrame) -> DataFrame:
    """
    Derive 'year' and 'month' integer columns from order_purchase_timestamp.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with order_purchase_timestamp as TimestampType.

    Returns
    -------
    DataFrame
        DataFrame enriched with 'year' and 'month' integer columns.
    """
    print(f"[{PIPELINE_NAME}] Deriving partition columns: year, month...")
    df_enriched: DataFrame = (
        df
        .withColumn("year", F.year(F.col("order_purchase_timestamp")))
        .withColumn("month", F.month(F.col("order_purchase_timestamp")))
    )
    print(f"[{PIPELINE_NAME}] Partition column sample:")
    df_enriched.select("order_purchase_timestamp", "year", "month").show(5, truncate=False)
    return df_enriched


def sort_output(df: DataFrame) -> DataFrame:
    """
    Sort the DataFrame by year (asc) and month (asc) for consistent partitioned output.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        Sorted DataFrame.
    """
    print(f"[{PIPELINE_NAME}] Sorting output by year ASC, month ASC...")
    df_sorted: DataFrame = df.orderBy(
        F.col("year").asc(),
        F.col("month").asc(),
    )
    return df_sorted


def write_output(df: DataFrame, path: str, partition_cols: list[str]) -> None:
    """
    Write the transformed DataFrame to S3 as Delta Lake format,
    partitioned by year and month.

    Parameters
    ----------
    df : DataFrame
        Transformed DataFrame ready for output.
    path : str
        Target S3 URI.
    partition_cols : list[str]
        List of column names to partition by.
    """
    print(f"[{PIPELINE_NAME}] Writing output to: {path}")
    print(f"[{PIPELINE_NAME}] Partition columns: {partition_cols}")
    final_row_count: int = df.count()
    print(f"[{PIPELINE_NAME}] Final row count to write: {final_row_count}")

    (
        df.write
        .format("delta")
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
    Execute the rfm_customer_segmentation_iphone17_campaign ETL pipeline.

    Pipeline steps:
        1. Read raw CSV from S3
        2. Rename generic columns to semantic names
        3. Filter null order_id rows
        4. Filter non-delivered orders
        5. Deduplicate on order_id
        6. Fill nulls in non-critical columns
        7. Cast order_purchase_timestamp to TimestampType
        8. Enrich with year/month partition columns
        9. Sort by year, month
        10. Write to Delta Lake partitioned by year/month
    """
    print(f"[{PIPELINE_NAME}] ========== Pipeline START ==========")

    # ── 1. SparkSession ──────────────────────────────────────────────────────
    spark: SparkSession = create_spark_session()
    print(f"[{PIPELINE_NAME}] SparkSession created. Spark version: {spark.version}")

    try:
        # ── 2. Read source ────────────────────────────────────────────────────
        df_raw: DataFrame = read_source(spark, SOURCE_PATH)

        # ── 3. Rename columns ─────────────────────────────────────────────────
        df_renamed: DataFrame = rename_columns(df_raw, COLUMN_RENAME_MAP)

        # ── 4. Filter: remove null order_id ───────────────────────────────────
        df_no_null_id: DataFrame = filter_non_null_order_id(df_renamed)

        # ── 5. Filter: delivered orders only ──────────────────────────────────
        df_delivered: DataFrame = filter_delivered_orders(df_no_null_id)

        # ── 6. Deduplicate ────────────────────────────────────────────────────
        df_deduped: DataFrame = deduplicate(df_delivered)

        # ── 7. Fill nulls ─────────────────────────────────────────────────────
        df_filled: DataFrame = fill_nulls(df_deduped)

        # ── 8. Cast timestamp ─────────────────────────────────────────────────
        df_cast: DataFrame = cast_timestamp_columns(df_filled)

        # ── 9. Enrich with partition columns ──────────────────────────────────
        df_enriched: DataFrame = enrich_partition_columns(df_cast)

        # ── 10. Sort ──────────────────────────────────────────────────────────
        df_sorted: DataFrame = sort_output(df_enriched)

        # ── 11. Write ─────────────────────────────────────────────────────────
        write_output(df_sorted, TARGET_PATH, PARTITION_COLS)

        print(f"[{PIPELINE_NAME}] ========== Pipeline COMPLETE ==========")

    except Exception as exc:
        print(f"[{PIPELINE_NAME}] FATAL ERROR: {exc}")
        raise
    finally:
        spark.stop()
        print(f"[{PIPELINE_NAME}] SparkSession stopped.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()