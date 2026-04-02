"""
revenue_report_for_completed_transactions: Filter the Olist orders dataset to include
only delivered orders, removing null order_ids, and partition output by year and month
of order_purchase_timestamp.
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
PARTITION_COLS: list[str] = ["year", "month"]

# Mapping from raw generic column names to semantic equivalents
# based on the known Olist orders schema.
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
            SparkSession.builder.appName(PIPELINE_NAME)
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            # Optimise small-file writes when partitioning
            .config("spark.databricks.delta.optimizeWrite.enabled", "true")
            .config("spark.databricks.delta.autoCompact.enabled", "true")
            # Allow schema evolution on overwrite
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        ).getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Transformation helpers
# ---------------------------------------------------------------------------
def read_source(spark: SparkSession, path: str) -> DataFrame:
    """
    Read the raw Olist orders CSV from S3.

    The source files use a header row; the inferred schema exposes columns
    as col0–col7 (all string), which are renamed in a subsequent step.

    Parameters
    ----------
    spark : SparkSession
        Active SparkSession.
    path : str
        S3 URI of the source CSV directory.

    Returns
    -------
    DataFrame
        Raw DataFrame with generic column names col0–col7.
    """
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "false")   # keep everything as string; cast explicitly later
        .option("multiLine", "false")
        .option("escape", '"')
        .csv(path)
    )


def rename_columns(df: DataFrame, rename_map: dict[str, str]) -> DataFrame:
    """
    Rename generic column names (col0–col7) to their semantic equivalents.

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
    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
        else:
            print(f"  [WARN] Expected column '{old_name}' not found in source — skipping rename.")
    return df


def filter_null_order_ids(df: DataFrame) -> DataFrame:
    """
    Remove rows where order_id is null or empty to ensure data quality.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        DataFrame with null/empty order_id rows removed.
    """
    return df.filter(
        F.col("order_id").isNotNull() & (F.trim(F.col("order_id")) != "")
    )


def filter_delivered_orders(df: DataFrame) -> DataFrame:
    """
    Keep only orders whose order_status equals 'delivered'.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        DataFrame containing only delivered orders.
    """
    return df.filter(F.col("order_status") == "delivered")


def cast_timestamp_columns(df: DataFrame) -> DataFrame:
    """
    Cast order_purchase_timestamp from string to TimestampType.

    Rows where the timestamp cannot be parsed will yield null rather than
    raising an exception (PySpark's default safe-cast behaviour).

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        DataFrame with order_purchase_timestamp as TimestampType.
    """
    return df.withColumn(
        "order_purchase_timestamp",
        F.to_timestamp(F.col("order_purchase_timestamp")),
    )


def enrich_partition_columns(df: DataFrame) -> DataFrame:
    """
    Derive 'year' and 'month' integer columns from order_purchase_timestamp.

    These columns are used as Delta Lake partition keys.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with order_purchase_timestamp as TimestampType.

    Returns
    -------
    DataFrame
        DataFrame enriched with 'year' and 'month' integer columns.
    """
    return df.withColumn(
        "year", F.year(F.col("order_purchase_timestamp"))
    ).withColumn(
        "month", F.month(F.col("order_purchase_timestamp"))
    )


def fill_nulls(df: DataFrame) -> DataFrame:
    """
    Apply graceful null handling across string columns.

    String columns that are null are filled with an empty string so that
    downstream consumers receive a consistent schema without unexpected nulls.
    Timestamp and integer columns are left as-is (nulls are meaningful there).

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        DataFrame with null strings replaced by empty strings.
    """
    string_cols = [
        field.name
        for field in df.schema.fields
        if str(field.dataType) == "StringType()"
    ]
    fill_map: dict[str, str] = {col: "" for col in string_cols}
    if fill_map:
        df = df.fillna(fill_map)
    return df


def sort_output(df: DataFrame) -> DataFrame:
    """
    Sort the DataFrame by year (asc) then month (asc) for consistent
    partition ordering and deterministic output.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        Sorted DataFrame.
    """
    return df.orderBy(
        F.col("year").asc(),
        F.col("month").asc(),
    )


def write_delta(df: DataFrame, path: str, partition_cols: list[str]) -> None:
    """
    Write the transformed DataFrame to Delta Lake in overwrite mode,
    partitioned by the specified columns.

    Parameters
    ----------
    df : DataFrame
        Transformed DataFrame ready for persistence.
    path : str
        S3 URI of the Delta Lake target location.
    partition_cols : list[str]
        Column names to use as partition keys.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(*partition_cols)
        .save(path)
    )


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------
def run() -> None:
    """
    Execute the revenue_report_for_completed_transactions ETL pipeline.

    Steps
    -----
    1. Initialise SparkSession with Delta Lake configuration.
    2. Read raw CSV source from S3.
    3. Rename generic columns to semantic names.
    4. Filter out null / empty order_ids.
    5. Filter to delivered orders only.
    6. Cast order_purchase_timestamp to TimestampType.
    7. Enrich with year / month partition columns.
    8. Fill remaining string nulls with empty strings.
    9. Sort by year, month.
    10. Write to Delta Lake (overwrite, partitioned by year/month).
    """
    # ------------------------------------------------------------------
    # Step 1 – SparkSession
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] Step 1/10 — Initialising SparkSession...")
    spark: SparkSession = create_spark_session()
    print(f"[{PIPELINE_NAME}] SparkSession created. Spark version: {spark.version}")

    # ------------------------------------------------------------------
    # Step 2 – Read source
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] Step 2/10 — Reading source CSV from: {SOURCE_PATH}")
    df_raw: DataFrame = read_source(spark, SOURCE_PATH)
    source_count: int = df_raw.count()
    print(f"[{PIPELINE_NAME}] Source rows read: {source_count:,}")
    print(f"[{PIPELINE_NAME}] Source schema: {df_raw.dtypes}")

    # ------------------------------------------------------------------
    # Step 3 – Rename columns
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] Step 3/10 — Renaming columns: {COLUMN_RENAME_MAP}")
    df_renamed: DataFrame = rename_columns(df_raw, COLUMN_RENAME_MAP)
    print(f"[{PIPELINE_NAME}] Columns after rename: {df_renamed.columns}")

    # ------------------------------------------------------------------
    # Step 4 – Filter null order_ids
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] Step 4/10 — Filtering null/empty order_ids...")
    df_no_nulls: DataFrame = filter_null_order_ids(df_renamed)
    after_null_filter: int = df_no_nulls.count()
    print(
        f"[{PIPELINE_NAME}] Rows after null order_id filter: {after_null_filter:,} "
        f"(removed {source_count - after_null_filter:,} rows)"
    )

    # ------------------------------------------------------------------
    # Step 5 – Filter delivered orders
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] Step 5/10 — Filtering to 'delivered' orders only...")
    df_delivered: DataFrame = filter_delivered_orders(df_no_nulls)
    after_status_filter: int = df_delivered.count()
    print(
        f"[{PIPELINE_NAME}] Rows after status filter: {after_status_filter:,} "
        f"(removed {after_null_filter - after_status_filter:,} non-delivered rows)"
    )

    # ------------------------------------------------------------------
    # Step 6 – Cast timestamp column
    # ------------------------------------------------------------------
    print(
        f"[{PIPELINE_NAME}] Step 6/10 — Casting 'order_purchase_timestamp' to TimestampType..."
    )
    df_cast: DataFrame = cast_timestamp_columns(df_delivered)
    # Warn if any timestamps failed to parse (resulted in null after cast)
    unparseable: int = df_cast.filter(F.col("order_purchase_timestamp").isNull()).count()
    if unparseable > 0:
        print(
            f"[{PIPELINE_NAME}] [WARN] {unparseable:,} rows have unparseable "
            "'order_purchase_timestamp' values — they will be null."
        )
    else:
        print(f"[{PIPELINE_NAME}] All timestamps parsed successfully.")

    # ------------------------------------------------------------------
    # Step 7 – Enrich with year / month
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] Step 7/10 — Deriving 'year' and 'month' partition columns...")
    df_enriched: DataFrame = enrich_partition_columns(df_cast)
    print(f"[{PIPELINE_NAME}] Schema after enrichment: {df_enriched.dtypes}")

    # ------------------------------------------------------------------
    # Step 8 – Fill nulls
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] Step 8/10 — Filling null string values with empty strings...")
    df_filled: DataFrame = fill_nulls(df_enriched)
    print(f"[{PIPELINE_NAME}] Null-fill complete.")

    # ------------------------------------------------------------------
    # Step 9 – Sort
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] Step 9/10 — Sorting output by year ASC, month ASC...")
    df_sorted: DataFrame = sort_output(df_filled)
    print(f"[{PIPELINE_NAME}] Sort applied.")

    # ------------------------------------------------------------------
    # Step 10 – Write to Delta Lake
    # ------------------------------------------------------------------
    print(
        f"[{PIPELINE_NAME}] Step 10/10 — Writing Delta Lake output to: {TARGET_PATH} "
        f"(partitioned by {PARTITION_COLS}, mode=overwrite)..."
    )
    write_delta(df_sorted, TARGET_PATH, PARTITION_COLS)
    print(f"[{PIPELINE_NAME}] Write complete. Final row count: {after_status_filter:,}")
    print(f"[{PIPELINE_NAME}] Pipeline finished successfully.")

    spark.stop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run()