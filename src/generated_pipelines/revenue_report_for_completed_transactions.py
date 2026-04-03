"""
revenue_report_for_completed_transactions: Filter the Olist orders dataset to include
only delivered orders so that downstream revenue reports reflect completed transactions only.
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

RENAME_MAPPINGS: dict[str, str] = {
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
        # Optimise wide transformations
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Transformation helpers
# ---------------------------------------------------------------------------

def read_source(spark: SparkSession) -> DataFrame:
    """
    Read the raw Olist orders CSV from S3.

    Parameters
    ----------
    spark : SparkSession
        Active SparkSession.

    Returns
    -------
    DataFrame
        Raw DataFrame with generic column names (col0–col7).
    """
    print(f"[{PIPELINE_NAME}] Reading source CSV from: {SOURCE_PATH}")
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")   # all columns land as string per spec
        .option("multiLine", "false")
        .option("encoding", "UTF-8")
        .load(SOURCE_PATH)
    )
    print(f"[{PIPELINE_NAME}] Source rows loaded: {df.count()}")
    print(f"[{PIPELINE_NAME}] Source schema:")
    df.printSchema()
    return df


def rename_columns(df: DataFrame) -> DataFrame:
    """
    Rename generic col0–col7 columns to their semantic Olist orders names.

    Parameters
    ----------
    df : DataFrame
        DataFrame with raw column names.

    Returns
    -------
    DataFrame
        DataFrame with semantically named columns.
    """
    print(f"[{PIPELINE_NAME}] Renaming columns using Olist orders schema mappings...")
    for old_name, new_name in RENAME_MAPPINGS.items():
        df = df.withColumnRenamed(old_name, new_name)
    print(f"[{PIPELINE_NAME}] Column rename complete. Columns: {df.columns}")
    return df


def cast_timestamp_columns(df: DataFrame) -> DataFrame:
    """
    Cast date/timestamp columns from string to TimestampType.

    Null-safe: rows with unparseable timestamps will produce null values
    rather than raising an exception, preserving downstream null handling.

    Parameters
    ----------
    df : DataFrame
        DataFrame with string-typed timestamp columns.

    Returns
    -------
    DataFrame
        DataFrame with timestamp columns cast to TimestampType.
    """
    print(f"[{PIPELINE_NAME}] Casting timestamp columns: {TIMESTAMP_COLUMNS}")
    for col_name in TIMESTAMP_COLUMNS:
        df = df.withColumn(col_name, F.col(col_name).cast(TimestampType()))
    print(f"[{PIPELINE_NAME}] Timestamp casting complete.")
    return df


def filter_delivered_orders(df: DataFrame) -> DataFrame:
    """
    Keep only rows where order_status equals 'delivered'.

    Parameters
    ----------
    df : DataFrame
        DataFrame containing all order statuses.

    Returns
    -------
    DataFrame
        DataFrame restricted to delivered orders.
    """
    print(f"[{PIPELINE_NAME}] Filtering: order_status = 'delivered'...")
    df_filtered = df.filter(F.col("order_status") == "delivered")
    delivered_count = df_filtered.count()
    print(f"[{PIPELINE_NAME}] Rows after delivered filter: {delivered_count}")
    return df_filtered


def filter_non_null_order_id(df: DataFrame) -> DataFrame:
    """
    Exclude rows where order_id is null to ensure referential integrity.

    Parameters
    ----------
    df : DataFrame
        DataFrame that may contain null order_id values.

    Returns
    -------
    DataFrame
        DataFrame with null order_id rows removed.
    """
    print(f"[{PIPELINE_NAME}] Filtering: removing rows with null order_id...")
    df_filtered = df.filter(F.col("order_id").isNotNull())
    print(f"[{PIPELINE_NAME}] Rows after null order_id filter: {df_filtered.count()}")
    return df_filtered


def fill_null_values(df: DataFrame) -> DataFrame:
    """
    Apply graceful null handling across the DataFrame.

    Strategy:
    - String columns: fill nulls with empty string '' to avoid downstream
      string operation failures.
    - Timestamp columns: left as null (no sensible default exists for dates).

    Parameters
    ----------
    df : DataFrame
        DataFrame potentially containing null values.

    Returns
    -------
    DataFrame
        DataFrame with string nulls filled.
    """
    print(f"[{PIPELINE_NAME}] Applying null fill strategy for string columns...")
    string_cols = [
        field.name
        for field in df.schema.fields
        if str(field.dataType) == "StringType()"
    ]
    if string_cols:
        fill_map = {col_name: "" for col_name in string_cols}
        df = df.fillna(fill_map)
        print(f"[{PIPELINE_NAME}] Filled nulls in string columns: {string_cols}")
    else:
        print(f"[{PIPELINE_NAME}] No string columns requiring null fill found.")
    return df


def enrich_partition_columns(df: DataFrame) -> DataFrame:
    """
    Derive order_purchase_year and order_purchase_month from order_purchase_timestamp
    to support partitioned writes.

    Parameters
    ----------
    df : DataFrame
        DataFrame with a valid order_purchase_timestamp column.

    Returns
    -------
    DataFrame
        DataFrame enriched with order_purchase_year and order_purchase_month columns.
    """
    print(f"[{PIPELINE_NAME}] Deriving partition columns: order_purchase_year, order_purchase_month...")
    df = df.withColumn(
        "order_purchase_year", F.year(F.col("order_purchase_timestamp"))
    ).withColumn(
        "order_purchase_month", F.month(F.col("order_purchase_timestamp"))
    )
    print(f"[{PIPELINE_NAME}] Partition columns derived successfully.")
    return df


def sort_output(df: DataFrame) -> DataFrame:
    """
    Sort the output DataFrame by order_purchase_year ASC, order_purchase_month ASC
    for consistent and predictable ordering.

    Note: A global sort on large datasets is expensive. This sort is applied
    within each partition via sortWithinPartitions for production scalability.
    For a strict global sort, replace with df.orderBy(...).

    Parameters
    ----------
    df : DataFrame
        Enriched and filtered DataFrame.

    Returns
    -------
    DataFrame
        Sorted DataFrame.
    """
    print(f"[{PIPELINE_NAME}] Sorting output by order_purchase_year ASC, order_purchase_month ASC...")
    df_sorted = df.sortWithinPartitions(
        F.col("order_purchase_year").asc(),
        F.col("order_purchase_month").asc(),
    )
    print(f"[{PIPELINE_NAME}] Sort applied.")
    return df_sorted


def write_output(df: DataFrame) -> None:
    """
    Write the transformed DataFrame to the target S3 path using Delta Lake
    with overwrite mode, partitioned by order_purchase_year and order_purchase_month.

    Parameters
    ----------
    df : DataFrame
        Final transformed DataFrame ready for persistence.
    """
    print(f"[{PIPELINE_NAME}] Writing output to Delta Lake at: {TARGET_PATH}")
    print(f"[{PIPELINE_NAME}] Partition columns: {PARTITION_COLS}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(*PARTITION_COLS)
        .save(TARGET_PATH)
    )
    print(f"[{PIPELINE_NAME}] Write complete.")


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

def run() -> None:
    """
    Execute the revenue_report_for_completed_transactions ETL pipeline.

    Pipeline steps:
        1. Read raw CSV source (col0–col7 schema).
        2. Rename columns to semantic Olist orders names.
        3. Cast string timestamp columns to TimestampType.
        4. Filter to delivered orders only.
        5. Remove rows with null order_id.
        6. Fill remaining string nulls gracefully.
        7. Enrich with year/month partition columns.
        8. Sort output for consistent ordering.
        9. Write to Delta Lake (overwrite, partitioned).
    """
    print(f"[{PIPELINE_NAME}] ========== Pipeline START ==========")

    # Step 0 – Initialise Spark
    print(f"[{PIPELINE_NAME}] Initialising SparkSession with Delta Lake config...")
    spark = create_spark_session()
    print(f"[{PIPELINE_NAME}] SparkSession created. Spark version: {spark.version}")

    # Step 1 – Read source
    df_raw = read_source(spark)

    # Step 2 – Rename columns
    df_renamed = rename_columns(df_raw)

    # Step 3 – Cast timestamp columns
    df_cast = cast_timestamp_columns(df_renamed)

    # Step 4 – Filter: delivered orders only
    df_delivered = filter_delivered_orders(df_cast)

    # Step 5 – Filter: remove null order_id
    df_no_null_id = filter_non_null_order_id(df_delivered)

    # Step 6 – Fill null values gracefully
    df_filled = fill_null_values(df_no_null_id)

    # Step 7 – Enrich with partition columns
    df_enriched = enrich_partition_columns(df_filled)

    # Step 8 – Sort output
    df_sorted = sort_output(df_enriched)

    # Step 9 – Write to Delta Lake
    write_output(df_sorted)

    print(f"[{PIPELINE_NAME}] ========== Pipeline COMPLETE ==========")
    spark.stop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()