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

TIMESTAMP_COLUMNS: list[str] = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

FILTER_CONDITION: str = "order_status = 'delivered' AND order_id IS NOT NULL"
SORT_COLUMNS: list[str] = ["order_purchase_year", "order_purchase_month"]


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------
def create_spark_session() -> SparkSession:
    """
    Create and return a SparkSession configured for Delta Lake.

    Returns
    -------
    SparkSession
        A fully configured SparkSession with Delta Lake extensions.
    """
    builder = (
        SparkSession.builder.appName(PIPELINE_NAME)
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
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Transformation helpers
# ---------------------------------------------------------------------------
def read_source(spark: SparkSession, path: str) -> DataFrame:
    """
    Read raw CSV data from S3 into a DataFrame.

    Parameters
    ----------
    spark : SparkSession
        Active SparkSession.
    path : str
        S3 URI of the source CSV directory.

    Returns
    -------
    DataFrame
        Raw DataFrame with inferred string columns (col0–col7).
    """
    print(f"[{PIPELINE_NAME}] Reading source CSV from: {path}")
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")   # All columns arrive as string per schema
        .option("nullValue", "")
        .option("mode", "PERMISSIVE")
        .load(path)
    )
    row_count = df.count()
    print(f"[{PIPELINE_NAME}] Source rows loaded: {row_count:,}")
    print(f"[{PIPELINE_NAME}] Source schema:")
    df.printSchema()
    return df


def rename_columns(df: DataFrame, rename_map: dict[str, str]) -> DataFrame:
    """
    Rename raw positional columns (col0–col7) to semantic business names.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with generic column names.
    rename_map : dict[str, str]
        Mapping of {old_name: new_name}.

    Returns
    -------
    DataFrame
        DataFrame with renamed columns.
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
    print(f"[{PIPELINE_NAME}] Columns after rename: {df.columns}")
    return df


def cast_timestamp_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    """
    Cast string date/timestamp columns to TimestampType.

    Null-safe: rows with unparseable timestamps will produce null values
    rather than raising an exception (Spark default PERMISSIVE behaviour).

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    columns : list[str]
        Column names to cast to timestamp.

    Returns
    -------
    DataFrame
        DataFrame with specified columns cast to TimestampType.
    """
    print(f"[{PIPELINE_NAME}] Casting columns to TimestampType: {columns}")
    for col_name in columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(TimestampType()))
        else:
            print(
                f"[{PIPELINE_NAME}] WARNING: Column '{col_name}' not found; "
                "skipping cast."
            )
    return df


def apply_filter(df: DataFrame, condition: str) -> DataFrame:
    """
    Filter rows based on a SQL-style condition string.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    condition : str
        SQL WHERE clause expression.

    Returns
    -------
    DataFrame
        Filtered DataFrame.
    """
    print(f"[{PIPELINE_NAME}] Applying filter: {condition}")
    df_filtered = df.filter(condition)
    row_count = df_filtered.count()
    print(f"[{PIPELINE_NAME}] Rows after filter: {row_count:,}")
    return df_filtered


def drop_null_rows(df: DataFrame, columns: list[str]) -> DataFrame:
    """
    Drop rows where any of the specified columns contain null values.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    columns : list[str]
        Columns to check for nulls; rows with nulls in these columns are dropped.

    Returns
    -------
    DataFrame
        DataFrame with null rows removed for the specified columns.
    """
    print(f"[{PIPELINE_NAME}] Dropping rows with nulls in columns: {columns}")
    df_clean = df.dropna(subset=columns)
    row_count = df_clean.count()
    print(f"[{PIPELINE_NAME}] Rows after null drop: {row_count:,}")
    return df_clean


def enrich_partition_columns(df: DataFrame) -> DataFrame:
    """
    Derive partitioning columns from order_purchase_timestamp.

    Adds:
      - order_purchase_year  : integer year extracted from order_purchase_timestamp
      - order_purchase_month : integer month extracted from order_purchase_timestamp

    Parameters
    ----------
    df : DataFrame
        Input DataFrame containing order_purchase_timestamp as TimestampType.

    Returns
    -------
    DataFrame
        DataFrame enriched with year and month partition columns.
    """
    print(
        f"[{PIPELINE_NAME}] Enriching DataFrame with partition columns: "
        "order_purchase_year, order_purchase_month"
    )
    df_enriched = df.withColumn(
        "order_purchase_year", F.year(F.col("order_purchase_timestamp"))
    ).withColumn(
        "order_purchase_month", F.month(F.col("order_purchase_timestamp"))
    )
    return df_enriched


def sort_dataframe(df: DataFrame, columns: list[str], ascending: bool = True) -> DataFrame:
    """
    Sort the DataFrame by the specified columns.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    columns : list[str]
        Column names to sort by.
    ascending : bool
        Sort direction; True for ascending (default).

    Returns
    -------
    DataFrame
        Sorted DataFrame.
    """
    print(
        f"[{PIPELINE_NAME}] Sorting by columns: {columns} "
        f"({'asc' if ascending else 'desc'})"
    )
    sort_exprs = [F.col(c).asc() if ascending else F.col(c).desc() for c in columns]
    return df.orderBy(*sort_exprs)


def write_delta(df: DataFrame, path: str, partition_cols: list[str]) -> None:
    """
    Write the transformed DataFrame to Delta Lake in overwrite mode,
    partitioned by the specified columns.

    Parameters
    ----------
    df : DataFrame
        Transformed DataFrame to persist.
    path : str
        Target S3 URI for the Delta table.
    partition_cols : list[str]
        Columns used to partition the Delta table on disk.
    """
    print(f"[{PIPELINE_NAME}] Writing Delta table to: {path}")
    print(f"[{PIPELINE_NAME}] Partition columns: {partition_cols}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(*partition_cols)
        .save(path)
    )
    print(f"[{PIPELINE_NAME}] Delta write complete.")


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------
def run() -> None:
    """
    Execute the revenue_report_for_completed_transactions ETL pipeline.

    Steps
    -----
    1. Initialise SparkSession with Delta Lake configuration.
    2. Read raw CSV orders data from S3.
    3. Rename positional columns to semantic names.
    4. Cast date/timestamp string columns to TimestampType.
    5. Filter to delivered orders with non-null order_id.
    6. Drop any residual null order_id rows.
    7. Enrich with year/month partition columns.
    8. Sort ascending by order_purchase_year, order_purchase_month.
    9. Write output as a partitioned Delta table to S3.
    """
    print(f"[{PIPELINE_NAME}] ========== Pipeline START ==========")

    # ------------------------------------------------------------------
    # Step 1 – SparkSession
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] Step 1: Initialising SparkSession...")
    spark = create_spark_session()
    print(f"[{PIPELINE_NAME}] SparkSession created. Spark version: {spark.version}")

    # ------------------------------------------------------------------
    # Step 2 – Read source
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] Step 2: Reading source data...")
    df_raw = read_source(spark, SOURCE_PATH)

    # ------------------------------------------------------------------
    # Step 3 – Rename columns
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] Step 3: Renaming columns...")
    df_renamed = rename_columns(df_raw, COLUMN_RENAME_MAP)

    # ------------------------------------------------------------------
    # Step 4 – Cast timestamp columns
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] Step 4: Casting timestamp columns...")
    df_cast = cast_timestamp_columns(df_renamed, TIMESTAMP_COLUMNS)

    # ------------------------------------------------------------------
    # Step 5 – Filter: delivered orders with non-null order_id
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] Step 5: Filtering rows...")
    df_filtered = apply_filter(df_cast, FILTER_CONDITION)

    # ------------------------------------------------------------------
    # Step 6 – Fill null / drop residual null order_id rows
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] Step 6: Dropping residual null order_id rows...")
    df_no_nulls = drop_null_rows(df_filtered, columns=["order_id"])

    # ------------------------------------------------------------------
    # Step 7 – Enrich with partition columns
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] Step 7: Enriching with partition columns...")
    df_enriched = enrich_partition_columns(df_no_nulls)

    # ------------------------------------------------------------------
    # Step 8 – Sort
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] Step 8: Sorting output...")
    df_sorted = sort_dataframe(df_enriched, SORT_COLUMNS, ascending=True)

    # ------------------------------------------------------------------
    # Step 9 – Write to Delta Lake
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] Step 9: Writing to Delta Lake...")
    write_delta(df_sorted, TARGET_PATH, PARTITION_COLS)

    # ------------------------------------------------------------------
    # Done
    # ------------------------------------------------------------------
    print(f"[{PIPELINE_NAME}] ========== Pipeline COMPLETE ==========")
    spark.stop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run()