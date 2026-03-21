"""
geographic_revenue_analysis_by_state_pipeline.py

Aggregate monthly iPhone 17 campaign revenue by US state to identify
top-performing regions.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SOURCE_PATH: str = "s3://etl-agent-raw-prod/amazon/orders/"
TARGET_PATH: str = "s3://etl-agent-artifacts-prod/analytics/geo_revenue/"
PARTITION_COLS: list[str] = ["shipping_state", "year", "month"]
PRODUCT_FILTER: str = "iPhone 17"


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

def create_spark_session(app_name: str = "geographic_revenue_analysis_by_state_pipeline") -> SparkSession:
    """
    Create and return a SparkSession configured for Delta Lake.

    Parameters
    ----------
    app_name : str
        Spark application name.

    Returns
    -------
    SparkSession
        Configured SparkSession instance.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def read_source(spark: SparkSession, path: str) -> DataFrame:
    """
    Read raw order data from S3 in Parquet format.

    Parameters
    ----------
    spark : SparkSession
        Active SparkSession.
    path : str
        S3 path to the source Parquet dataset.

    Returns
    -------
    DataFrame
        Raw orders DataFrame.
    """
    print(f"[EXTRACT] Reading source Parquet data from: {path}")
    df = spark.read.parquet(path)
    print(f"[EXTRACT] Source schema:\n{df._jdf.schema().treeString()}")
    print(f"[EXTRACT] Estimated partition count: {df.rdd.getNumPartitions()}")
    return df


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def filter_iphone17_orders(df: DataFrame, product_category: str = PRODUCT_FILTER) -> DataFrame:
    """
    Filter orders to iPhone 17 product SKUs only.

    Null values in ``product_category`` are treated as non-matching and
    excluded from the result set.

    Parameters
    ----------
    df : DataFrame
        Raw orders DataFrame.
    product_category : str
        Product category value to retain.

    Returns
    -------
    DataFrame
        Filtered DataFrame containing only iPhone 17 orders.
    """
    print(f"[FILTER] Applying product_category filter: '{product_category}'")
    df_filtered = df.filter(
        F.col("product_category").isNotNull()
        & (F.col("product_category") == product_category)
    )
    print("[FILTER] Filter applied successfully.")
    return df_filtered


def aggregate_revenue_by_state(df: DataFrame) -> DataFrame:
    """
    Sum revenue and count orders grouped by shipping state, year, and month.

    Null values in ``order_value`` are coerced to 0.0 before aggregation so
    that a single bad row does not suppress an entire state/month bucket.

    Parameters
    ----------
    df : DataFrame
        Filtered orders DataFrame.

    Returns
    -------
    DataFrame
        Aggregated DataFrame with columns:
        shipping_state, year, month, total_revenue, order_count.
    """
    print("[AGGREGATE] Computing revenue aggregations grouped by shipping_state, year, month.")
    df_safe = df.withColumn(
        "order_value",
        F.coalesce(F.col("order_value").cast("double"), F.lit(0.0)),
    )
    df_agg = df_safe.groupBy("shipping_state", "year", "month").agg(
        F.sum("order_value").alias("total_revenue"),
        F.count("order_id").alias("order_count"),
    )
    print("[AGGREGATE] Aggregation complete.")
    return df_agg


def sort_by_revenue(df: DataFrame) -> DataFrame:
    """
    Sort results by total revenue descending to rank top-performing regions.

    Parameters
    ----------
    df : DataFrame
        Aggregated DataFrame.

    Returns
    -------
    DataFrame
        Sorted DataFrame.
    """
    print("[SORT] Sorting by total_revenue descending.")
    df_sorted = df.orderBy(F.col("total_revenue").desc())
    print("[SORT] Sort applied.")
    return df_sorted


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def write_delta(df: DataFrame, path: str, partition_cols: list[str]) -> None:
    """
    Write the transformed DataFrame to Delta Lake using overwrite mode.

    Parameters
    ----------
    df : DataFrame
        Final transformed DataFrame.
    path : str
        Target S3 Delta Lake path.
    partition_cols : list[str]
        Columns used to partition the Delta table on disk.
    """
    print(f"[LOAD] Writing Delta table to: {path}")
    print(f"[LOAD] Partition columns: {partition_cols}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(*partition_cols)
        .save(path)
    )
    print("[LOAD] Delta write complete.")


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def run() -> None:
    """
    Execute the geographic_revenue_analysis_by_state_pipeline end-to-end.

    Steps
    -----
    1. Create SparkSession with Delta Lake configuration.
    2. Extract raw orders from S3 (Parquet).
    3. Filter to iPhone 17 orders only.
    4. Aggregate revenue and order counts by state / year / month.
    5. Sort by total revenue descending.
    6. Write results to Delta Lake, partitioned by shipping_state / year / month.
    """
    print("[PIPELINE] Starting geographic_revenue_analysis_by_state_pipeline")

    # 1. Session
    spark = create_spark_session()
    print(f"[PIPELINE] Spark version: {spark.version}")

    # 2. Extract
    df_raw = read_source(spark, SOURCE_PATH)

    # 3. Filter
    df_filtered = filter_iphone17_orders(df_raw)

    # 4. Aggregate
    df_aggregated = aggregate_revenue_by_state(df_filtered)

    # 5. Sort
    df_sorted = sort_by_revenue(df_aggregated)

    # 6. Load
    write_delta(df_sorted, TARGET_PATH, PARTITION_COLS)

    print("[PIPELINE] geographic_revenue_analysis_by_state_pipeline finished successfully.")
    spark.stop()


if __name__ == "__main__":
    run()