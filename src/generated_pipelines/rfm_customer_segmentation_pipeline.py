"""
rfm_customer_segmentation_pipeline: Reads raw transactions from S3, computes Recency,
Frequency, and Monetary scores per customer, and writes the result as a Delta table.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SOURCE_PATH: str = "s3://etl-agent-raw-prod/amazon/orders/"
TARGET_PATH: str = "s3://etl-agent-artifacts-prod/analytics/geo_revenue/"
APP_NAME: str = "rfm_customer_segmentation_pipeline"


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
        SparkSession.builder.appName(APP_NAME)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Adaptive query execution for large aggregations
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Step 1 – Read
# ---------------------------------------------------------------------------

def read_source(spark: SparkSession, path: str) -> DataFrame:
    """
    Read raw order transactions from S3 in Parquet format.

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
    print(f"[READ] Reading source Parquet data from: {path}")
    df = spark.read.parquet(path)
    print(f"[READ] Source schema:\n{df._jdf.schema().treeString()}")
    print(f"[READ] Source row count: {df.count():,}")
    return df


# ---------------------------------------------------------------------------
# Step 2 – Filter
# ---------------------------------------------------------------------------

def apply_filter(df: DataFrame) -> DataFrame:
    """
    Remove rows where customer_id is null to satisfy acceptance criteria.

    Parameters
    ----------
    df : DataFrame
        Raw orders DataFrame.

    Returns
    -------
    DataFrame
        Filtered DataFrame with non-null customer_id values only.
    """
    print("[FILTER] Removing rows where customer_id IS NULL ...")
    df_filtered = df.filter(F.col("customer_id").isNotNull())
    filtered_count = df_filtered.count()
    print(f"[FILTER] Rows after filter: {filtered_count:,}")
    return df_filtered


# ---------------------------------------------------------------------------
# Step 3 – Aggregate
# ---------------------------------------------------------------------------

def apply_aggregation(df: DataFrame) -> DataFrame:
    """
    Group by customer_id and compute RFM base metrics:
      - recency_days  : days since the customer's most recent order
      - frequency     : total number of orders placed
      - monetary      : total spend across all orders

    The recency_days metric is computed as the difference (in days) between
    the current date and the customer's most recent order_date.

    Parameters
    ----------
    df : DataFrame
        Filtered orders DataFrame.

    Returns
    -------
    DataFrame
        Aggregated DataFrame with one row per customer.
    """
    print("[AGGREGATE] Computing recency_days, frequency, and monetary per customer ...")

    df_agg = df.groupBy("customer_id").agg(
        # Recency: days since the most recent order (lower = more recent)
        F.datediff(F.current_date(), F.max(F.col("order_date"))).alias("recency_days"),
        # Frequency: total number of orders
        F.count(F.col("order_id")).alias("frequency"),
        # Monetary: total spend; coalesce guards against all-null order_amount groups
        F.coalesce(F.sum(F.col("order_amount")), F.lit(0.0)).alias("monetary"),
    )

    # Ensure non-negative recency (handles future-dated records gracefully)
    df_agg = df_agg.withColumn(
        "recency_days",
        F.when(F.col("recency_days") < 0, F.lit(0)).otherwise(F.col("recency_days")),
    )

    print(f"[AGGREGATE] Distinct customers after aggregation: {df_agg.count():,}")
    return df_agg


# ---------------------------------------------------------------------------
# Step 4 – Enrich (RFM scoring)
# ---------------------------------------------------------------------------

def apply_enrichment(df: DataFrame) -> DataFrame:
    """
    Add quintile-based RFM scores and a human-readable segment label.

    Derived columns
    ---------------
    r_score     : ntile(5) over recency_days ASC  (1 = least recent, 5 = most recent)
    f_score     : ntile(5) over frequency ASC     (1 = lowest frequency, 5 = highest)
    m_score     : ntile(5) over monetary ASC      (1 = lowest spend, 5 = highest)
    rfm_score   : r_score + f_score + m_score     (range 3–15)
    rfm_segment : categorical label derived from rfm_score

    Parameters
    ----------
    df : DataFrame
        Aggregated RFM base-metrics DataFrame.

    Returns
    -------
    DataFrame
        Enriched DataFrame with RFM scores and segment labels.
    """
    print("[ENRICH] Computing r_score, f_score, m_score quintiles ...")

    # Window specs – no partition, ordered globally for ntile quintiles
    w_recency = Window.orderBy(F.col("recency_days").asc())
    w_frequency = Window.orderBy(F.col("frequency").asc())
    w_monetary = Window.orderBy(F.col("monetary").asc())

    df_scored = (
        df
        .withColumn("r_score", F.ntile(5).over(w_recency))
        .withColumn("f_score", F.ntile(5).over(w_frequency))
        .withColumn("m_score", F.ntile(5).over(w_monetary))
    )

    print("[ENRICH] Computing combined rfm_score ...")
    df_scored = df_scored.withColumn(
        "rfm_score",
        F.col("r_score") + F.col("f_score") + F.col("m_score"),
    )

    print("[ENRICH] Assigning rfm_segment labels ...")
    df_enriched = df_scored.withColumn(
        "rfm_segment",
        F.when(F.col("rfm_score") >= 13, F.lit("Champions"))
        .when(F.col("rfm_score") >= 10, F.lit("Loyal Customers"))
        .when(F.col("rfm_score") >= 7, F.lit("Potential Loyalists"))
        .when(F.col("rfm_score") >= 4, F.lit("At Risk"))
        .otherwise(F.lit("Lost")),
    )

    # Segment distribution summary for observability
    print("[ENRICH] RFM segment distribution:")
    df_enriched.groupBy("rfm_segment").count().orderBy("rfm_segment").show(
        truncate=False
    )

    return df_enriched


# ---------------------------------------------------------------------------
# Step 5 – Write
# ---------------------------------------------------------------------------

def write_delta(df: DataFrame, path: str) -> None:
    """
    Write the enriched RFM DataFrame to a Delta Lake table using overwrite mode.

    Parameters
    ----------
    df : DataFrame
        Final enriched DataFrame to persist.
    path : str
        S3 destination path for the Delta table.
    """
    print(f"[WRITE] Writing enriched RFM data to Delta Lake at: {path}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
    print("[WRITE] Delta write complete.")


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

def run() -> None:
    """
    Execute the end-to-end RFM customer segmentation pipeline.

    Steps
    -----
    1. Create SparkSession with Delta Lake configuration.
    2. Read raw Parquet orders from S3.
    3. Filter out records with null customer_id.
    4. Aggregate per-customer recency, frequency, and monetary metrics.
    5. Enrich with quintile scores and segment labels.
    6. Write the result as a Delta table to S3.
    """
    print(f"[PIPELINE] Starting pipeline: {APP_NAME}")

    # 1. Session
    spark = create_spark_session()
    print(f"[PIPELINE] Spark version: {spark.version}")

    try:
        # 2. Read
        df_raw = read_source(spark, SOURCE_PATH)

        # 3. Filter
        df_filtered = apply_filter(df_raw)

        # 4. Aggregate
        df_aggregated = apply_aggregation(df_filtered)

        # 5. Enrich
        df_enriched = apply_enrichment(df_aggregated)

        # 6. Write
        write_delta(df_enriched, TARGET_PATH)

        print(f"[PIPELINE] Pipeline '{APP_NAME}' completed successfully.")

    except Exception as exc:  # noqa: BLE001
        print(f"[PIPELINE] Pipeline '{APP_NAME}' FAILED with error: {exc}")
        raise

    finally:
        spark.stop()
        print("[PIPELINE] SparkSession stopped.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()