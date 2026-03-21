"""
rfm_customer_segmentation_iphone17_campaign_pipeline

Compute Recency, Frequency, and Monetary (RFM) scores for Amazon customers
to identify high-value targets for the iPhone 17 launch campaign.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from delta import configure_spark_with_delta_pip


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SOURCE_PATH: str = "s3://etl-agent-raw-prod/amazon/orders/"
TARGET_PATH: str = "s3://etl-agent-artifacts-prod/analytics/rfm_scores/"
APP_NAME: str = "rfm_customer_segmentation_iphone17_campaign_pipeline"

EXCLUDED_STATUSES: list[str] = ["CANCELLED", "RETURNED"]
RFM_BUCKETS: int = 5


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
# Step 1 – Ingest
# ---------------------------------------------------------------------------

def read_source(spark: SparkSession) -> DataFrame:
    """
    Read raw Amazon orders from S3 in Parquet format.

    Parameters
    ----------
    spark : SparkSession
        Active SparkSession.

    Returns
    -------
    DataFrame
        Raw orders DataFrame.
    """
    print(f"[INGEST] Reading source parquet data from: {SOURCE_PATH}")
    df = spark.read.parquet(SOURCE_PATH)
    row_count = df.count()
    print(f"[INGEST] Source rows loaded: {row_count:,}")
    print(f"[INGEST] Schema:\n{df._jdf.schema().treeString()}")
    return df


# ---------------------------------------------------------------------------
# Step 2 – Filter
# ---------------------------------------------------------------------------

def filter_orders(df: DataFrame) -> DataFrame:
    """
    Exclude cancelled and returned orders from the dataset.

    Null values in ``order_status`` are treated conservatively and retained
    so that valid orders with missing status are not silently dropped.

    Parameters
    ----------
    df : DataFrame
        Raw orders DataFrame.

    Returns
    -------
    DataFrame
        Filtered DataFrame containing only actionable orders.
    """
    print("[FILTER] Excluding orders with status in: CANCELLED, RETURNED")

    df_filtered = df.filter(
        ~F.col("order_status").isin(EXCLUDED_STATUSES)
        | F.col("order_status").isNull()
    )

    # Drop rows where core RFM columns are null to avoid skewing aggregations
    df_filtered = df_filtered.filter(
        F.col("customer_id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("order_date").isNotNull()
        & F.col("order_value").isNotNull()
    )

    row_count = df_filtered.count()
    print(f"[FILTER] Rows after filtering: {row_count:,}")
    return df_filtered


# ---------------------------------------------------------------------------
# Step 3 – Aggregate
# ---------------------------------------------------------------------------

def aggregate_rfm_base(df: DataFrame) -> DataFrame:
    """
    Group by customer_id and compute RFM base metrics.

    Metrics computed
    ----------------
    - last_order_date : most recent order date per customer
    - frequency       : total number of orders placed
    - monetary        : total spend (sum of order_value)

    Parameters
    ----------
    df : DataFrame
        Filtered orders DataFrame.

    Returns
    -------
    DataFrame
        One row per customer with RFM base metrics.
    """
    print("[AGGREGATE] Computing last_order_date, frequency, and monetary per customer_id")

    df_agg = df.groupBy("customer_id").agg(
        F.max("order_date").alias("last_order_date"),
        F.count("order_id").alias("frequency"),
        F.sum(F.coalesce(F.col("order_value"), F.lit(0.0))).alias("monetary"),
    )

    # Ensure monetary is never negative (e.g. due to refund adjustments)
    df_agg = df_agg.withColumn(
        "monetary", F.greatest(F.col("monetary"), F.lit(0.0))
    )

    row_count = df_agg.count()
    print(f"[AGGREGATE] Distinct customers after aggregation: {row_count:,}")
    return df_agg


# ---------------------------------------------------------------------------
# Step 4 – Enrich (RFM Scoring)
# ---------------------------------------------------------------------------

def enrich_rfm_scores(df: DataFrame) -> DataFrame:
    """
    Score each customer 1-5 on Recency, Frequency, and Monetary dimensions
    using ntile bucketing, then derive a composite RFM score and segment label.

    Scoring logic
    -------------
    - recency_days : days since last order (lower is better → ASC ntile)
    - r_score      : ntile(5) OVER (ORDER BY recency_days ASC)
    - f_score      : ntile(5) OVER (ORDER BY frequency ASC)
    - m_score      : ntile(5) OVER (ORDER BY monetary ASC)
    - rfm_score    : r_score + f_score + m_score  (range 3–15)
    - rfm_segment  : Champions / Loyal Customers / Potential Loyalists /
                     At Risk / Lost

    Parameters
    ----------
    df : DataFrame
        Aggregated RFM base DataFrame (one row per customer).

    Returns
    -------
    DataFrame
        Enriched DataFrame with all RFM scores and segment labels.
    """
    print("[ENRICH] Computing recency_days from current_date()")

    # --- Derived column: recency_days ---
    df = df.withColumn(
        "recency_days",
        F.datediff(F.current_date(), F.col("last_order_date")),
    )

    # Null-safe: if last_order_date is somehow null after aggregation, assign
    # a large recency value so the customer scores low on recency.
    df = df.withColumn(
        "recency_days",
        F.coalesce(F.col("recency_days"), F.lit(99999)),
    )

    print("[ENRICH] Applying ntile(5) window functions for R, F, M scores")

    # Window specifications (global ordering – no partition)
    w_recency = Window.orderBy(F.col("recency_days").asc())
    w_frequency = Window.orderBy(F.col("frequency").asc())
    w_monetary = Window.orderBy(F.col("monetary").asc())

    # --- RFM dimension scores ---
    df = (
        df.withColumn("r_score", F.ntile(RFM_BUCKETS).over(w_recency))
          .withColumn("f_score", F.ntile(RFM_BUCKETS).over(w_frequency))
          .withColumn("m_score", F.ntile(RFM_BUCKETS).over(w_monetary))
    )

    # --- Composite score ---
    df = df.withColumn(
        "rfm_score",
        F.col("r_score") + F.col("f_score") + F.col("m_score"),
    )

    # --- Segment label ---
    print("[ENRICH] Assigning rfm_segment labels based on composite rfm_score")
    df = df.withColumn(
        "rfm_segment",
        F.when(F.col("rfm_score") >= 13, F.lit("Champions"))
         .when(F.col("rfm_score") >= 10, F.lit("Loyal Customers"))
         .when(F.col("rfm_score") >= 7,  F.lit("Potential Loyalists"))
         .when(F.col("rfm_score") >= 4,  F.lit("At Risk"))
         .otherwise(F.lit("Lost")),
    )

    # --- Audit metadata ---
    df = df.withColumn("pipeline_run_ts", F.current_timestamp())

    enriched_count = df.count()
    print(f"[ENRICH] Enriched customer records: {enriched_count:,}")

    # Segment distribution summary
    print("[ENRICH] RFM segment distribution:")
    df.groupBy("rfm_segment").count().orderBy("rfm_segment").show(truncate=False)

    return df


# ---------------------------------------------------------------------------
# Step 5 – Write
# ---------------------------------------------------------------------------

def write_delta(df: DataFrame) -> None:
    """
    Persist the enriched RFM DataFrame to Delta Lake using overwrite mode.

    Parameters
    ----------
    df : DataFrame
        Final enriched RFM DataFrame ready for persistence.
    """
    print(f"[WRITE] Writing enriched RFM scores to Delta Lake: {TARGET_PATH}")

    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .save(TARGET_PATH)
    )

    print(f"[WRITE] Delta write complete → {TARGET_PATH}")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run() -> None:
    """
    Execute the full RFM customer segmentation pipeline.

    Pipeline stages
    ---------------
    1. Create SparkSession
    2. Ingest raw orders (Parquet)
    3. Filter cancelled / returned orders
    4. Aggregate per-customer RFM base metrics
    5. Enrich with ntile RFM scores and segment labels
    6. Write results to Delta Lake
    """
    print("=" * 70)
    print(f"[PIPELINE] Starting: {APP_NAME}")
    print("=" * 70)

    # 1. Session
    spark = create_spark_session()
    print(f"[PIPELINE] Spark version: {spark.version}")

    try:
        # 2. Ingest
        df_raw = read_source(spark)

        # 3. Filter
        df_filtered = filter_orders(df_raw)

        # 4. Aggregate
        df_aggregated = aggregate_rfm_base(df_filtered)

        # 5. Enrich
        df_enriched = enrich_rfm_scores(df_aggregated)

        # 6. Write
        write_delta(df_enriched)

        print("=" * 70)
        print(f"[PIPELINE] Successfully completed: {APP_NAME}")
        print("=" * 70)

    except Exception as exc:  # noqa: BLE001
        print(f"[PIPELINE] FATAL ERROR – pipeline aborted: {exc}")
        raise

    finally:
        spark.stop()
        print("[PIPELINE] SparkSession stopped.")


if __name__ == "__main__":
    run()