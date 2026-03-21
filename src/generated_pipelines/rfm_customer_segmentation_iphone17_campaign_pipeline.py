"""
rfm_customer_segmentation_iphone17_campaign_pipeline

Compute Recency, Frequency, and Monetary (RFM) scores for Amazon customers
to identify high-value targets for the iPhone 17 launch campaign.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
    print(f"[INIT] Creating SparkSession for '{APP_NAME}'...")
    spark = (
        configure_spark_with_delta_pip(
            SparkSession.builder
            .appName(APP_NAME)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        ).getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("[INIT] SparkSession created successfully.")
    return spark


# ---------------------------------------------------------------------------
# Step 1 – Read
# ---------------------------------------------------------------------------

def read_source(spark: SparkSession) -> DataFrame:
    """
    Read raw Amazon orders data from S3 in Parquet format.

    Parameters
    ----------
    spark : SparkSession
        Active SparkSession.

    Returns
    -------
    DataFrame
        Raw orders DataFrame.
    """
    print(f"[READ] Reading source Parquet data from: {SOURCE_PATH}")
    df = spark.read.parquet(SOURCE_PATH)
    row_count = df.count()
    print(f"[READ] Source rows loaded: {row_count:,}")
    print(f"[READ] Schema:\n{df._jdf.schema().treeString()}")
    return df


# ---------------------------------------------------------------------------
# Step 2 – Filter
# ---------------------------------------------------------------------------

def filter_orders(df: DataFrame) -> DataFrame:
    """
    Exclude cancelled and returned orders from the dataset.

    Rows where ``order_status`` is NULL are also dropped to avoid
    polluting downstream aggregations.

    Parameters
    ----------
    df : DataFrame
        Raw orders DataFrame.

    Returns
    -------
    DataFrame
        Filtered orders DataFrame containing only valid orders.
    """
    print("[FILTER] Excluding CANCELLED and RETURNED orders...")
    print(f"[FILTER] Excluded statuses: {EXCLUDED_STATUSES}")

    df_filtered = df.filter(
        F.col("order_status").isNotNull()
        & ~F.col("order_status").isin(EXCLUDED_STATUSES)
    )

    row_count = df_filtered.count()
    print(f"[FILTER] Rows after filter: {row_count:,}")
    return df_filtered


# ---------------------------------------------------------------------------
# Step 3 – Aggregate
# ---------------------------------------------------------------------------

def aggregate_customer_metrics(df: DataFrame) -> DataFrame:
    """
    Group by ``customer_id`` and compute RFM base metrics.

    Aggregations
    ------------
    - ``last_order_date`` : most recent order date per customer (max).
    - ``frequency``       : total number of orders (count of order_id).
    - ``monetary``        : total spend (sum of order_value).

    Rows with a NULL ``customer_id`` are dropped before aggregation to
    ensure referential integrity of the output.

    Parameters
    ----------
    df : DataFrame
        Filtered orders DataFrame.

    Returns
    -------
    DataFrame
        One row per customer with RFM base metrics.
    """
    print("[AGGREGATE] Dropping rows with null customer_id...")
    df_valid = df.filter(F.col("customer_id").isNotNull())

    print("[AGGREGATE] Computing last_order_date, frequency, and monetary per customer...")
    df_agg = df_valid.groupBy("customer_id").agg(
        F.max("order_date").alias("last_order_date"),
        F.count("order_id").alias("frequency"),
        F.sum(
            F.coalesce(F.col("order_value"), F.lit(0.0))
        ).alias("monetary"),
    )

    # Ensure monetary is never negative (e.g. due to refund rows slipping through)
    df_agg = df_agg.withColumn(
        "monetary",
        F.when(F.col("monetary") < 0, F.lit(0.0)).otherwise(F.col("monetary")),
    )

    row_count = df_agg.count()
    print(f"[AGGREGATE] Distinct customers after aggregation: {row_count:,}")
    return df_agg


# ---------------------------------------------------------------------------
# Step 4 – Enrich (RFM scoring)
# ---------------------------------------------------------------------------

def enrich_rfm_scores(df: DataFrame) -> DataFrame:
    """
    Score customers 1-5 on each RFM dimension using ntile bucketing and
    compute a composite ``rfm_score`` and human-readable ``rfm_segment`` label.

    Derived columns
    ---------------
    - ``recency_days`` : days since last order (lower = more recent).
    - ``r_score``      : recency ntile (5 = most recent).
    - ``f_score``      : frequency ntile (5 = most frequent).
    - ``m_score``      : monetary ntile (5 = highest spender).
    - ``rfm_score``    : sum of r_score + f_score + m_score (3–15).
    - ``rfm_segment``  : categorical label derived from rfm_score.

    Segment thresholds
    ------------------
    - Champions        : rfm_score >= 13
    - Loyal Customers  : rfm_score >= 10
    - Potential Loyalists : rfm_score >= 7
    - At Risk          : rfm_score >= 4
    - Lost             : rfm_score < 4

    Parameters
    ----------
    df : DataFrame
        Aggregated customer metrics DataFrame.

    Returns
    -------
    DataFrame
        Enriched DataFrame with all RFM score columns appended.
    """
    print("[ENRICH] Computing recency_days from last_order_date...")
    df_recency = df.withColumn(
        "recency_days",
        F.datediff(F.current_date(), F.col("last_order_date")),
    )

    # Null-safe recency: customers with no parseable date get max recency penalty
    df_recency = df_recency.withColumn(
        "recency_days",
        F.when(F.col("recency_days").isNull(), F.lit(99999)).otherwise(
            F.col("recency_days")
        ),
    )

    # Window specifications – global ordering (no partition) for ntile
    print(f"[ENRICH] Applying ntile({RFM_BUCKETS}) bucketing for R, F, M scores...")
    w_recency = Window.orderBy(F.col("recency_days").asc())
    w_frequency = Window.orderBy(F.col("frequency").asc())
    w_monetary = Window.orderBy(F.col("monetary").asc())

    df_scored = (
        df_recency
        .withColumn("r_score", F.ntile(RFM_BUCKETS).over(w_recency))
        .withColumn("f_score", F.ntile(RFM_BUCKETS).over(w_frequency))
        .withColumn("m_score", F.ntile(RFM_BUCKETS).over(w_monetary))
    )

    print("[ENRICH] Computing composite rfm_score...")
    df_scored = df_scored.withColumn(
        "rfm_score",
        F.col("r_score") + F.col("f_score") + F.col("m_score"),
    )

    print("[ENRICH] Assigning rfm_segment labels...")
    df_enriched = df_scored.withColumn(
        "rfm_segment",
        F.when(F.col("rfm_score") >= 13, F.lit("Champions"))
        .when(F.col("rfm_score") >= 10, F.lit("Loyal Customers"))
        .when(F.col("rfm_score") >= 7, F.lit("Potential Loyalists"))
        .when(F.col("rfm_score") >= 4, F.lit("At Risk"))
        .otherwise(F.lit("Lost")),
    )

    # Audit: segment distribution
    print("[ENRICH] RFM segment distribution:")
    df_enriched.groupBy("rfm_segment").count().orderBy("rfm_segment").show(
        truncate=False
    )

    return df_enriched


# ---------------------------------------------------------------------------
# Step 5 – Write
# ---------------------------------------------------------------------------

def write_delta(df: DataFrame) -> None:
    """
    Write the enriched RFM DataFrame to Delta Lake in overwrite mode.

    Parameters
    ----------
    df : DataFrame
        Final enriched RFM DataFrame to persist.
    """
    print(f"[WRITE] Writing RFM scores to Delta Lake at: {TARGET_PATH}")
    print(f"[WRITE] Mode: overwrite | Format: delta")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(TARGET_PATH)
    )

    print("[WRITE] Delta write completed successfully.")


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

def run() -> None:
    """
    Execute the full RFM customer segmentation pipeline.

    Pipeline steps
    --------------
    1. Create SparkSession with Delta Lake configuration.
    2. Read raw Amazon orders from S3 (Parquet).
    3. Filter out CANCELLED and RETURNED orders.
    4. Aggregate per-customer metrics (recency, frequency, monetary).
    5. Enrich with ntile-based RFM scores and segment labels.
    6. Write results to Delta Lake (overwrite).
    """
    print("=" * 70)
    print(f"[PIPELINE] Starting: {APP_NAME}")
    print("=" * 70)

    spark = create_spark_session()

    try:
        # Step 1 – Read
        df_raw = read_source(spark)

        # Step 2 – Filter
        df_filtered = filter_orders(df_raw)

        # Step 3 – Aggregate
        df_aggregated = aggregate_customer_metrics(df_filtered)

        # Step 4 – Enrich
        df_enriched = enrich_rfm_scores(df_aggregated)

        # Step 5 – Write
        write_delta(df_enriched)

        print("=" * 70)
        print(f"[PIPELINE] '{APP_NAME}' completed successfully.")
        print("=" * 70)

    except Exception as exc:
        print(f"[ERROR] Pipeline failed with exception: {exc}")
        raise

    finally:
        spark.stop()
        print("[INIT] SparkSession stopped.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()