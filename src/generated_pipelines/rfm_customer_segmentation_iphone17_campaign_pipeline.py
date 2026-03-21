"""
rfm_customer_segmentation_iphone17_campaign_pipeline

Build a PySpark pipeline to compute Recency, Frequency, and Monetary (RFM)
scores for Amazon customers to identify high-value targets for the iPhone 17
launch campaign.
"""

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SOURCE_PATH: str = "s3://etl-agent-raw-prod/amazon/orders/"
TARGET_PATH: str = "s3://etl-agent-artifacts-prod/analytics/rfm_scores/"
APP_NAME: str = "rfm_customer_segmentation_iphone17_campaign_pipeline"

EXCLUDED_STATUSES: list[str] = ["CANCELLED", "RETURNED"]
NTILE_BUCKETS: int = 5


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
        # Improve stability for large aggregations
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
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

def filter_invalid_orders(df: DataFrame) -> DataFrame:
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
        Filtered DataFrame containing only valid orders.
    """
    print("[FILTER] Excluding CANCELLED and RETURNED orders...")
    print(f"[FILTER] Also dropping rows with NULL order_status or NULL customer_id.")

    df_filtered = (
        df.filter(F.col("order_status").isNotNull())
        .filter(~F.col("order_status").isin(EXCLUDED_STATUSES))
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("order_date").isNotNull())
        # Treat NULL / negative order_value as zero to keep the customer record
        .withColumn(
            "order_value",
            F.when(
                F.col("order_value").isNull() | (F.col("order_value") < 0),
                F.lit(0.0),
            ).otherwise(F.col("order_value").cast("double")),
        )
    )

    row_count = df_filtered.count()
    print(f"[FILTER] Rows after filtering: {row_count:,}")
    return df_filtered


# ---------------------------------------------------------------------------
# Step 3 – Aggregate (Recency, Frequency, Monetary)
# ---------------------------------------------------------------------------

def compute_rfm_base(df: DataFrame) -> DataFrame:
    """
    Compute per-customer RFM base metrics.

    Metrics computed
    ----------------
    * ``last_order_date``  – most recent order date (MAX)
    * ``frequency``        – total number of orders (COUNT)
    * ``monetary``         – total spend (SUM of order_value)
    * ``recency_days``     – calendar days between last order and today

    Parameters
    ----------
    df : DataFrame
        Filtered orders DataFrame.

    Returns
    -------
    DataFrame
        One row per customer with RFM base metrics.
    """
    print("[AGGREGATE] Computing recency, frequency, and monetary per customer...")

    df_agg = df.groupBy("customer_id").agg(
        F.max("order_date").alias("last_order_date"),
        F.count("order_id").alias("frequency"),
        F.sum("order_value").alias("monetary"),
    )

    # Recency: days since last order (lower = more recent = better)
    df_agg = df_agg.withColumn(
        "recency_days",
        F.datediff(F.current_date(), F.col("last_order_date")),
    )

    # Guard against NULL recency (e.g. future-dated orders)
    df_agg = df_agg.withColumn(
        "recency_days",
        F.when(F.col("recency_days").isNull() | (F.col("recency_days") < 0), F.lit(0))
        .otherwise(F.col("recency_days")),
    )

    row_count = df_agg.count()
    print(f"[AGGREGATE] Unique customers after aggregation: {row_count:,}")
    return df_agg


# ---------------------------------------------------------------------------
# Step 4 – Enrich (RFM Scoring & Segmentation)
# ---------------------------------------------------------------------------

def enrich_rfm_scores(df: DataFrame) -> DataFrame:
    """
    Score each customer 1-5 on each RFM dimension using ntile bucketing,
    then derive a composite ``rfm_score`` and human-readable ``rfm_segment``.

    Scoring logic
    -------------
    * ``r_score`` – ntile(5) ordered by recency_days ASC
      (lower recency = higher score, i.e. more recent customers score higher)
    * ``f_score`` – ntile(5) ordered by frequency ASC
    * ``m_score`` – ntile(5) ordered by monetary ASC
    * ``rfm_score`` = r_score + f_score + m_score  (range 3–15)
    * ``rfm_segment`` – label derived from rfm_score thresholds

    Parameters
    ----------
    df : DataFrame
        Aggregated RFM base metrics DataFrame.

    Returns
    -------
    DataFrame
        Enriched DataFrame with RFM scores and segment labels.
    """
    print("[ENRICH] Applying ntile(5) bucketing for R, F, M dimensions...")

    # Window specs – no partition, order by each metric
    # NOTE: For recency, lower days = more recent = better, so ASC gives
    #       ntile bucket 1 to the least recent and 5 to the most recent.
    w_recency = Window.orderBy(F.col("recency_days").asc())
    w_frequency = Window.orderBy(F.col("frequency").asc())
    w_monetary = Window.orderBy(F.col("monetary").asc())

    df_scored = (
        df
        # Individual dimension scores
        .withColumn("r_score", F.ntile(NTILE_BUCKETS).over(w_recency))
        .withColumn("f_score", F.ntile(NTILE_BUCKETS).over(w_frequency))
        .withColumn("m_score", F.ntile(NTILE_BUCKETS).over(w_monetary))
        # Composite score
        .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))
    )

    print("[ENRICH] Deriving rfm_segment labels from composite rfm_score...")

    df_enriched = df_scored.withColumn(
        "rfm_segment",
        F.when(F.col("rfm_score") >= 13, F.lit("Champions"))
        .when(F.col("rfm_score") >= 10, F.lit("Loyal Customers"))
        .when(F.col("rfm_score") >= 7, F.lit("Potential Loyalists"))
        .when(F.col("rfm_score") >= 4, F.lit("At Risk"))
        .otherwise(F.lit("Lost")),
    )

    # Add pipeline metadata columns
    df_enriched = df_enriched.withColumn(
        "pipeline_run_ts", F.current_timestamp()
    ).withColumn(
        "pipeline_name", F.lit(APP_NAME)
    )

    print("[ENRICH] Segment distribution preview:")
    df_enriched.groupBy("rfm_segment").count().orderBy("rfm_segment").show(
        truncate=False
    )

    return df_enriched


# ---------------------------------------------------------------------------
# Step 5 – Write
# ---------------------------------------------------------------------------

def write_delta(df: DataFrame) -> None:
    """
    Persist the enriched RFM DataFrame to Delta Lake (overwrite mode).

    Parameters
    ----------
    df : DataFrame
        Final enriched RFM DataFrame to persist.
    """
    print(f"[WRITE] Writing RFM scores to Delta Lake at: {TARGET_PATH}")

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(TARGET_PATH)
    )

    print("[WRITE] Delta write complete.")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run() -> None:
    """
    Execute the full RFM customer segmentation pipeline end-to-end.

    Pipeline steps
    --------------
    1. Create SparkSession
    2. Ingest raw orders from S3 (Parquet)
    3. Filter out CANCELLED / RETURNED orders
    4. Aggregate per-customer RFM base metrics
    5. Enrich with ntile scores and segment labels
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
        df_filtered = filter_invalid_orders(df_raw)

        # 4. Aggregate
        df_rfm_base = compute_rfm_base(df_filtered)

        # 5. Enrich
        df_rfm_final = enrich_rfm_scores(df_rfm_base)

        # 6. Write
        write_delta(df_rfm_final)

        print("=" * 70)
        print(f"[PIPELINE] Successfully completed: {APP_NAME}")
        print("=" * 70)

    except Exception as exc:  # noqa: BLE001
        print(f"[PIPELINE] FATAL ERROR – pipeline aborted: {exc}")
        raise

    finally:
        spark.stop()
        print("[PIPELINE] SparkSession stopped.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()