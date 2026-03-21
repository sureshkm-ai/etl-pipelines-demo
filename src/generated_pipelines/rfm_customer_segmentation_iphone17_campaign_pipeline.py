"""
rfm_customer_segmentation_iphone17_campaign_pipeline

Build a PySpark pipeline to compute Recency, Frequency, and Monetary (RFM)
scores for Amazon customers to identify high-value targets for the iPhone 17
launch campaign.
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
        # Allow window functions across the full dataset
        .config("spark.sql.shuffle.partitions", "200")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Step 1 – Ingest
# ---------------------------------------------------------------------------

def read_orders(spark: SparkSession) -> DataFrame:
    """
    Read raw Amazon orders from the S3 Parquet source.

    Parameters
    ----------
    spark : SparkSession
        Active SparkSession.

    Returns
    -------
    DataFrame
        Raw orders DataFrame.
    """
    print(f"[INGEST] Reading Parquet source from: {SOURCE_PATH}")
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
    Exclude cancelled and returned orders, and drop rows with critical nulls.

    Transformation
    --------------
    - Remove rows where ``order_status`` is 'CANCELLED' or 'RETURNED'.
    - Drop rows where ``customer_id``, ``order_id``, ``order_date``, or
      ``order_value`` are null (required for RFM computation).

    Parameters
    ----------
    df : DataFrame
        Raw orders DataFrame.

    Returns
    -------
    DataFrame
        Filtered DataFrame containing only valid, actionable orders.
    """
    print("[FILTER] Excluding CANCELLED and RETURNED orders...")

    # Guard: ensure order_status nulls don't accidentally pass through
    df_filtered = df.filter(
        F.col("order_status").isNotNull()
        & ~F.col("order_status").isin(EXCLUDED_STATUSES)
    )

    print("[FILTER] Dropping rows with null values in critical columns...")
    critical_columns: list[str] = ["customer_id", "order_id", "order_date", "order_value"]
    df_clean = df_filtered.dropna(subset=critical_columns)

    # Coerce types defensively
    df_clean = (
        df_clean
        .withColumn("order_date", F.col("order_date").cast("date"))
        .withColumn("order_value", F.col("order_value").cast("double"))
        .withColumn("order_id", F.col("order_id").cast("string"))
        .withColumn("customer_id", F.col("customer_id").cast("string"))
    )

    row_count = df_clean.count()
    print(f"[FILTER] Rows after filtering: {row_count:,}")
    return df_clean


# ---------------------------------------------------------------------------
# Step 3 – Aggregate (RFM base metrics)
# ---------------------------------------------------------------------------

def compute_rfm_base_metrics(df: DataFrame) -> DataFrame:
    """
    Aggregate orders to compute per-customer RFM base metrics.

    Aggregations
    ------------
    - ``last_order_date``  : MAX(order_date)  – most recent purchase date.
    - ``frequency``        : COUNT(order_id)  – total number of orders.
    - ``monetary``         : SUM(order_value) – total spend.
    - ``recency_days``     : DATEDIFF(current_date, last_order_date).

    Parameters
    ----------
    df : DataFrame
        Filtered orders DataFrame.

    Returns
    -------
    DataFrame
        One row per customer with RFM base metrics.
    """
    print("[AGGREGATE] Computing per-customer RFM base metrics...")

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

    # Null-safe defaults: if monetary is somehow null after sum, default to 0
    df_agg = (
        df_agg
        .withColumn("monetary", F.coalesce(F.col("monetary"), F.lit(0.0)))
        .withColumn("frequency", F.coalesce(F.col("frequency"), F.lit(0)))
        .withColumn("recency_days", F.coalesce(F.col("recency_days"), F.lit(9999)))
    )

    row_count = df_agg.count()
    print(f"[AGGREGATE] Unique customers after aggregation: {row_count:,}")
    return df_agg


# ---------------------------------------------------------------------------
# Step 4 – Enrich (RFM scoring via ntile bucketing)
# ---------------------------------------------------------------------------

def enrich_rfm_scores(df: DataFrame) -> DataFrame:
    """
    Score each customer 1–5 on Recency, Frequency, and Monetary dimensions
    using ntile window bucketing, then derive a composite RFM segment label.

    Scoring Logic
    -------------
    - ``r_score`` : ntile(5) ORDER BY recency_days DESC
                    (higher score = purchased more recently).
    - ``f_score`` : ntile(5) ORDER BY frequency ASC
                    (higher score = ordered more frequently).
    - ``m_score`` : ntile(5) ORDER BY monetary ASC
                    (higher score = spent more).
    - ``rfm_score``: r_score + f_score + m_score  (range 3–15).
    - ``rfm_segment``:
        * Champions        : rfm_score >= 13
        * Loyal Customers  : rfm_score >= 10
        * Potential Loyalists: rfm_score >= 7
        * At Risk          : rfm_score >= 4
        * Lost             : rfm_score < 4

    Parameters
    ----------
    df : DataFrame
        Aggregated RFM base metrics DataFrame.

    Returns
    -------
    DataFrame
        Enriched DataFrame with RFM scores and segment labels.
    """
    print(f"[ENRICH] Applying ntile({RFM_BUCKETS}) bucketing for RFM scores...")

    # Window specs – no partition, order across entire dataset
    w_recency = Window.orderBy(F.col("recency_days").desc())
    w_frequency = Window.orderBy(F.col("frequency").asc())
    w_monetary = Window.orderBy(F.col("monetary").asc())

    df_scored = (
        df
        .withColumn("r_score", F.ntile(RFM_BUCKETS).over(w_recency))
        .withColumn("f_score", F.ntile(RFM_BUCKETS).over(w_frequency))
        .withColumn("m_score", F.ntile(RFM_BUCKETS).over(w_monetary))
    )

    # Composite score
    df_scored = df_scored.withColumn(
        "rfm_score",
        F.col("r_score") + F.col("f_score") + F.col("m_score"),
    )

    # Segment label
    df_scored = df_scored.withColumn(
        "rfm_segment",
        F.when(F.col("rfm_score") >= 13, F.lit("Champions"))
        .when(F.col("rfm_score") >= 10, F.lit("Loyal Customers"))
        .when(F.col("rfm_score") >= 7, F.lit("Potential Loyalists"))
        .when(F.col("rfm_score") >= 4, F.lit("At Risk"))
        .otherwise(F.lit("Lost")),
    )

    # Audit: segment distribution
    print("[ENRICH] RFM segment distribution:")
    df_scored.groupBy("rfm_segment").count().orderBy("rfm_segment").show(truncate=False)

    return df_scored


# ---------------------------------------------------------------------------
# Step 5 – Write
# ---------------------------------------------------------------------------

def write_rfm_scores(df: DataFrame) -> None:
    """
    Write the enriched RFM scores DataFrame to Delta Lake (overwrite mode).

    Parameters
    ----------
    df : DataFrame
        Final enriched RFM scores DataFrame.
    """
    print(f"[WRITE] Writing RFM scores to Delta Lake at: {TARGET_PATH}")

    (
        df.write
        .format("delta")
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
    Execute the full RFM customer segmentation pipeline for the iPhone 17
    launch campaign.

    Pipeline Steps
    --------------
    1. Create SparkSession with Delta Lake configuration.
    2. Ingest raw Amazon orders from S3 Parquet.
    3. Filter out CANCELLED / RETURNED orders and null-critical rows.
    4. Aggregate per-customer Recency, Frequency, and Monetary metrics.
    5. Enrich with ntile-based RFM scores and segment labels.
    6. Write results to Delta Lake (overwrite).
    """
    print("=" * 70)
    print(f"[PIPELINE] Starting: {APP_NAME}")
    print("=" * 70)

    spark = create_spark_session()
    print(f"[PIPELINE] Spark version: {spark.version}")

    try:
        # Step 1 – Ingest
        df_raw = read_orders(spark)

        # Step 2 – Filter
        df_filtered = filter_invalid_orders(df_raw)

        # Step 3 – Aggregate
        df_aggregated = compute_rfm_base_metrics(df_filtered)

        # Step 4 – Enrich
        df_enriched = enrich_rfm_scores(df_aggregated)

        # Step 5 – Write
        write_rfm_scores(df_enriched)

        print("=" * 70)
        print(f"[PIPELINE] Successfully completed: {APP_NAME}")
        print("=" * 70)

    except Exception as exc:  # noqa: BLE001
        print(f"[PIPELINE] FATAL ERROR: {exc}")
        raise

    finally:
        spark.stop()
        print("[PIPELINE] SparkSession stopped.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()