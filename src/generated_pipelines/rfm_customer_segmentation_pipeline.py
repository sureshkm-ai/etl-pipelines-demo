"""
rfm_customer_segmentation_pipeline: Reads raw transactions from S3, computes Recency,
Frequency, and Monetary scores per customer, and writes the result as a Delta table.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from delta import configure_spark_with_delta_pip


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SOURCE_PATH: str = "s3://etl-agent-raw-prod/amazon/orders/"
TARGET_PATH: str = "s3://etl-agent-artifacts-prod/analytics/rfm/"
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
        A fully configured SparkSession with Delta Lake extensions enabled.
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
        # S3 optimisations
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Step 1 – Ingest
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
        Raw transactions DataFrame.
    """
    print(f"[INGEST] Reading source Parquet data from: {path}")
    df = spark.read.parquet(path)
    row_count = df.count()
    print(f"[INGEST] Source rows loaded: {row_count:,}")
    print(f"[INGEST] Schema:\n{df._jdf.schema().treeString()}")
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
        Raw transactions DataFrame.

    Returns
    -------
    DataFrame
        Filtered DataFrame with non-null customer_id rows only.
    """
    print("[FILTER] Removing rows where customer_id IS NULL ...")
    df_filtered = df.filter(F.col("customer_id").isNotNull())
    row_count = df_filtered.count()
    print(f"[FILTER] Rows after filter: {row_count:,}")
    return df_filtered


# ---------------------------------------------------------------------------
# Step 3 – Aggregate (RFM base metrics)
# ---------------------------------------------------------------------------

def compute_rfm_base(df: DataFrame) -> DataFrame:
    """
    Group by customer_id and compute recency_days, frequency, and monetary.

    - recency_days : days since the customer's most recent order
                     (relative to the latest order date in the dataset).
    - frequency    : total number of distinct orders placed.
    - monetary     : total spend across all orders.

    Parameters
    ----------
    df : DataFrame
        Filtered transactions DataFrame. Expected columns:
        customer_id, order_id, order_date (date/timestamp), order_amount (numeric).

    Returns
    -------
    DataFrame
        One row per customer with columns:
        customer_id, recency_days, frequency, monetary.
    """
    print("[AGGREGATE] Computing RFM base metrics per customer ...")

    # Derive the reference date (latest order date in the dataset)
    max_date_row = df.agg(F.max(F.col("order_date")).alias("max_date")).collect()[0]
    max_date = max_date_row["max_date"]
    print(f"[AGGREGATE] Reference date (max order_date in dataset): {max_date}")

    df_agg = (
        df.groupBy("customer_id")
        .agg(
            # Recency: days between the customer's last order and the reference date
            F.datediff(
                F.lit(max_date),
                F.max(F.col("order_date")),
            ).alias("recency_days"),
            # Frequency: count of orders
            F.count(F.col("order_id")).alias("frequency"),
            # Monetary: total spend — coalesce guards against all-null order_amount
            F.coalesce(F.sum(F.col("order_amount")), F.lit(0.0)).alias("monetary"),
        )
    )

    row_count = df_agg.count()
    print(f"[AGGREGATE] Unique customers after aggregation: {row_count:,}")
    return df_agg


# ---------------------------------------------------------------------------
# Step 4 – Enrich (quintile scores + segment labels)
# ---------------------------------------------------------------------------

def enrich_rfm_scores(df: DataFrame) -> DataFrame:
    """
    Add quintile scores (r_score, f_score, m_score), a combined rfm_score,
    and a human-readable rfm_segment label to the aggregated customer DataFrame.

    Quintile logic
    --------------
    - r_score : ntile(5) ordered by recency_days ASC
                (lower recency_days → more recent → higher score)
    - f_score : ntile(5) ordered by frequency ASC
    - m_score : ntile(5) ordered by monetary ASC

    Segment thresholds (rfm_score = r + f + m, range 3–15)
    -------------------------------------------------------
    >= 13 → Champions
    >= 10 → Loyal Customers
    >=  7 → Potential Loyalists
    >=  4 → At Risk
    else  → Lost

    Parameters
    ----------
    df : DataFrame
        Aggregated RFM base metrics DataFrame.

    Returns
    -------
    DataFrame
        Enriched DataFrame with r_score, f_score, m_score, rfm_score,
        and rfm_segment columns appended.
    """
    print("[ENRICH] Computing quintile scores (r_score, f_score, m_score) ...")

    # Window specs – no partition, global ordering for ntile
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

    # Defensive null-fill for any edge-case nulls in derived columns
    df_enriched = df_enriched.fillna(
        {
            "r_score": 0,
            "f_score": 0,
            "m_score": 0,
            "rfm_score": 0,
            "rfm_segment": "Unknown",
        }
    )

    print("[ENRICH] Segment distribution:")
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
        Enriched RFM DataFrame to persist.
    path : str
        Target S3 Delta Lake path.
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
# Orchestrator
# ---------------------------------------------------------------------------

def run() -> None:
    """
    Execute the end-to-end RFM customer segmentation pipeline.

    Pipeline steps
    --------------
    1. Create SparkSession with Delta Lake configuration.
    2. Ingest raw order transactions from S3 (Parquet).
    3. Filter out rows with null customer_id.
    4. Aggregate per-customer recency, frequency, and monetary metrics.
    5. Enrich with quintile scores and segment labels.
    6. Write the result as a Delta table to S3.
    """
    print("=" * 70)
    print(f"[PIPELINE] Starting: {APP_NAME}")
    print("=" * 70)

    # Step 1 – Session
    spark = create_spark_session()
    print(f"[PIPELINE] Spark version: {spark.version}")

    try:
        # Step 2 – Ingest
        df_raw = read_source(spark, SOURCE_PATH)

        # Step 3 – Filter
        df_filtered = apply_filter(df_raw)

        # Step 4 – Aggregate
        df_rfm_base = compute_rfm_base(df_filtered)

        # Step 5 – Enrich
        df_enriched = enrich_rfm_scores(df_rfm_base)

        # Step 6 – Write
        write_delta(df_enriched, TARGET_PATH)

        print("=" * 70)
        print(f"[PIPELINE] {APP_NAME} completed successfully.")
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