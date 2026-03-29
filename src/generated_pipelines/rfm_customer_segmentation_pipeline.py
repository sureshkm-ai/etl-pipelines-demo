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
        # S3 optimisations
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
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
        Raw orders DataFrame.
    """
    print(f"[INGEST] Reading source Parquet data from: {path}")
    df = spark.read.parquet(path)
    print(f"[INGEST] Source schema:")
    df.printSchema()
    print(f"[INGEST] Source row count: {df.count():,}")
    return df


# ---------------------------------------------------------------------------
# Step 2 – Filter
# ---------------------------------------------------------------------------

def apply_filter(df: DataFrame) -> DataFrame:
    """
    Remove rows where customer_id is null.

    Satisfies acceptance criteria: every downstream record must have a valid
    customer identifier.

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
    row_count = df_filtered.count()
    print(f"[FILTER] Rows after filter: {row_count:,}")
    return df_filtered


# ---------------------------------------------------------------------------
# Step 3 – Aggregate (RFM base metrics)
# ---------------------------------------------------------------------------

def compute_rfm_base(df: DataFrame) -> DataFrame:
    """
    Aggregate orders per customer to derive Recency, Frequency, and Monetary
    base metrics.

    - recency_days : days since the customer's most recent order
                     (relative to the latest order date in the dataset)
    - frequency    : total number of distinct orders placed
    - monetary     : total spend across all orders

    Parameters
    ----------
    df : DataFrame
        Filtered orders DataFrame containing at minimum:
        customer_id (string), order_date (date/timestamp),
        order_id (string), order_amount (numeric).

    Returns
    -------
    DataFrame
        One row per customer with columns:
        customer_id, recency_days, frequency, monetary.
    """
    print("[AGGREGATE] Computing RFM base metrics per customer ...")

    # Determine the reference date (latest order date in the dataset)
    reference_date = df.agg(F.max("order_date").alias("max_date")).collect()[0]["max_date"]
    print(f"[AGGREGATE] Reference date for recency calculation: {reference_date}")

    df_agg = (
        df.groupBy("customer_id")
        .agg(
            # Recency: days between the customer's last order and the reference date
            F.datediff(
                F.lit(reference_date),
                F.max(F.col("order_date").cast("date")),
            ).alias("recency_days"),
            # Frequency: count of orders
            F.count("order_id").alias("frequency"),
            # Monetary: total spend – coalesce guards against null order_amount
            F.sum(F.coalesce(F.col("order_amount"), F.lit(0.0))).alias("monetary"),
        )
        # Ensure non-negative recency (handles same-day edge case)
        .withColumn("recency_days", F.greatest(F.col("recency_days"), F.lit(0)))
        # Ensure monetary is never negative
        .withColumn("monetary", F.greatest(F.col("monetary"), F.lit(0.0)))
    )

    print(f"[AGGREGATE] Distinct customers: {df_agg.count():,}")
    return df_agg


# ---------------------------------------------------------------------------
# Step 4 – Enrich (RFM scores & segment labels)
# ---------------------------------------------------------------------------

def enrich_rfm_scores(df: DataFrame) -> DataFrame:
    """
    Add quintile-based RFM scores and a human-readable segment label.

    Score logic
    -----------
    - r_score : ntile(5) over recency_days ASC
                (lower recency_days → more recent → higher score)
    - f_score : ntile(5) over frequency ASC
    - m_score : ntile(5) over monetary ASC
    - rfm_score   : r_score + f_score + m_score  (range 3–15)
    - rfm_segment : categorical label derived from rfm_score

    Segment thresholds
    ------------------
    ≥ 13 → Champions
    ≥ 10 → Loyal Customers
    ≥  7 → Potential Loyalists
    ≥  4 → At Risk
    else → Lost

    Parameters
    ----------
    df : DataFrame
        Aggregated RFM base metrics DataFrame.

    Returns
    -------
    DataFrame
        Enriched DataFrame with r_score, f_score, m_score,
        rfm_score, and rfm_segment columns appended.
    """
    print("[ENRICH] Computing RFM quintile scores ...")

    # Window specifications – no partition, ordered globally
    w_recency = Window.orderBy(F.col("recency_days").asc())
    w_frequency = Window.orderBy(F.col("frequency").asc())
    w_monetary = Window.orderBy(F.col("monetary").asc())

    df_scored = (
        df
        # Individual quintile scores
        .withColumn("r_score", F.ntile(5).over(w_recency))
        .withColumn("f_score", F.ntile(5).over(w_frequency))
        .withColumn("m_score", F.ntile(5).over(w_monetary))
        # Combined score
        .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))
    )

    print("[ENRICH] Assigning RFM segment labels ...")

    df_enriched = df_scored.withColumn(
        "rfm_segment",
        F.when(F.col("rfm_score") >= 13, "Champions")
        .when(F.col("rfm_score") >= 10, "Loyal Customers")
        .when(F.col("rfm_score") >= 7, "Potential Loyalists")
        .when(F.col("rfm_score") >= 4, "At Risk")
        .otherwise("Lost"),
    )

    print("[ENRICH] RFM segment distribution:")
    df_enriched.groupBy("rfm_segment").count().orderBy("rfm_segment").show(truncate=False)

    return df_enriched


# ---------------------------------------------------------------------------
# Step 5 – Write
# ---------------------------------------------------------------------------

def write_delta(df: DataFrame, path: str) -> None:
    """
    Persist the enriched RFM DataFrame as a Delta Lake table using overwrite mode.

    Parameters
    ----------
    df : DataFrame
        Final enriched RFM DataFrame.
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

    print(f"[WRITE] Delta write complete → {path}")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run() -> None:
    """
    Execute the end-to-end RFM Customer Segmentation pipeline.

    Pipeline stages
    ---------------
    1. Create SparkSession
    2. Ingest raw orders from S3 (Parquet)
    3. Filter: drop rows with null customer_id
    4. Aggregate: compute recency_days, frequency, monetary per customer
    5. Enrich: add r_score, f_score, m_score, rfm_score, rfm_segment
    6. Write: persist as Delta Lake table (overwrite)
    """
    print("=" * 70)
    print(f"[PIPELINE] Starting: {APP_NAME}")
    print("=" * 70)

    # 1. Session
    spark = create_spark_session()
    print(f"[PIPELINE] Spark version: {spark.version}")

    try:
        # 2. Ingest
        df_raw = read_source(spark, SOURCE_PATH)

        # 3. Filter
        df_filtered = apply_filter(df_raw)

        # 4. Aggregate
        df_aggregated = compute_rfm_base(df_filtered)

        # 5. Enrich
        df_enriched = enrich_rfm_scores(df_aggregated)

        # 6. Write
        write_delta(df_enriched, TARGET_PATH)

        print("=" * 70)
        print(f"[PIPELINE] {APP_NAME} completed successfully.")
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