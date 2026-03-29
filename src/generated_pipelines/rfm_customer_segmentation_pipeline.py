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
    print(f"[READ] Source schema:")
    df.printSchema()
    print(f"[READ] Source row count: {df.count():,}")
    return df


# ---------------------------------------------------------------------------
# Step 2 – Filter
# ---------------------------------------------------------------------------

def apply_filter(df: DataFrame) -> DataFrame:
    """
    Remove rows where customer_id is null.

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
# Step 3 – Aggregate (Recency, Frequency, Monetary)
# ---------------------------------------------------------------------------

def apply_aggregation(df: DataFrame) -> DataFrame:
    """
    Group by customer_id and compute RFM base metrics.

    - recency_days : days since the customer's most recent order
                     (relative to the latest order_date in the dataset)
    - frequency    : total number of distinct orders
    - monetary     : total spend across all orders

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

    # Determine the reference date (max order_date in the dataset)
    max_date_row = df.agg(F.max(F.col("order_date")).alias("max_date")).collect()[0]
    max_date = max_date_row["max_date"]
    print(f"[AGGREGATE] Reference date for recency calculation: {max_date}")

    df_agg = (
        df.groupBy("customer_id")
        .agg(
            # Recency: days between the customer's latest order and the global max date
            F.datediff(
                F.lit(max_date),
                F.max(F.col("order_date")),
            ).alias("recency_days"),
            # Frequency: count of orders
            F.count(F.col("order_id")).alias("frequency"),
            # Monetary: total spend (nulls treated as 0 before summing)
            F.sum(F.coalesce(F.col("order_amount"), F.lit(0.0))).alias("monetary"),
        )
    )

    # Defensive null handling for aggregated columns
    df_agg = (
        df_agg
        .withColumn("recency_days", F.coalesce(F.col("recency_days"), F.lit(0)))
        .withColumn("frequency", F.coalesce(F.col("frequency"), F.lit(0)))
        .withColumn("monetary", F.coalesce(F.col("monetary"), F.lit(0.0)))
    )

    print(f"[AGGREGATE] Distinct customers after aggregation: {df_agg.count():,}")
    return df_agg


# ---------------------------------------------------------------------------
# Step 4 – Enrich (RFM Scores & Segment Label)
# ---------------------------------------------------------------------------

def apply_enrichment(df: DataFrame) -> DataFrame:
    """
    Add quintile-based RFM scores and a human-readable segment label.

    Scoring logic
    -------------
    - r_score : ntile(5) ordered by recency_days DESC
                (lower recency = more recent = higher score)
    - f_score : ntile(5) ordered by frequency ASC
                (higher frequency = higher score; ASC so ntile assigns 1 to lowest)
    - m_score : ntile(5) ordered by monetary ASC
                (higher spend = higher score)
    - rfm_score   : r_score + f_score + m_score  (range 3–15)
    - rfm_segment : categorical label derived from rfm_score

    Parameters
    ----------
    df : DataFrame
        Aggregated RFM base-metrics DataFrame.

    Returns
    -------
    DataFrame
        Enriched DataFrame with score and segment columns appended.
    """
    print("[ENRICH] Computing r_score, f_score, m_score quintiles ...")

    # Window specifications (no partition – global ranking across all customers)
    w_recency = Window.orderBy(F.col("recency_days").desc())
    w_frequency = Window.orderBy(F.col("frequency").asc())
    w_monetary = Window.orderBy(F.col("monetary").asc())

    df_scored = (
        df
        .withColumn("r_score", F.ntile(5).over(w_recency))
        .withColumn("f_score", F.ntile(5).over(w_frequency))
        .withColumn("m_score", F.ntile(5).over(w_monetary))
    )

    print("[ENRICH] Computing rfm_score (sum of individual scores) ...")
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

    print("[ENRICH] Segment distribution:")
    df_enriched.groupBy("rfm_segment").count().orderBy("count", ascending=False).show(
        truncate=False
    )

    return df_enriched


# ---------------------------------------------------------------------------
# Step 5 – Write
# ---------------------------------------------------------------------------

def write_delta(df: DataFrame, path: str) -> None:
    """
    Write the enriched RFM DataFrame to Delta Lake in overwrite mode.

    Parameters
    ----------
    df : DataFrame
        Final enriched DataFrame to persist.
    path : str
        Target S3 Delta Lake path.
    """
    print(f"[WRITE] Writing {df.count():,} rows to Delta Lake at: {path}")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
    print("[WRITE] Delta write complete.")


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------

def run() -> None:
    """
    Orchestrate the full RFM customer segmentation ETL pipeline.

    Steps
    -----
    1. Create SparkSession
    2. Read raw orders from S3 (Parquet)
    3. Filter: remove null customer_id rows
    4. Aggregate: compute recency_days, frequency, monetary per customer
    5. Enrich: add r_score, f_score, m_score, rfm_score, rfm_segment
    6. Write enriched data to Delta Lake (overwrite)
    """
    print("=" * 70)
    print(f"[PIPELINE] Starting: {APP_NAME}")
    print("=" * 70)

    # 1. SparkSession
    print("[INIT] Creating SparkSession ...")
    spark = create_spark_session()
    print(f"[INIT] Spark version: {spark.version}")

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

        print("=" * 70)
        print(f"[PIPELINE] {APP_NAME} completed successfully.")
        print("=" * 70)

    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] Pipeline failed: {exc}")
        raise

    finally:
        spark.stop()
        print("[INIT] SparkSession stopped.")


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()