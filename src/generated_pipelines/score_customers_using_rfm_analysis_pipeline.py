"""
score_customers_using_rfm_analysis_pipeline: Compute RFM (Recency, Frequency, Monetary)
scores for all customers using quintile bucketing to support targeted marketing campaigns.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SOURCE_PATH: str = "s3://etl-agent-raw/amazon_orders.parquet"
TARGET_PATH: str = "s3://etl-agent-processed/rfm_scores"
APP_NAME: str = "score_customers_using_rfm_analysis_pipeline"


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
        # Adaptive query execution for better performance on skewed data
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Step 1 – Read
# ---------------------------------------------------------------------------

def read_source(spark: SparkSession) -> DataFrame:
    """
    Read raw Amazon orders from Parquet.

    Parameters
    ----------
    spark : SparkSession
        Active SparkSession.

    Returns
    -------
    DataFrame
        Raw orders DataFrame.
    """
    print(f"[READ] Reading source data from: {SOURCE_PATH}")
    df = spark.read.parquet(SOURCE_PATH)
    row_count = df.count()
    print(f"[READ] Source rows loaded: {row_count:,}")
    print(f"[READ] Schema:\n{df._jdf.schema().treeString()}")
    return df


# ---------------------------------------------------------------------------
# Step 2 – Validate / Null Handling
# ---------------------------------------------------------------------------

def validate_and_clean(df: DataFrame) -> DataFrame:
    """
    Drop rows with null values in critical columns required for RFM computation.

    Columns validated: customer_id, order_id, order_date, order_amount.

    Parameters
    ----------
    df : DataFrame
        Raw orders DataFrame.

    Returns
    -------
    DataFrame
        Cleaned DataFrame with nulls removed from critical columns.
    """
    print("[VALIDATE] Applying null-value guards on critical columns...")

    critical_columns = ["customer_id", "order_id", "order_date", "order_amount"]

    # Report null counts per critical column before dropping
    for col_name in critical_columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        print(f"[VALIDATE]   Null count in '{col_name}': {null_count:,}")

    df_clean = df.dropna(subset=critical_columns)

    # Ensure order_amount is non-negative (guard against data quality issues)
    df_clean = df_clean.filter(F.col("order_amount") >= 0)

    # Cast order_date to DateType if it is not already
    df_clean = df_clean.withColumn(
        "order_date", F.col("order_date").cast("date")
    )

    rows_after = df_clean.count()
    print(f"[VALIDATE] Rows after cleaning: {rows_after:,}")
    return df_clean


# ---------------------------------------------------------------------------
# Step 3 – Aggregate (Recency, Frequency, Monetary)
# ---------------------------------------------------------------------------

def aggregate_rfm_metrics(df: DataFrame) -> DataFrame:
    """
    Group by customer_id and compute the three raw RFM metrics.

    - recency_days : days between today and the customer's most recent order
    - frequency    : total number of distinct orders placed
    - monetary     : total spend across all orders

    Parameters
    ----------
    df : DataFrame
        Cleaned orders DataFrame.

    Returns
    -------
    DataFrame
        Aggregated DataFrame with one row per customer.
    """
    print("[AGGREGATE] Computing RFM metrics per customer...")

    df_rfm = df.groupBy("customer_id").agg(
        F.expr("datediff(current_date(), MAX(order_date))").alias("recency_days"),
        F.count("order_id").alias("frequency"),
        F.sum("order_amount").alias("monetary"),
    )

    # Null-safe defaults: if aggregation somehow yields nulls, coerce to safe values
    df_rfm = (
        df_rfm
        .withColumn("recency_days", F.coalesce(F.col("recency_days"), F.lit(9999)))
        .withColumn("frequency", F.coalesce(F.col("frequency"), F.lit(0)))
        .withColumn("monetary", F.coalesce(F.col("monetary"), F.lit(0.0)))
    )

    customer_count = df_rfm.count()
    print(f"[AGGREGATE] Unique customers after aggregation: {customer_count:,}")
    return df_rfm


# ---------------------------------------------------------------------------
# Step 4 – Enrich (Quintile Scores + Segment Labels)
# ---------------------------------------------------------------------------

def enrich_with_rfm_scores(df: DataFrame) -> DataFrame:
    """
    Add quintile-based R, F, M scores (1–5) and derive the combined RFM score
    and segment label.

    Scoring logic
    -------------
    - r_score : ntile(5) ordered by recency_days DESC
                (higher recency_days = less recent = lower score)
    - f_score : ntile(5) ordered by frequency ASC
                (higher frequency = more loyal = higher score)
    - m_score : ntile(5) ordered by monetary ASC
                (higher spend = more valuable = higher score)
    - rfm_score   : r_score + f_score + m_score  (range 3–15)
    - rfm_segment : categorical label derived from rfm_score

    Parameters
    ----------
    df : DataFrame
        Aggregated RFM metrics DataFrame.

    Returns
    -------
    DataFrame
        Enriched DataFrame with score and segment columns appended.
    """
    print("[ENRICH] Computing quintile scores (r_score, f_score, m_score)...")

    # Window specifications – no partition, global ordering for ntile
    w_recency = Window.orderBy(F.col("recency_days").desc())
    w_frequency = Window.orderBy(F.col("frequency").asc())
    w_monetary = Window.orderBy(F.col("monetary").asc())

    df_scored = (
        df
        .withColumn("r_score", F.ntile(5).over(w_recency))
        .withColumn("f_score", F.ntile(5).over(w_frequency))
        .withColumn("m_score", F.ntile(5).over(w_monetary))
    )

    print("[ENRICH] Computing combined rfm_score...")
    df_scored = df_scored.withColumn(
        "rfm_score",
        F.col("r_score") + F.col("f_score") + F.col("m_score"),
    )

    print("[ENRICH] Assigning rfm_segment labels...")
    df_scored = df_scored.withColumn(
        "rfm_segment",
        F.when(F.col("rfm_score") >= 13, F.lit("Champions"))
        .when(F.col("rfm_score") >= 10, F.lit("Loyal Customers"))
        .when(F.col("rfm_score") >= 7, F.lit("Potential Loyalists"))
        .when(F.col("rfm_score") >= 4, F.lit("At Risk"))
        .otherwise(F.lit("Lost")),
    )

    # Add pipeline metadata columns
    df_scored = df_scored.withColumn(
        "pipeline_run_date", F.current_date()
    ).withColumn(
        "pipeline_name", F.lit(APP_NAME)
    )

    print("[ENRICH] Segment distribution:")
    df_scored.groupBy("rfm_segment").count().orderBy("rfm_segment").show(
        truncate=False
    )

    return df_scored


# ---------------------------------------------------------------------------
# Step 5 – Write
# ---------------------------------------------------------------------------

def write_delta(df: DataFrame) -> None:
    """
    Persist the enriched RFM DataFrame to Delta Lake using overwrite mode.

    Parameters
    ----------
    df : DataFrame
        Final enriched RFM DataFrame to persist.
    """
    print(f"[WRITE] Writing RFM scores to Delta Lake at: {TARGET_PATH}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")   # allow schema evolution on reruns
        .save(TARGET_PATH)
    )

    print("[WRITE] Delta write complete.")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run() -> None:
    """
    Execute the full score_customers_using_rfm_analysis_pipeline.

    Steps
    -----
    1. Create SparkSession
    2. Read raw orders from Parquet
    3. Validate and clean critical columns
    4. Aggregate RFM metrics per customer
    5. Enrich with quintile scores and segment labels
    6. Write results to Delta Lake
    """
    print("=" * 70)
    print(f"[PIPELINE] Starting: {APP_NAME}")
    print("=" * 70)

    spark = create_spark_session()

    try:
        # Step 1 – Read
        df_raw = read_source(spark)

        # Step 2 – Validate / Clean
        df_clean = validate_and_clean(df_raw)

        # Step 3 – Aggregate
        df_aggregated = aggregate_rfm_metrics(df_clean)

        # Step 4 – Enrich
        df_enriched = enrich_with_rfm_scores(df_aggregated)

        # Step 5 – Write
        write_delta(df_enriched)

        print("=" * 70)
        print(f"[PIPELINE] Completed successfully: {APP_NAME}")
        print("=" * 70)

    except Exception as exc:
        print(f"[PIPELINE] FAILED with error: {exc}")
        raise

    finally:
        spark.stop()
        print("[PIPELINE] SparkSession stopped.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()