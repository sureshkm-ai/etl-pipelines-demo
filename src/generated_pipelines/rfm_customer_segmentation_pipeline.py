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
        # Delta Lake write optimisations
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
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
    print(f"[READ] Schema:\n{df._jdf.schema().treeString()}")
    print(f"[READ] Source row count: {df.count():,}")
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
    print("[FILTER] Removing rows where customer_id IS NULL …")
    df_filtered = df.filter(F.col("customer_id").isNotNull())
    filtered_count = df_filtered.count()
    print(f"[FILTER] Rows after filter: {filtered_count:,}")
    return df_filtered


# ---------------------------------------------------------------------------
# Step 3 – Aggregate (Recency / Frequency / Monetary)
# ---------------------------------------------------------------------------

def apply_aggregation(df: DataFrame) -> DataFrame:
    """
    Group by customer_id and compute RFM base metrics.

    Metrics computed
    ----------------
    recency_days : int
        Number of days since the customer's most recent order, measured
        relative to the most recent order date in the dataset
        (i.e. ``max(order_date)`` across all customers).
    frequency : long
        Total number of distinct orders placed by the customer.
    monetary : double
        Total spend (sum of order_amount) by the customer.

    Parameters
    ----------
    df : DataFrame
        Filtered orders DataFrame.

    Returns
    -------
    DataFrame
        Aggregated DataFrame with one row per customer.
    """
    print("[AGGREGATE] Computing recency_days, frequency, and monetary per customer …")

    # Derive the reference date (latest order date in the dataset) once so
    # that recency is consistent across all customers.
    max_date_row = df.agg(F.max(F.col("order_date")).alias("max_order_date")).collect()
    max_order_date = max_date_row[0]["max_order_date"]
    print(f"[AGGREGATE] Reference date for recency calculation: {max_order_date}")

    df_agg = (
        df.groupBy("customer_id")
        .agg(
            # Recency: days between the customer's latest order and the global max date
            F.datediff(
                F.lit(max_order_date),
                F.max(F.col("order_date")),
            ).alias("recency_days"),
            # Frequency: count of orders
            F.count(F.col("order_id")).alias("frequency"),
            # Monetary: total spend – coerce nulls to 0 before summing
            F.sum(F.coalesce(F.col("order_amount"), F.lit(0.0))).alias("monetary"),
        )
    )

    # Defensive null handling on aggregated columns
    df_agg = (
        df_agg
        .withColumn("recency_days", F.coalesce(F.col("recency_days"), F.lit(0)))
        .withColumn("frequency", F.coalesce(F.col("frequency"), F.lit(0)))
        .withColumn("monetary", F.coalesce(F.col("monetary"), F.lit(0.0)))
    )

    agg_count = df_agg.count()
    print(f"[AGGREGATE] Distinct customers after aggregation: {agg_count:,}")
    return df_agg


# ---------------------------------------------------------------------------
# Step 4 – Enrich (RFM Scores & Segment Labels)
# ---------------------------------------------------------------------------

def apply_enrichment(df: DataFrame) -> DataFrame:
    """
    Add quintile-based RFM scores and a human-readable segment label.

    Derived columns
    ---------------
    r_score : int  (1–5)
        Recency quintile.  Lower recency_days → higher score (better).
        ``ntile(5) OVER (ORDER BY recency_days ASC)`` means customers with
        the *smallest* recency_days (most recent) receive score 1, which is
        intentional for the combined rfm_score calculation below where a
        *lower* r_score is better.

        .. note::
            The expression in the spec uses ``ORDER BY recency_days ASC``
            which assigns ntile=1 to the *most recent* customers.  The
            combined ``rfm_score`` therefore treats lower r_score as better
            recency.  This is preserved faithfully from the spec.

    f_score : int  (1–5)
        Frequency quintile.  Higher frequency → higher score.
    m_score : int  (1–5)
        Monetary quintile.  Higher spend → higher score.
    rfm_score : int  (3–15)
        Sum of r_score + f_score + m_score.
    rfm_segment : str
        Human-readable segment derived from rfm_score thresholds.

    Parameters
    ----------
    df : DataFrame
        Aggregated RFM base-metrics DataFrame.

    Returns
    -------
    DataFrame
        Enriched DataFrame with all RFM score and segment columns appended.
    """
    print("[ENRICH] Computing r_score, f_score, m_score quintiles …")

    # Window specifications (no partition – global quintiles)
    w_recency = Window.orderBy(F.col("recency_days").asc())
    w_frequency = Window.orderBy(F.col("frequency").asc())
    w_monetary = Window.orderBy(F.col("monetary").asc())

    df_scored = (
        df
        .withColumn("r_score", F.ntile(5).over(w_recency))
        .withColumn("f_score", F.ntile(5).over(w_frequency))
        .withColumn("m_score", F.ntile(5).over(w_monetary))
    )

    print("[ENRICH] Computing combined rfm_score …")
    df_scored = df_scored.withColumn(
        "rfm_score",
        F.col("r_score") + F.col("f_score") + F.col("m_score"),
    )

    print("[ENRICH] Assigning rfm_segment labels …")
    df_enriched = df_scored.withColumn(
        "rfm_segment",
        F.when(F.col("rfm_score") >= 13, F.lit("Champions"))
        .when(F.col("rfm_score") >= 10, F.lit("Loyal Customers"))
        .when(F.col("rfm_score") >= 7, F.lit("Potential Loyalists"))
        .when(F.col("rfm_score") >= 4, F.lit("At Risk"))
        .otherwise(F.lit("Lost")),
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
        Final enriched DataFrame to persist.
    path : str
        S3 destination path for the Delta table.
    """
    print(f"[WRITE] Writing {df.count():,} rows to Delta Lake at: {path}")
    (
        df.write.format("delta")
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
    Execute the end-to-end RFM customer segmentation pipeline.

    Pipeline steps
    --------------
    1. Create SparkSession with Delta Lake configuration.
    2. Read raw Parquet orders from S3.
    3. Filter out records with null customer_id.
    4. Aggregate per-customer recency, frequency, and monetary metrics.
    5. Enrich with quintile scores and segment labels.
    6. Write the result as a Delta table to S3.
    """
    print("=" * 70)
    print(f"[PIPELINE] Starting: {APP_NAME}")
    print("=" * 70)

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

        print("=" * 70)
        print(f"[PIPELINE] {APP_NAME} finished successfully.")
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