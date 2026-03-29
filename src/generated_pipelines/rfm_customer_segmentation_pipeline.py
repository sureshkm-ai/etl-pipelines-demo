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
        # Delta Lake write optimisations
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[{APP_NAME}] SparkSession created successfully.")
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
        Raw transactions DataFrame.
    """
    print(f"[{APP_NAME}] Reading source data from: {path}")
    df: DataFrame = spark.read.parquet(path)
    row_count: int = df.count()
    print(f"[{APP_NAME}] Source rows loaded: {row_count:,}")
    print(f"[{APP_NAME}] Source schema:")
    df.printSchema()
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
        Filtered DataFrame with non-null customer_id values only.
    """
    print(f"[{APP_NAME}] Applying filter: customer_id IS NOT NULL ...")
    df_filtered: DataFrame = df.filter(F.col("customer_id").isNotNull())
    filtered_count: int = df_filtered.count()
    print(f"[{APP_NAME}] Rows after filter: {filtered_count:,}")
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
        Filtered transactions DataFrame.

    Returns
    -------
    DataFrame
        Aggregated DataFrame with one row per customer.
    """
    print(f"[{APP_NAME}] Computing RFM aggregation metrics ...")

    # Determine the reference date (latest order_date in the dataset)
    max_date_row = df.agg(F.max(F.col("order_date")).alias("max_date")).collect()[0]
    max_date = max_date_row["max_date"]
    print(f"[{APP_NAME}] Reference date for recency calculation: {max_date}")

    df_agg: DataFrame = df.groupBy("customer_id").agg(
        # recency_days: days between the customer's last order and the reference date
        F.datediff(
            F.lit(max_date),
            F.max(F.col("order_date"))
        ).alias("recency_days"),
        # frequency: count of orders
        F.count(F.col("order_id")).alias("frequency"),
        # monetary: sum of order amounts, coalesce nulls to 0
        F.sum(F.coalesce(F.col("order_amount"), F.lit(0.0))).alias("monetary"),
    )

    # Ensure non-negative recency (guard against future-dated records)
    df_agg = df_agg.withColumn(
        "recency_days",
        F.greatest(F.col("recency_days"), F.lit(0))
    )

    agg_count: int = df_agg.count()
    print(f"[{APP_NAME}] Unique customers after aggregation: {agg_count:,}")
    return df_agg


# ---------------------------------------------------------------------------
# Step 4 – Enrich (RFM Scores & Segment Labels)
# ---------------------------------------------------------------------------

def apply_enrichment(df: DataFrame) -> DataFrame:
    """
    Add quintile-based RFM scores and a human-readable segment label.

    Derived columns
    ---------------
    r_score     : ntile(5) over recency_days ASC  (lower recency → higher score)
    f_score     : ntile(5) over frequency ASC
    m_score     : ntile(5) over monetary ASC
    rfm_score   : r_score + f_score + m_score  (range 3–15)
    rfm_segment : categorical label derived from rfm_score

    Parameters
    ----------
    df : DataFrame
        Aggregated RFM base-metrics DataFrame.

    Returns
    -------
    DataFrame
        Enriched DataFrame with all RFM score columns and segment label.
    """
    print(f"[{APP_NAME}] Enriching data with RFM quintile scores ...")

    # Window specifications – no partition, ordered globally
    w_recency: Window = Window.orderBy(F.col("recency_days").asc())
    w_frequency: Window = Window.orderBy(F.col("frequency").asc())
    w_monetary: Window = Window.orderBy(F.col("monetary").asc())

    # Step 4a – compute individual quintile scores
    df_scored: DataFrame = (
        df
        .withColumn("r_score", F.ntile(5).over(w_recency))
        .withColumn("f_score", F.ntile(5).over(w_frequency))
        .withColumn("m_score", F.ntile(5).over(w_monetary))
    )

    # Step 4b – combined RFM score
    df_scored = df_scored.withColumn(
        "rfm_score",
        F.col("r_score") + F.col("f_score") + F.col("m_score")
    )

    # Step 4c – segment label
    df_enriched: DataFrame = df_scored.withColumn(
        "rfm_segment",
        F.when(F.col("rfm_score") >= 13, F.lit("Champions"))
         .when(F.col("rfm_score") >= 10, F.lit("Loyal Customers"))
         .when(F.col("rfm_score") >= 7,  F.lit("Potential Loyalists"))
         .when(F.col("rfm_score") >= 4,  F.lit("At Risk"))
         .otherwise(F.lit("Lost"))
    )

    print(f"[{APP_NAME}] RFM segment distribution:")
    df_enriched.groupBy("rfm_segment").count().orderBy("rfm_segment").show(truncate=False)

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
    print(f"[{APP_NAME}] Writing output to Delta Lake at: {path}")
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .save(path)
    )
    print(f"[{APP_NAME}] Delta write complete.")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run() -> None:
    """
    Execute the full RFM customer segmentation pipeline.

    Pipeline steps
    --------------
    1. Create SparkSession
    2. Read raw Parquet transactions from S3
    3. Filter out null customer_id rows
    4. Aggregate to per-customer RFM base metrics
    5. Enrich with quintile scores and segment labels
    6. Write enriched data to Delta Lake
    """
    print(f"[{APP_NAME}] ========== Pipeline START ==========")

    # 1. Session
    spark: SparkSession = create_spark_session()

    try:
        # 2. Read
        df_raw: DataFrame = read_source(spark, SOURCE_PATH)

        # 3. Filter
        df_filtered: DataFrame = apply_filter(df_raw)

        # 4. Aggregate
        df_aggregated: DataFrame = apply_aggregation(df_filtered)

        # 5. Enrich
        df_enriched: DataFrame = apply_enrichment(df_aggregated)

        # 6. Write
        write_delta(df_enriched, TARGET_PATH)

        final_count: int = df_enriched.count()
        print(f"[{APP_NAME}] Total records written: {final_count:,}")

    except Exception as exc:  # noqa: BLE001
        print(f"[{APP_NAME}] PIPELINE FAILED with error: {exc}")
        raise

    finally:
        print(f"[{APP_NAME}] ========== Pipeline END ==========")
        spark.stop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()