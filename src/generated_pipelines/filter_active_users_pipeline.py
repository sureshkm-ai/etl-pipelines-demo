"""filter_active_users_pipeline: Filter users parquet data to retain only active users and write to the processed bucket."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip


def create_spark_session() -> SparkSession:
    """
    Create and configure a SparkSession with Delta Lake support.

    Returns:
        SparkSession: Configured Spark session instance.
    """
    spark = (
        configure_spark_with_delta_pip(
            SparkSession.builder.appName("filter_active_users_pipeline")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        ).getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_source(spark: SparkSession, source_path: str) -> DataFrame:
    """
    Read source parquet data from S3.

    Args:
        spark: Active SparkSession instance.
        source_path: S3 path to the source parquet files.

    Returns:
        DataFrame: Raw source DataFrame.
    """
    print(f"[READ] Reading source parquet data from: {source_path}")
    df = spark.read.parquet(source_path)
    print(f"[READ] Source row count: {df.count()}")
    print(f"[READ] Source schema: {df.schema.simpleString()}")
    return df


def apply_filter_active_users(df: DataFrame) -> DataFrame:
    """
    Retain only rows where status is equal to 'active'.

    Uses a SQL string expression for the filter condition to ensure
    compatibility regardless of SparkContext state at import time.

    Args:
        df: Input DataFrame containing user records.

    Returns:
        DataFrame: Filtered DataFrame containing only active users.
    """
    condition = "status = 'active'"
    print(f"[TRANSFORM] Applying filter: Retain only rows where {condition}")
    df_filtered = df.filter(condition)
    return df_filtered


def handle_nulls(df: DataFrame) -> DataFrame:
    """
    Handle null values gracefully by dropping rows with null in critical columns.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame: DataFrame with null handling applied.
    """
    print("[TRANSFORM] Handling null values in critical columns...")
    # Drop rows where 'status' column is null to avoid ambiguous filter results
    df_clean = df.filter(F.col("status").isNotNull())
    return df_clean


def write_target(df: DataFrame, target_path: str) -> None:
    """
    Write the processed DataFrame to the target S3 path in parquet format
    using overwrite mode.

    Args:
        df: Processed DataFrame to write.
        target_path: S3 path for the output parquet files.
    """
    print(f"[WRITE] Writing output to: {target_path}")
    print(f"[WRITE] Output row count: {df.count()}")
    (
        df.write
        .format("parquet")
        .mode("overwrite")
        .save(target_path)
    )
    print(f"[WRITE] Successfully written to: {target_path}")


def run() -> None:
    """
    Execute the filter_active_users_pipeline end-to-end.

    Pipeline steps:
        1. Create SparkSession with Delta Lake configuration.
        2. Read raw user parquet data from S3 source.
        3. Handle null values in critical columns.
        4. Filter to retain only active users.
        5. Write processed data to S3 target in parquet format.
    """
    print("[PIPELINE] Starting filter_active_users_pipeline")

    # Step 1: Initialise Spark
    print("[PIPELINE] Step 1/5 - Initialising SparkSession...")
    spark = create_spark_session()
    print("[PIPELINE] SparkSession initialised successfully.")

    source_path = "s3://etl-agent-raw/users/"
    target_path = "s3://etl-agent-processed/active-users/"

    try:
        # Step 2: Read source data
        print("[PIPELINE] Step 2/5 - Reading source data...")
        df_raw = read_source(spark, source_path)

        # Step 3: Handle nulls
        print("[PIPELINE] Step 3/5 - Handling null values...")
        df_no_nulls = handle_nulls(df_raw)

        # Step 4: Apply filter transformation
        print("[PIPELINE] Step 4/5 - Applying filter transformation...")
        df_active = apply_filter_active_users(df_no_nulls)

        # Step 5: Write output
        print("[PIPELINE] Step 5/5 - Writing output data...")
        write_target(df_active, target_path)

        print("[PIPELINE] filter_active_users_pipeline completed successfully.")

    except Exception as exc:
        print(f"[PIPELINE] Pipeline failed with error: {exc}")
        raise

    finally:
        spark.stop()
        print("[PIPELINE] SparkSession stopped.")


if __name__ == "__main__":
    run()