"""filter_active_users_pipeline: Filter users parquet data to retain only active users and write to the processed bucket."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip


def create_spark_session(app_name: str) -> SparkSession:
    """
    Create and configure a SparkSession with Delta Lake support.

    Args:
        app_name: The name of the Spark application.

    Returns:
        A configured SparkSession instance.
    """
    spark = (
        configure_spark_with_delta_pip(
            SparkSession.builder.appName(app_name)
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
    return spark


def read_source(spark: SparkSession, source_path: str) -> DataFrame:
    """
    Read source parquet data from S3.

    Args:
        spark: Active SparkSession instance.
        source_path: S3 path to the source parquet files.

    Returns:
        DataFrame containing the raw source data.
    """
    print(f"[READ] Reading source parquet data from: {source_path}")
    df = spark.read.parquet(source_path)
    row_count = df.count()
    print(f"[READ] Source schema:")
    df.printSchema()
    print(f"[READ] Source row count: {row_count:,}")
    return df


def validate_source(df: DataFrame) -> None:
    """
    Validate that the source DataFrame contains the expected columns.

    Args:
        df: Source DataFrame to validate.

    Raises:
        ValueError: If required columns are missing from the source data.
    """
    print("[VALIDATE] Validating source data schema...")
    required_columns = ["status"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"[VALIDATE] Missing required columns in source data: {missing_columns}. "
            f"Available columns: {df.columns}"
        )
    print(f"[VALIDATE] Schema validation passed. Required columns present: {required_columns}")

    null_status_count = df.filter(F.col("status").isNull()).count()
    print(f"[VALIDATE] Rows with null 'status' (will be excluded by filter): {null_status_count:,}")


def apply_filter_active_users(df: DataFrame) -> DataFrame:
    """
    Filter the DataFrame to retain only rows where status equals 'active'.

    Null values in the 'status' column are handled gracefully and excluded
    from the output, as they do not satisfy the equality condition.

    Args:
        df: Input DataFrame containing user records.

    Returns:
        Filtered DataFrame containing only active users.
    """
    print("[TRANSFORM] Applying filter: Retain only rows where status = 'active'")
    filter_condition = "status = 'active'"
    df_filtered = df.filter(F.expr(filter_condition))
    filtered_count = df_filtered.count()
    print(f"[TRANSFORM] Filter condition applied: {filter_condition}")
    print(f"[TRANSFORM] Rows after filter: {filtered_count:,}")
    return df_filtered


def write_target(df: DataFrame, target_path: str) -> None:
    """
    Write the transformed DataFrame to the target S3 location in parquet format
    using Delta Lake overwrite mode.

    Args:
        df: Transformed DataFrame to write.
        target_path: S3 path for the target output location.
    """
    print(f"[WRITE] Writing output to: {target_path}")
    print(f"[WRITE] Write mode: overwrite | Format: parquet")
    (
        df.write
        .format("parquet")
        .mode("overwrite")
        .save(target_path)
    )
    print(f"[WRITE] Successfully written data to: {target_path}")


def run() -> None:
    """
    Execute the filter_active_users_pipeline end-to-end.

    Pipeline Steps:
        1. Create SparkSession with Delta Lake configuration.
        2. Read raw user data from S3 (parquet).
        3. Validate source schema and data quality.
        4. Apply filter to retain only active users (status = 'active').
        5. Write filtered data to the processed S3 bucket (parquet, overwrite).
        6. Stop SparkSession.
    """
    pipeline_name = "filter_active_users_pipeline"
    source_path = "s3://etl-agent-raw/users/"
    target_path = "s3://etl-agent-processed/active-users/"

    print(f"[PIPELINE] Starting pipeline: {pipeline_name}")
    print(f"[PIPELINE] Source: {source_path}")
    print(f"[PIPELINE] Target: {target_path}")

    # Step 1: Create SparkSession
    print("[PIPELINE] Step 1/5 - Initialising SparkSession...")
    spark = create_spark_session(pipeline_name)
    print(f"[PIPELINE] SparkSession created. Spark version: {spark.version}")

    try:
        # Step 2: Read source data
        print("[PIPELINE] Step 2/5 - Reading source data...")
        df_raw = read_source(spark, source_path)

        # Step 3: Validate source data
        print("[PIPELINE] Step 3/5 - Validating source data...")
        validate_source(df_raw)

        # Step 4: Apply transformations
        print("[PIPELINE] Step 4/5 - Applying transformations...")
        df_active_users = apply_filter_active_users(df_raw)

        # Step 5: Write output
        print("[PIPELINE] Step 5/5 - Writing output data...")
        write_target(df_active_users, target_path)

        print(f"[PIPELINE] Pipeline '{pipeline_name}' completed successfully.")

    except Exception as e:
        print(f"[PIPELINE] ERROR: Pipeline '{pipeline_name}' failed with exception: {e}")
        raise

    finally:
        print("[PIPELINE] Stopping SparkSession...")
        spark.stop()
        print("[PIPELINE] SparkSession stopped.")


if __name__ == "__main__":
    run()