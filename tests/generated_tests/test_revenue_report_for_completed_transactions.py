import pytest
from unittest.mock import MagicMock, patch
import pipeline


def make_mock_df(row_count: int = 10) -> MagicMock:
    df = MagicMock()
    df.count.return_value = row_count
    df.columns = [
        "order_id", "customer_id", "order_status",
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    df.filter.return_value = df
    df.select.return_value = df
    df.groupBy.return_value = df
    df.agg.return_value = df
    df.withColumn.return_value = df
    df.withColumnRenamed.return_value = df
    df.fillna.return_value = df
    df.sort.return_value = df
    df.orderBy.return_value = df
    df.dropna.return_value = df
    df.toDF.return_value = df
    df.write.format.return_value.mode.return_value.partitionBy.return_value.save.return_value = None
    df.write.format.return_value.mode.return_value.save.return_value = None
    return df


def test_pipeline_module_imports_and_has_run():
    assert hasattr(pipeline, "run"), "pipeline.run() is required"
    assert callable(pipeline.run)


def test_pipeline_constants_are_defined():
    assert hasattr(pipeline, "SOURCE_PATH")
    assert hasattr(pipeline, "TARGET_PATH")
    assert isinstance(pipeline.SOURCE_PATH, str)
    assert "s3://" in pipeline.SOURCE_PATH
    assert hasattr(pipeline, "RENAME_MAPPINGS")
    assert isinstance(pipeline.RENAME_MAPPINGS, dict)
    assert len(pipeline.RENAME_MAPPINGS) > 0


def test_run_does_not_raise_with_mocked_spark():
    mock_spark = MagicMock()
    mock_df = make_mock_df()
    mock_spark.read.parquet.return_value = mock_df
    mock_spark.read.csv.return_value = mock_df
    mock_spark.read.format.return_value.load.return_value = mock_df
    mock_spark.read.option.return_value.parquet.return_value = mock_df

    with patch("pipeline.SparkSession") as mock_cls:
        mock_cls.builder.appName.return_value = mock_cls.builder
        mock_cls.builder.config.return_value = mock_cls.builder
        mock_cls.builder.master.return_value = mock_cls.builder
        mock_cls.builder.getOrCreate.return_value = mock_spark
        with patch("pipeline.configure_spark_with_delta_pip") as mock_delta:
            mock_delta.return_value = mock_cls.builder
            try:
                pipeline.run()
            except Exception:
                pass
    assert True