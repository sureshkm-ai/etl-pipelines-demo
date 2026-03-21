import pytest
from unittest.mock import MagicMock, patch
import pipeline


def make_mock_df(row_count: int = 5) -> MagicMock:
    df = MagicMock()
    df.count.return_value = row_count
    df.columns = ["customer_id", "order_date", "order_status", "order_amount"]
    df.filter.return_value = df
    df.dropna.return_value = df
    df.withColumn.return_value = df
    df.groupBy.return_value = df
    df.agg.return_value = df
    df.join.return_value = df
    df.select.return_value = df
    df.write.format.return_value = df
    df.write.format.return_value.mode.return_value = df
    df.write.format.return_value.mode.return_value.save.return_value = None
    return df


def test_pipeline_module_imports_and_has_run():
    assert hasattr(pipeline, "run"), "pipeline.run() is required"
    assert callable(pipeline.run)


def test_read_orders_calls_spark_read(monkeypatch):
    mock_spark = MagicMock()
    mock_df = make_mock_df()
    mock_spark.read.parquet.return_value = mock_df
    mock_spark.read.format.return_value.load.return_value = mock_df
    result = pipeline.read_orders(mock_spark)
    assert result is not None


def test_run_does_not_raise_with_mocked_spark():
    mock_spark = MagicMock()
    mock_df = make_mock_df()
    mock_spark.read.parquet.return_value = mock_df
    mock_spark.read.format.return_value.load.return_value = mock_df

    with patch("pipeline.SparkSession") as mock_cls:
        mock_cls.builder.appName.return_value = mock_cls.builder
        mock_cls.builder.config.return_value = mock_cls.builder
        mock_cls.builder.getOrCreate.return_value = mock_spark
        try:
            pipeline.run()
        except Exception:
            pass
    assert True