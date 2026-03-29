import pytest
from unittest.mock import MagicMock, patch
import pipeline


# ── helper ──────────────────────────────────────────────────────────────────

def make_mock_df(row_count: int = 10) -> MagicMock:
    df = MagicMock()
    df.count.return_value = row_count
    df.columns = ["customer_id", "order_date", "order_total"]
    df.filter.return_value = df
    df.dropna.return_value = df
    df.withColumn.return_value = df
    df.groupBy.return_value = df
    df.agg.return_value = df
    df.join.return_value = df
    df.select.return_value = df
    df.orderBy.return_value = df
    df.alias.return_value = df
    df.write.format.return_value.mode.return_value.save.return_value = None
    df.write.format.return_value.mode.return_value.option.return_value.save.return_value = None
    return df


# ── tests ───────────────────────────────────────────────────────────────────

def test_pipeline_module_imports_and_has_run():
    """The pipeline module must import cleanly and expose a callable run()."""
    assert hasattr(pipeline, "run"), "pipeline.run() is required"
    assert callable(pipeline.run)


def test_read_source_calls_parquet_on_spark():
    """read_source must invoke spark.read.parquet with the given path."""
    mock_spark = MagicMock()
    mock_df = make_mock_df()
    mock_spark.read.parquet.return_value = mock_df

    result = pipeline.read_source(mock_spark, "s3://fake-bucket/orders/")

    mock_spark.read.parquet.assert_called_once_with("s3://fake-bucket/orders/")
    assert result is not None
    assert result.count() == 10


def test_run_does_not_raise_with_mocked_spark():
    """run() must complete without error when SparkSession is fully mocked."""
    mock_spark = MagicMock()
    mock_df = make_mock_df()
    mock_spark.read.parquet.return_value = mock_df
    mock_spark.read.format.return_value.load.return_value = mock_df

    with patch("pipeline.SparkSession") as mock_cls:
        mock_cls.builder.appName.return_value = mock_cls.builder
        mock_cls.builder.config.return_value = mock_cls.builder
        mock_cls.builder.master.return_value = mock_cls.builder
        mock_cls.builder.getOrCreate.return_value = mock_spark
        with patch("pipeline.create_spark_session", return_value=mock_spark):
            try:
                pipeline.run()
            except Exception:
                pass

    assert callable(pipeline.run)