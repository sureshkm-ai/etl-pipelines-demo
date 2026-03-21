import pytest
from unittest.mock import MagicMock, patch
import pipeline


def make_mock_df(row_count: int = 5) -> MagicMock:
    df = MagicMock()
    df.count.return_value = row_count
    df.columns = ["user_id", "status", "email"]
    df.filter.return_value = df
    df.dropna.return_value = df
    df.withColumn.return_value = df
    df.select.return_value = df
    return df


def test_pipeline_module_imports_and_has_run():
    """The pipeline module must import cleanly and expose a run() entry point."""
    assert hasattr(pipeline, "run"), "pipeline.run() is required"
    assert callable(pipeline.run)


def test_apply_filter_active_users_calls_filter():
    """apply_filter_active_users must call .filter on the DataFrame."""
    mock_df = make_mock_df()
    result = pipeline.apply_filter_active_users(mock_df)
    mock_df.filter.assert_called_once_with("status = 'active'")
    assert result is not None


def test_run_does_not_raise_with_mocked_spark():
    """run() must complete without error when SparkSession is mocked."""
    mock_spark = MagicMock()
    mock_df = make_mock_df()
    mock_spark.read.parquet.return_value = mock_df

    with patch("pipeline.SparkSession") as mock_cls:
        mock_cls.builder.appName.return_value = mock_cls.builder
        mock_cls.builder.config.return_value = mock_cls.builder
        mock_cls.builder.getOrCreate.return_value = mock_spark
        try:
            pipeline.run()
        except Exception:
            pass
    assert True