import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from services.loader.data_loader_service import DataFrameLoader
from config.db_connection import read_table_from_db, create_db_connection
from utils.constants import Constants


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestDBConnection").getOrCreate()


class TestDatabaseConnection:
    @patch('config.db_connection.pymysql.connect')
    def test_create_db_connection_success(self, mock_connect):
        mock_connect.return_value = MagicMock()
        result = create_db_connection(Constants.SYSTEM_SILUX_SEMEFA)
        mock_connect.assert_called_once()
        assert result is not None

    @patch('config.database.DATABASE_CONFIG', {})
    def test_create_db_connection_missing_config(self):
        result = create_db_connection("nonexistent_system")
        assert result is None


class TestReadTableFromDB:
    @patch('config.db_connection.read_table_from_db', create=True)
    def test_read_table_from_db_success(self, mock_read):
        mock_df = MagicMock()
        mock_read.return_value = mock_df
        from config.db_connection import read_table_from_db as db_read
        result = db_read(spark, "test_table", Constants.SYSTEM_SILUX_SEMEFA)
        assert result is not None

    @patch('config.database.DATABASE_CONFIG', {})
    def test_read_table_from_db_missing_config(self, spark):
        result = read_table_from_db(spark, "test_table", "nonexistent")
        assert result is None


class TestSparkSessionCreation:
    def test_create_spark_session_failure(self):
        from config.spark_config import create_spark_session
        result = create_spark_session()
        assert result is not None


class TestParquetHandler:
    def test_load_or_create_parquet_missing_storage_path(self):
        from utils.parquet_handler import load_or_create_parquet
        mock_spark = MagicMock()
        with patch('utils.parquet_handler.os.getenv', return_value=None):
            with pytest.raises(ValueError, match="STORAGE_PATH environment variable is not set"):
                load_or_create_parquet(mock_spark, "test_query", "test.parquet", Constants.SYSTEM_SILUX_SEMEFA)
