import pytest
from unittest.mock import patch, MagicMock, PropertyMock
from pyspark.sql import SparkSession
from config.db_connection import read_table_from_db, create_db_connection
from config.spark_config import get_spark_session
from utils.constants import Constants


@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder.master("local[1]").appName("TestDBConnection").getOrCreate()
    yield spark_session
    spark_session.stop()


class TestDatabaseConnection:
    @patch('config.db_connection.pymysql')
    def test_create_db_connection_success(self, mock_pymysql):
        mock_conn = MagicMock()
        mock_pymysql.connect.return_value = mock_conn
        
        result = create_db_connection(Constants.SYSTEM_SILUX_PROTECTA)
        
        mock_pymysql.connect.assert_called_once()
        assert result is not None

    def test_create_db_connection_missing_config(self):
        with patch('config.db_connection.DATABASE_CONFIG', {}):
            result = create_db_connection("nonexistent_system")
            assert result is None


class TestReadTableFromDB:
    @patch('config.db_connection.read_table_from_db')
    def test_read_table_from_db_success(self, mock_read, spark):
        mock_df = MagicMock()
        mock_read.return_value = mock_df
        
        result = mock_read(spark, "test_table", Constants.SYSTEM_SILUX_PROTECTA)
        
        assert result is not None
        mock_read.assert_called_once_with(spark, "test_table", Constants.SYSTEM_SILUX_PROTECTA)

    def test_read_table_from_db_missing_config(self, spark):
        with patch('config.db_connection.DATABASE_CONFIG', {}):
            result = read_table_from_db(spark, "test_table", "nonexistent")
            assert result is None


class TestSparkSession:
    def test_spark_session_singleton(self):
        session1 = get_spark_session()
        session2 = get_spark_session()
        assert session1 is session2

    def test_spark_session_creation(self):
        with patch('config.spark_config.SparkSession') as mock_spark_class:
            mock_session = MagicMock()
            mock_spark_class.builder.appName.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_session
            
            from config.spark_config import _spark_session
            import config.spark_config as sc
            sc._spark_session = None
            
            result = get_spark_session()
            
            assert result is not None


class TestParquetHandler:
    def test_load_or_create_parquet_missing_storage_path(self):
        from utils.parquet_handler import load_or_create_parquet
        mock_spark = MagicMock()
        with patch('utils.parquet_handler.os.getenv', return_value=None):
            with pytest.raises(ValueError, match="STORAGE_PATH environment variable is not set"):
                load_or_create_parquet(mock_spark, "test_query", "test.parquet", Constants.SYSTEM_SILUX_PROTECTA)

    @patch('utils.parquet_handler.os.path.exists', return_value=True)
    @patch('utils.parquet_handler.read_table_from_db')
    def test_load_or_create_parquet_existing(self, mock_read, mock_exists):
        from utils.parquet_handler import load_or_create_parquet
        mock_df = MagicMock()
        mock_read.return_value = mock_df
        mock_spark = MagicMock()
        mock_spark.read.parquet.return_value = mock_df
    
        load_or_create_parquet(mock_spark, "test_query", "test.parquet", Constants.SYSTEM_SILUX_PROTECTA)
    
        mock_spark.read.parquet.assert_called_once()
        mock_read.assert_not_called()
        mock_read.assert_not_called()
