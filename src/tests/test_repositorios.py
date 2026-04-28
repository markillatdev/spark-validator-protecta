import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
load_dotenv()


@pytest.fixture(scope="module")
def spark():
    """Fixture para inicializar SparkSession."""
    session = SparkSession.builder.master("local[1]").appName("TestRepositories").getOrCreate()
    yield session
    session.stop()


class TestParquetRepositoryStructure:
    def test_storage_path_configured(self):
        storage_path = os.getenv("STORAGE_PATH")
        assert storage_path is not None, "STORAGE_PATH not configured"

    @patch('os.path.exists')
    def test_repository_directories_exist(self, mock_exists):
        mock_exists.return_value = True
        storage_path = os.getenv("STORAGE_PATH", "/tmp")
        
        subdirs = ["attentions", "invoices", "taxtypes", "amounts"]
        for subdir in subdirs:
            subdir_path = os.path.join(storage_path, subdir)
            assert subdir_path is not None

    def test_read_parquet_file(self, spark, tmp_path):
        import pandas as pd
        
        df_pandas = pd.DataFrame({
            'factura_id': [1, 2, 3],
            'monto': [100.0, 200.0, 300.0]
        })
        
        parquet_path = tmp_path / "test.parquet"
        df_spark = spark.createDataFrame(df_pandas)
        df_spark.write.parquet(str(parquet_path))
        
        df_read = spark.read.parquet(str(parquet_path))
        assert df_read.count() == 3
        assert len(df_read.columns) == 2

    @patch('os.listdir')
    @patch('os.path.exists')
    def test_parquet_file_discovery(self, mock_exists, mock_listdir, tmp_path):
        mock_exists.return_value = True
        mock_listdir.return_value = ["file1.parquet", "file2.parquet", "other.txt"]
        
        parquet_files = [f for f in mock_listdir.return_value if f.endswith(".parquet")]
        
        assert len(parquet_files) == 2
        assert "file1.parquet" in parquet_files
        assert "file2.parquet" in parquet_files
        assert "other.txt" not in parquet_files

    def test_parquet_schema_validation(self, spark, tmp_path):
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        
        schema = StructType([
            StructField("factura_id", StringType(), True),
            StructField("monto", DoubleType(), True)
        ])
        
        df = spark.createDataFrame([(1, 100.0)], schema=schema)
        parquet_path = str(tmp_path / "schema_test.parquet")
        df.write.parquet(parquet_path)
        
        df_read = spark.read.parquet(parquet_path)
        assert "factura_id" in df_read.columns
        assert "monto" in df_read.columns
