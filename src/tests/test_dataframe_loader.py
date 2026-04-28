import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from services.loader.data_loader_service import DataFrameLoader
from utils.constants import Constants


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("TestDataFrameLoader").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def loader():
    return DataFrameLoader(Constants.SYSTEM_SILUX_PROTECTA)


class TestDataFrameLoader:
    def test_load_data_invalid_system(self, loader):
        result = loader.load_data([2019, 2020], "invalid_system")
        assert result["success"] is False
        assert "No existe el sistema" in result["msg"]

    def test_load_data_valid_system_silux(self, loader):
        with patch.object(loader, 'querys') as mock_querys, \
             patch.object(loader, 'load_resources') as mock_load_resources:
            
            mock_querys.return_value = "SELECT * FROM test"
            result = loader.load_data([2019], Constants.SYSTEM_SILUX_PROTECTA)
            
            assert result["success"] is True
            assert result["msg"] == "Datos cargados exitosamente"
            mock_querys.assert_called_once()
            mock_load_resources.assert_called_once()

    def test_load_data_valid_system_solben(self, loader):
        loader.system = Constants.SYSTEM_SOLBEN_PROTECTA
        with patch.object(loader, 'querys') as mock_querys, \
             patch.object(loader, 'load_resources') as mock_load_resources:
            
            mock_querys.return_value = "SELECT * FROM test"
            result = loader.load_data([2019], Constants.SYSTEM_SOLBEN_PROTECTA)
            
            assert result["success"] is True
            mock_querys.assert_called_once()

    def test_load_data_querys_return_empty(self, loader):
        with patch.object(loader, 'querys') as mock_querys:
            mock_querys.return_value = ""
            result = loader.load_data([2019], Constants.SYSTEM_SILUX_PROTECTA)
            
            assert result["success"] is False
            assert "No se pudo realizar la carga" in result["msg"]

    def test_load_data_empty_years(self, loader):
        with patch.object(loader, 'querys') as mock_querys, \
             patch.object(loader, 'load_resources') as mock_load_resources:
            
            mock_querys.return_value = "SELECT * FROM test"
            result = loader.load_data([], Constants.SYSTEM_SILUX_PROTECTA)
            
            assert result["success"] is True

    def test_querys_silux_returns_correct_structure(self, loader):
        result = loader.querys("2019, 2020", Constants.SYSTEM_SILUX_PROTECTA)
        
        assert isinstance(result, str)
        assert "factura" in result.lower()
        assert "liqtempo" in result.lower()

    def test_querys_solben_returns_correct_structure(self, loader):
        loader.system = Constants.SYSTEM_SOLBEN_PROTECTA
        result = loader.querys("2019, 2020", Constants.SYSTEM_SOLBEN_PROTECTA)
        
        assert isinstance(result, str)
        assert "liquidacion" in result.lower()

    def test_destroy_dataframe_invalid_system(self, loader):
        result = loader.destroy_dataframe("invalid_system")
        assert result["success"] is False
        assert "No existe el sistema" in result["msg"]

    @patch('services.loader.data_loader_service.shutil.rmtree')
    def test_destroy_dataframe_success(self, mock_rmtree, loader):
        with patch('services.loader.data_loader_service.os.path.exists', return_value=True):
            result = loader.destroy_dataframe(Constants.SYSTEM_SILUX_PROTECTA)
            assert result["success"] is True
            assert "eliminados" in result["msg"].lower()

    def test_destroy_dataframe_directory_not_exists(self, loader):
        with patch('services.loader.data_loader_service.os.path.exists', return_value=False):
            result = loader.destroy_dataframe(Constants.SYSTEM_SILUX_PROTECTA)
            assert result["success"] is True
            assert "ninguno" in result["msg"]


class TestDataFrameSchemaValidation:
    def test_dataframe_schema_valid(self):
        from schemas.schema import DataFrameSchema
        schema = DataFrameSchema(years=[2019, 2020], origen=Constants.SYSTEM_SILUX_PROTECTA)
        assert schema.years == [2019, 2020]
        assert schema.origen == Constants.SYSTEM_SILUX_PROTECTA

    def test_dataframe_schema_empty_years(self):
        from schemas.schema import DataFrameSchema
        schema = DataFrameSchema(years=[], origen=Constants.SYSTEM_SOLBEN_PROTECTA)
        assert schema.years == []

    def test_dataframe_schema_none_years(self):
        from schemas.schema import DataFrameSchema
        from pydantic import ValidationError
        try:
            schema = DataFrameSchema(years=None, origen=Constants.SYSTEM_SILUX_PROTECTA)
            assert False, "Should have raised ValidationError"
        except ValidationError:
            assert True


class TestLoadResources:
    def test_load_resources_creates_directory(self, loader):
        with patch('services.loader.data_loader_service.os.path.exists') as mock_exists, \
             patch('services.loader.data_loader_service.os.makedirs') as mock_makedirs, \
             patch('services.loader.data_loader_service.get_spark_session') as mock_spark, \
             patch('services.loader.data_loader_service.load_or_create_parquet') as mock_load:
             
            mock_exists.return_value = False
            mock_spark.return_value = MagicMock()
            loader.load_resources("2019", "test_query", Constants.SYSTEM_SILUX_PROTECTA)
            
            mock_makedirs.assert_called_once()
            mock_load.assert_called_once()

    def test_load_resources_directory_exists(self, loader):
        with patch('services.loader.data_loader_service.os.getenv', return_value="/tmp/test"), \
             patch('services.loader.data_loader_service.os.makedirs') as mock_makedirs, \
             patch('services.loader.data_loader_service.get_spark_session') as mock_spark, \
             patch('services.loader.data_loader_service.load_or_create_parquet') as mock_load:
             
            mock_spark.return_value = MagicMock()
            loader.load_resources("2019", "test_query", Constants.SYSTEM_SILUX_PROTECTA)
            
            mock_makedirs.assert_called_once()
            mock_load.assert_called_once()


class TestExceptionHandling:
    @patch('services.loader.data_loader_service.get_spark_session')
    @patch('services.loader.data_loader_service.load_or_create_parquet')
    def test_load_data_with_jdbc_failure(self, mock_load_parquet, mock_spark):
        loader = DataFrameLoader(Constants.SYSTEM_SOLBEN_PROTECTA)
        mock_spark.return_value = MagicMock()
        mock_load_parquet.side_effect = Exception("JDBC Connection failed")
        
        result = loader.load_data([2019], Constants.SYSTEM_SOLBEN_PROTECTA)
        
        assert result["success"] is False
        assert "Error al cargar datos" in result["msg"]

    def test_querys_with_empty_years_string(self, loader):
        result = loader.querys("", Constants.SYSTEM_SILUX_PROTECTA)
        assert isinstance(result, str)
