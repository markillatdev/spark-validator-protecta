import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from services.loader.data_loader_service import DataFrameLoader
from utils.constants import Constants


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestDataFrameLoader").getOrCreate()


@pytest.fixture
def loader():
    return DataFrameLoader(Constants.SYSTEM_SILUX_SEMEFA)


class TestDataFrameLoader:
    def test_load_data_invalid_system(self, loader):
        loader.system = "invalid_system"
        result = loader.load_data([2019, 2020], "invalid_system")
        assert result["success"] is False
        assert "No existe el sistema invalid_system" in result["msg"]

    def test_load_data_valid_system_silux(self, loader):
        with patch.object(loader, 'load_attention') as mock_attention, \
             patch.object(loader, 'load_invoices') as mock_invoices, \
             patch.object(loader, 'load_tax_type') as mock_tax_type, \
             patch.object(loader, 'load_amount') as mock_amount, \
             patch.object(loader, 'querys') as mock_querys:
            
            mock_querys.return_value = ("query1", "query2", "query3", "query4")
            result = loader.load_data([2019], Constants.SYSTEM_SILUX_SEMEFA)
            
            assert result["success"] is True
            assert result["msg"] == "Datos cargados exitosamente"
            mock_attention.assert_called_once()
            mock_invoices.assert_called_once()
            mock_tax_type.assert_called_once()
            mock_amount.assert_called_once()

    def test_load_data_valid_system_solben(self, loader):
        loader.system = Constants.SYSTEM_SOLBEN_SEMEFA
        with patch.object(loader, 'load_attention') as mock_attention, \
             patch.object(loader, 'load_invoices') as mock_invoices, \
             patch.object(loader, 'load_tax_type') as mock_tax_type, \
             patch.object(loader, 'load_amount') as mock_amount, \
             patch.object(loader, 'querys') as mock_querys:
            
            mock_querys.return_value = ("query1", "query2", "query3", "query4")
            result = loader.load_data([2019], Constants.SYSTEM_SOLBEN_SEMEFA)
            
            assert result["success"] is True
            mock_attention.assert_called_once()

    def test_load_data_querys_return_empty(self, loader):
        with patch.object(loader, 'querys') as mock_querys:
            mock_querys.return_value = (None, None, None, None)
            result = loader.load_data([2019], Constants.SYSTEM_SILUX_SEMEFA)
            
            assert result["success"] is False
            assert "No se pudo realizar la carga" in result["msg"]

    def test_querys_silux_returns_correct_structure(self, loader):
        result = loader.querys("2019, 2020", Constants.SYSTEM_SILUX_SEMEFA)
        
        assert len(result) == 4
        ordenes, facturas, tipo_impuesto, importe = result
        assert "factura f" in ordenes.lower()
        assert "factura_proveedor fp" in facturas.lower()

    def test_querys_solben_returns_correct_structure(self, loader):
        loader.system = Constants.SYSTEM_SOLBEN_SEMEFA
        result = loader.querys("2019, 2020", Constants.SYSTEM_SOLBEN_SEMEFA)
        
        assert len(result) == 4
        ordenes, facturas, tipo_impuesto, importe = result
        assert "liquidacion" in ordenes.lower()

    def test_destroy_dataframe_invalid_system(self, loader):
        result = loader.destroy_dataframe("invalid_system")
        assert result["success"] is False
        assert "No existe el sistema invalid_system" in result["msg"]

    @patch('services.loader.data_loader_service.os.path.exists')
    @patch('services.loader.data_loader_service.shutil.rmtree')
    def test_destroy_dataframe_success(self, mock_rmtree, mock_exists, loader):
        mock_exists.return_value = True
        result = loader.destroy_dataframe(Constants.SYSTEM_SILUX_SEMEFA)
        assert result["success"] is True
        assert "eliminados exitosamente" in result["msg"]

    @patch('services.loader.data_loader_service.os.path.exists')
    def test_destroy_dataframe_directory_not_exists(self, mock_exists, loader):
        mock_exists.return_value = False
        result = loader.destroy_dataframe(Constants.SYSTEM_SILUX_SEMEFA)
        assert result["success"] is False
        assert "No existe el directorio" in result["msg"]


class TestDataFrameSchemaValidation:
    def test_dataframe_schema_valid(self):
        from schemas.schema import DataFrameSchema
        schema = DataFrameSchema(years=[2019, 2020], origen=Constants.SYSTEM_SILUX_SEMEFA)
        assert schema.years == [2019, 2020]
        assert schema.origen == Constants.SYSTEM_SILUX_SEMEFA

    def test_dataframe_schema_empty_years(self):
        from schemas.schema import DataFrameSchema
        schema = DataFrameSchema(years=[], origen=Constants.SYSTEM_SOLBEN_SEMEFA)
        assert schema.years == []

    def test_dataframe_schema_invalid_origen(self):
        from schemas.schema import DataFrameSchema
        schema = DataFrameSchema(years=[2019], origen="invalid")
        assert schema.origen == "invalid"


class TestLoadResources:
    def test_load_resources_creates_directory(self, loader):
        with patch('services.loader.data_loader_service.os.path.exists') as mock_exists, \
             patch('services.loader.data_loader_service.os.makedirs') as mock_makedirs, \
             patch('services.loader.data_loader_service.create_spark_session') as mock_spark, \
             patch('services.loader.data_loader_service.load_or_create_parquet') as mock_load:
            
            mock_exists.return_value = False
            mock_spark.return_value = MagicMock()
            loader.load_resources(2019, "test_query", "attentions", Constants.SYSTEM_SILUX_SEMEFA)
            
            mock_makedirs.assert_called_once()
            mock_load.assert_called_once()


class TestDebug500Error:
    @patch('services.loader.data_loader_service.create_spark_session')
    @patch('services.loader.data_loader_service.load_or_create_parquet')
    def test_load_data_with_real_query_failure(self, mock_load_parquet, mock_spark):
        loader = DataFrameLoader(Constants.SYSTEM_SOLBEN_SEMEFA)
        mock_spark.return_value = MagicMock()
        mock_load_parquet.side_effect = Exception("JDBC Connection failed: Connection refused")
        
        result = loader.load_data([2019], Constants.SYSTEM_SOLBEN_SEMEFA)
        
        assert result["success"] is False
        assert "Error al cargar datos" in result["msg"]
        assert "JDBC Connection failed" in result["msg"]

    def test_schema_validation_edge_cases(self):
        from schemas.schema import DataFrameSchema
        schema = DataFrameSchema(years=[], origen="")
        assert schema.years == []
        assert schema.origen == ""
