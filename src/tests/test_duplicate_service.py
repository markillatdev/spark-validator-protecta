import pytest # type: ignore
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import Row

from services.validate.attention_duplicate_service import AttentionDuplicateHandler
from utils.constants import Constants

# Simulación de un SparkSession
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("Test").getOrCreate()

@pytest.fixture
def handler(spark):
    # Mock de la conexión a la base de datos
    mock_connection = MagicMock()
    return AttentionDuplicateHandler(spark, mock_connection, Constants.SYSTEM_SILUX_SABSA)

def test_buscar_duplicados(spark: SparkSession, handler: AttentionDuplicateHandler):
    # Crear DataFrames simulados
    df_facturas_filtradas = spark.createDataFrame([
        Row(codigo_afiliado="123", monto=100.0, nro_solben="001", ruc_proveedor="ABC", count=2)
    ])
    df_facturas_buscar = spark.createDataFrame([
        Row(codigo_afiliado="123", monto=100.0, nro_solben="001", ruc_proveedor="ABC", factura_id=1)
    ])

    # Mock del método `update_invoices_detected`
    handler.invoice_updater.update_invoices_detected = MagicMock()

    # Ejecución del método
    handler.buscar_duplicados(df_facturas_filtradas, df_facturas_buscar, "system1")

    # Verificar que `update_invoices_detected` fue llamado correctamente
    handler.invoice_updater.update_invoices_detected.assert_called_once_with(
        "Atencion duplicada", 1, "system1"
    )

def systems() -> list:
    return [
        {"name": Constants.SYSTEM_SILUX_SABSA},
        {"name": Constants.SYSTEM_UNIX_SABSA, "load_dataframes": True}
    ]