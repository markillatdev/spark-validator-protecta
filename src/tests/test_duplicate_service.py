import pytest # type: ignore
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, collect_list, count

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
    return AttentionDuplicateHandler(spark, mock_connection, Constants.SYSTEM_SILUX_PROTECTA)

def test_buscar_duplicados(spark: SparkSession, handler: AttentionDuplicateHandler):
    from datetime import date
    df_facturas_buscar = spark.createDataFrame([
        Row(codigo_afiliado="123", monto=100.0, nro_solben="001", ruc_proveedor="ABC", codigo_iafa="IAFA001", fch_atencion=date(2024, 1, 1), factura_id=1, id_estado=9)
    ])

    df_base = spark.createDataFrame([
        Row(codigo_afiliado="123", monto=100.0, nro_solben="001", ruc_proveedor="ABC", codigo_iafa="IAFA001", fch_atencion=date(2024, 1, 1), factura_id=1),
        Row(codigo_afiliado="123", monto=100.0, nro_solben="001", ruc_proveedor="ABC", codigo_iafa="IAFA001", fch_atencion=date(2024, 1, 1), factura_id=2)
    ])

    df_facturas_filtradas = df_base.groupBy("codigo_afiliado", "monto", "nro_solben", "ruc_proveedor", "codigo_iafa", "fch_atencion").agg(
        count("factura_id").alias("count"),
        collect_list("factura_id").alias("factura_ids"),
    )

    handler.invoice_updater.update_invoices_detected = MagicMock()

    handler.buscar_duplicados(df_facturas_filtradas, df_facturas_buscar, Constants.SYSTEM_SILUX_PROTECTA)

    handler.invoice_updater.update_invoices_detected.assert_called_once_with(
        "Atencion duplicada: La Atencion 001 se encuentra duplicado en: silux_protecta, monto: 100.0, codigo afiliado: 123, clinica: ABC", 1, Constants.SYSTEM_SILUX_PROTECTA, [2]
    )

def systems() -> list:
    return [
        {"name": Constants.SYSTEM_SILUX_PROTECTA},
        {"name": Constants.SYSTEM_SOLBEN_PROTECTA, "load_dataframes": True}
    ]