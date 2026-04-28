import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import collect_list, count

from services.validator.attention_duplicate_service import AttentionDuplicateHandler
from utils.constants import Constants


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("TestDuplicateService").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def handler(spark):
    mock_connection = MagicMock()
    handler = AttentionDuplicateHandler(spark, mock_connection, Constants.SYSTEM_SILUX_PROTECTA)
    return handler


@pytest.fixture
def sample_invoice():
    from datetime import date
    return Row(
        codigo_afiliado="123",
        monto=100.0,
        nro_solben="001",
        ruc_proveedor="ABC",
        codigo_iafa="IAFA001",
        fch_atencion=date(2024, 1, 1),
        factura_id=1,
        id_estado=9
    )


class TestAttentionDuplicateHandler:
    def test_buscar_duplicados_detects_duplicates(self, spark, handler, sample_invoice):
        df_facturas_buscar = spark.createDataFrame([sample_invoice])

        df_base = spark.createDataFrame([
            sample_invoice,
            Row(
                codigo_afiliado="123", monto=100.0, nro_solben="001",
                ruc_proveedor="ABC", codigo_iafa="IAFA001",
                fch_atencion=sample_invoice.fch_atencion, factura_id=2, id_estado=9
            )
        ])

        df_facturas_filtradas = df_base.groupBy(
            "codigo_afiliado", "monto", "nro_solben", "ruc_proveedor", "codigo_iafa", "fch_atencion"
        ).agg(
            count("factura_id").alias("count"),
            collect_list("factura_id").alias("factura_ids"),
        )

        handler.invoice_updater.update_invoices_detected = MagicMock()

        handler.buscar_duplicados(df_facturas_filtradas, df_facturas_buscar, Constants.SYSTEM_SILUX_PROTECTA)

        handler.invoice_updater.update_invoices_detected.assert_called_once()
        call_args = handler.invoice_updater.update_invoices_detected.call_args
        assert "duplicidad" in call_args[0][0].lower()
        assert call_args[0][1] == 1

    def test_buscar_duplicados_no_duplicates(self, spark, handler, sample_invoice):
        df_facturas_buscar = spark.createDataFrame([sample_invoice])

        df_base = spark.createDataFrame([Row(
            codigo_afiliado="123", monto=100.0, nro_solben="001",
            ruc_proveedor="ABC", codigo_iafa="IAFA001",
            fch_atencion=sample_invoice.fch_atencion, factura_id=1, id_estado=9
        )])

        df_facturas_filtradas = df_base.groupBy(
            "codigo_afiliado", "monto", "nro_solben", "ruc_proveedor", "codigo_iafa", "fch_atencion"
        ).agg(
            count("factura_id").alias("count"),
            collect_list("factura_id").alias("factura_ids"),
        )

        handler.invoice_updater.update_invoices_detected = MagicMock()

        handler.buscar_duplicados(df_facturas_filtradas, df_facturas_buscar, Constants.SYSTEM_SILUX_PROTECTA)

        handler.invoice_updater.update_invoices_detected.assert_not_called()

    def test_buscar_duplicados_multiple_duplicates(self, spark, handler, sample_invoice):
        from datetime import date
        df_facturas_buscar = spark.createDataFrame([sample_invoice])

        df_base = spark.createDataFrame([
            sample_invoice,
            Row(codigo_afiliado="123", monto=100.0, nro_solben="001", ruc_proveedor="ABC", 
                codigo_iafa="IAFA001", fch_atencion=date(2024, 1, 1), factura_id=2, id_estado=9),
            Row(codigo_afiliado="123", monto=100.0, nro_solben="001", ruc_proveedor="ABC",
                codigo_iafa="IAFA001", fch_atencion=date(2024, 1, 1), factura_id=3, id_estado=9),
        ])

        df_facturas_filtradas = df_base.groupBy(
            "codigo_afiliado", "monto", "nro_solben", "ruc_proveedor", "codigo_iafa", "fch_atencion"
        ).agg(
            count("factura_id").alias("count"),
            collect_list("factura_id").alias("factura_ids"),
        )

        handler.invoice_updater.update_invoices_detected = MagicMock()

        handler.buscar_duplicados(df_facturas_filtradas, df_facturas_buscar, Constants.SYSTEM_SILUX_PROTECTA)

        assert handler.invoice_updater.update_invoices_detected.call_count == 1

    def test_handler_initialization(self, spark):
        mock_connection = MagicMock()
        handler = AttentionDuplicateHandler(spark, mock_connection, Constants.SYSTEM_SOLBEN_PROTECTA)
        assert handler.coreSystem == Constants.SYSTEM_SOLBEN_PROTECTA
        assert handler.invoice_updater is not None
