from pyspark.sql.functions import col # type: ignore
from config.database import PARQUET_ATTENTIONS_PATHS
from config.db_connection import create_db_connection, read_table_from_db
from config.spark_config import create_spark_session
from services.validate.update_service import InvoiceUpdate
from utils.queries_handler import (
    db_table_liquidacion_ordenes_silux,
    db_table_liquidacion_ordenes_silux_with_ids
)

class AttentionDuplicateGroup:


    def __init__(self, spark, connection, coreSystem):
        self.spark = spark
        self.connection = connection
        self.invoice_updater = InvoiceUpdate(connection)
        self.coreSystem = coreSystem
        self.message = "Atencion duplicada"


    def procesar_atenciones(self):
        df_solben_sabsa = read_table_from_db(self.spark, db_table_liquidacion_ordenes_silux, self.coreSystem)
        df_facturas_silux = read_table_from_db(self.spark, db_table_liquidacion_ordenes_silux_with_ids, "sabsa_dev")
        self.invoice_updater.update_spark_processing()
        path = PARQUET_ATTENTIONS_PATHS.get("unix_sabsa")
        df_antiguas = self.spark.read.parquet(path)
        filtered_df = df_antiguas.join(
            df_solben_sabsa.select("codigo_afiliado", "monto", "nro_solben", "ruc_proveedor").distinct(),
            on=["codigo_afiliado", "monto", "nro_solben", "ruc_proveedor"],
            how="inner"
        )
        duplicados = filtered_df.groupBy("codigo_afiliado", "monto", "nro_solben", "ruc_proveedor") \
            .count() \
            .filter("count > 1")
        
        print(f"validando desde el sistema de unix_sabsa, cantidad {duplicados.count()}")

        resources = duplicados.toLocalIterator()
        
        for resource in resources:
            codigo_afiliado = resource['codigo_afiliado']
            monto = resource['monto']
            nro_solben = resource['nro_solben']
            ruc_proveedor = resource['ruc_proveedor']
            facturas_encontradas = df_facturas_silux.filter(
               (col('codigo_afiliado') == codigo_afiliado) &
               (col('monto') == monto) &
               (col('nro_solben') == nro_solben) &
               (col('ruc_proveedor') == ruc_proveedor)
            )
            factura_lists = facturas_encontradas.collect()
            for factura in factura_lists:
                try: 
                    factura_id = factura["factura_id"]
                    self.invoice_updater.update_invoices_detected(self.message, factura_id, "unix_sabsa")
                    print(f"Factura {factura_id} actualizada con la observación: {self.message}")
                except Exception as e:
                    print(f"Error al actualizar la factura {factura_id}: {e}")


if __name__ == "__main__":
    spark = create_spark_session()
    connection = create_db_connection("sabsa_dev")
    attention = AttentionDuplicateGroup(spark, connection, "sabsa_solben")
    attention.procesar_atenciones()