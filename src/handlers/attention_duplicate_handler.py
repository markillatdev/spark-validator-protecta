from pyspark.sql.functions import col # type: ignore
from config.database import PARQUET_ATTENTIONS_PATHS
from config.db_connection import read_table_from_db
from handlers.update_handler import InvoiceUpdate
from utils.queries_handler import (
    db_table_medden_ordenes,
    db_table_validacion_ordenes,
    db_table_validacion_ordenes_with_ids
)

class AttentionDuplicateHandler:


    def __init__(self, spark, connection, coreSystem):
        self.spark = spark
        self.connection = connection
        self.invoice_updater = InvoiceUpdate(connection)
        self.coreSystem = coreSystem
        self.message = "Atencion duplicada"
    

    def procesar_atenciones(self, systems_validate):
        df_facturas_por_validar = read_table_from_db(self.spark, db_table_validacion_ordenes, self.coreSystem)
        df_facturas_buscar = read_table_from_db(self.spark, db_table_validacion_ordenes_with_ids, self.coreSystem)
        self.invoice_updater.update_spark_processing()

        for system in systems_validate:

            if df_facturas_por_validar.count() == 0:
                print("No hay facturas pendientes por procesar.")
                break

            print(f"validando desde el sistema de {system['name']}, cantidad {df_facturas_por_validar.count()}")
            
            df_liquidaciones = (
                self.load_dataframes(system['name']) if system.get("load_dataframes") else 
                read_table_from_db(self.spark, db_table_medden_ordenes, system['name'])
            )                
            
            if df_liquidaciones is None:
                print(f"No se pudieron cargar datos para el sistema: {system['name']}")
                continue 

            df_facturas_filtradas = df_liquidaciones.join(
                df_facturas_por_validar.select("codigo_afiliado", "monto", "nro_solben", "ruc_proveedor").distinct(),
                on=["codigo_afiliado", "monto", "nro_solben", "ruc_proveedor"], 
                how="inner"
            )

            self.buscar_duplicados(df_facturas_filtradas, df_facturas_buscar, system['name'])
            df_facturas_por_validar = read_table_from_db(self.spark, db_table_validacion_ordenes, self.coreSystem)

            if df_facturas_por_validar.count() == 0:
                print("No hay facturas pendientes por procesar.")
                break
        

    def buscar_duplicados(self, df_facturas_filtradas, df_facturas_buscar, system):
        duplicados_list = df_facturas_filtradas.collect() 
        for row in duplicados_list:
            codigo_afiliado = row['codigo_afiliado']
            monto = row['monto']
            nro_solben = row['nro_solben']
            ruc_proveedor = row['ruc_proveedor']
            
            facturas_encontradas = df_facturas_buscar.filter(
                (col("codigo_afiliado") == codigo_afiliado) & 
                (col("monto") == monto) & 
                (col("nro_solben") == nro_solben) &
                (col("ruc_proveedor") == ruc_proveedor))

            facturas_lists = facturas_encontradas.collect()

            for value in facturas_lists:
                factura_id = value["factura_id"]
                self.invoice_updater.update_invoices_detected(self.message, factura_id, system)
                print(f"Factura {factura_id} actualizada con la observación: {self.message}")


    def load_dataframes(self, system):
        path = PARQUET_ATTENTIONS_PATHS.get(system)
        return self.spark.read.parquet(path) if path else None