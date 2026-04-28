from typing import List, Dict
from pyspark.sql.functions import col, collect_list, count
from services.validator.update_service import InvoiceUpdate
from config.db_connection import read_table_from_db
from config.database import PARQUET_INVOICES_PATHS
from pyspark.sql import SparkSession, DataFrame
from pymysql.connections import Connection
from utils.constants import Constants
from pyspark.sql.types import StructType, StructField, StringType, DateType
from utils.queries_handler import (
    db_table_medden_facturas,
    db_table_validacion_facturas_with_ids
)
from utils.message_handler import MessageHandler

class InvoiceDuplicateHandler:


    def __init__(self, spark: SparkSession, connection: Connection, coreSystem: str):
        self.spark = spark
        self.connection = connection
        self.invoice_updater = InvoiceUpdate(connection)
        self.coreSystem = coreSystem
        self.message = "Duplicidad Caso 1"
    

    def procesar_facturas(self, dataFrame: DataFrame, system: Dict, invoiceIds: List[int]):
        df_facturas_buscar = read_table_from_db(self.spark, db_table_validacion_facturas_with_ids(invoiceIds), self.coreSystem)
        df_facturas_por_validar = self.createDataFrameInvoice(df_facturas_buscar)
        self.invoice_updater.update_spark_processing(invoiceIds)

        if df_facturas_por_validar.count() == 0:
            print("No hay facturas pendientes por procesar.")
            return

        df_liquidaciones = (
            dataFrame if system.get("load_dataframes") else 
            read_table_from_db(self.spark, db_table_medden_facturas, system['name'])
        )                     
        
        if df_liquidaciones is None:
            print(f"No se pudieron cargar datos para el sistema: {system['name']}")
            return

        print(f"validando desde el sistema de {system['name']}, cantidad {df_facturas_por_validar.count()}")

        df_facturas_filtradas = df_liquidaciones.join(
            df_facturas_por_validar.select("codigo_iafa", "ruc_proveedor", "nro_factu", "codigo_afiliado", "fch_atencion", "nro_solben", "monto").distinct(),
            on=["codigo_iafa", "ruc_proveedor", "nro_factu", "codigo_afiliado", "fch_atencion", "nro_solben", "monto"], 
            how="inner"
        )

        df_facturas_filtradas = df_facturas_filtradas.groupBy("codigo_iafa", "ruc_proveedor", "nro_factu", "codigo_afiliado", "fch_atencion", "nro_solben", "monto").agg(
            count("factura_id").alias("count"),
            collect_list("factura_id").alias("factura_ids"),
        )

        self.buscar_duplicados(df_facturas_filtradas, df_facturas_buscar, system['name'])


    def buscar_duplicados(self, df_facturas_filtradas: DataFrame, df_facturas_buscar: DataFrame, system: str):
        duplicados_list = df_facturas_filtradas.collect() 
        for row in duplicados_list:
            codigo_iafa = row['codigo_iafa']
            ruc_proveedor = row['ruc_proveedor']
            nro_factu = row['nro_factu']
            codigo_afiliado = row['codigo_afiliado']
            fch_atencion = row['fch_atencion']
            nro_solben = row['nro_solben']
            monto = row['monto']
            cantidad = row['count']
            factura_ids = row['factura_ids']
            
            facturas_encontradas = df_facturas_buscar.filter(
                (col("codigo_iafa") == codigo_iafa) &
                (col("ruc_proveedor") == ruc_proveedor) & 
                (col("nro_factu") == nro_factu) &
                (col("codigo_afiliado") == codigo_afiliado) & 
                (col("fch_atencion") == fch_atencion) &
                (col("nro_solben") == nro_solben) &
                (col("monto") == monto))

            facturas_lists = facturas_encontradas.collect()

            factura_ids_unicos = list(set(factura_ids))

            for value in facturas_lists:
                factura_id = value["factura_id"]
                estado_id = value["id_estado"]

                if estado_id in Constants.ESTADOS_VALIDOS:
                    if cantidad > 1:
                        factura_ids_filtrados = [
                            item for item in factura_ids_unicos
                            if not (not isinstance(item, list) and int(item) == int(factura_id))
                        ]                        
                        observation: str = MessageHandler.message_case_1(self.message, value, system)
                        self.invoice_updater.update_invoices_detected(observation, factura_id, system, factura_ids_filtrados)
                        print(f"Factura {factura_id} actualizada con la observación: {self.message} con estado {estado_id}")
                else:
                    print(f"El estado {estado_id} no esta contemplado")


    def createDataFrameInvoice(self, df_facturas_buscar: DataFrame) -> DataFrame:
        query_results = df_facturas_buscar.collect()
        data = [(row['codigo_iafa'], row['ruc_proveedor'], row['nro_factu'], row['codigo_afiliado'], row['fch_atencion'], row['nro_solben'], row['monto'], row['factura_id']) for row in query_results]
        schema = StructType([
            StructField("codigo_iafa", StringType(), True),
            StructField("ruc_proveedor", StringType(), True),
            StructField("nro_factu", StringType(), True),
            StructField("codigo_afiliado", StringType(), True),
            StructField("fch_atencion", DateType(), True),
            StructField("nro_solben", StringType(), True),
            StructField("monto", StringType(), True),
            StructField("factura_id", StringType(), True)
        ])
        return self.spark.createDataFrame(data, schema)