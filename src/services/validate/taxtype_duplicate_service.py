import os
from typing import List
from pyspark.sql.functions import col # type: ignore
from config.database import PARQUET_ATTENTIONS_PATHS
from config.db_connection import read_table_from_db
from services.validate.update_service import InvoiceUpdate
from pyspark.sql import SparkSession, DataFrame
from pymysql.connections import Connection
from utils.constants import Constants
from pyspark.sql.types import StructType, StructField, StringType
from utils.queries_handler import (
    db_table_medden_impuestos,
    db_table_validacion_taxtypes_with_ids
)
from utils.message_handler import MessageHandler

class TaxTypeDuplicateHandler:


    def __init__(self, spark: SparkSession, connection: Connection, coreSystem: str):
        self.spark = spark
        self.connection = connection
        self.invoice_updater = InvoiceUpdate(connection)
        self.coreSystem = coreSystem
        self.message = "Tipo de impuesto duplicado"
    

    def procesar_tipo_impuesto(self, systems_validate: List[str], invoiceIds: List[int]):
        df_facturas_buscar = read_table_from_db(self.spark, db_table_validacion_taxtypes_with_ids(invoiceIds), self.coreSystem)
        df_facturas_por_validar = self.createDataFrameInvoice(df_facturas_buscar)
        self.invoice_updater.update_spark_processing(invoiceIds)

        for system in systems_validate:

            if df_facturas_por_validar.count() == 0:
                print("No hay facturas pendientes por procesar.")
                break
            
            df_liquidaciones = (
                self.load_dataframes(system['name']) if system.get("load_dataframes") else 
                read_table_from_db(self.spark, db_table_medden_impuestos, system['name'])
            )                            

            if df_liquidaciones is None:
                print(f"No se pudieron cargar datos para el sistema: {system['name']}")
                continue 
            
            print(f"validando desde el sistema de {system['name']}, cantidad {df_facturas_por_validar.count()}")

            df_facturas_filtradas = df_liquidaciones.join(
                df_facturas_por_validar.select("codigo_afiliado", "nro_solben", "ruc_proveedor", "tipo_impuesto").distinct(),
                on=["codigo_afiliado", "nro_solben", "ruc_proveedor", "tipo_impuesto"], 
                how="inner"
            )

            df_facturas_filtradas = df_facturas_filtradas.groupBy("codigo_afiliado", "nro_solben", "ruc_proveedor", "tipo_impuesto").count()

            self.buscar_duplicados(df_facturas_filtradas, df_facturas_buscar, system['name'])
        

    def buscar_duplicados(self, df_facturas_filtradas: DataFrame, df_facturas_buscar: DataFrame, system: str):
        duplicados_list = df_facturas_filtradas.collect() 
        for row in duplicados_list:
            codigo_afiliado = row['codigo_afiliado']
            nro_solben = row['nro_solben']
            ruc_proveedor = row['ruc_proveedor']
            tipo_impuesto = row['tipo_impuesto']
            cantidad = row['count']
            
            facturas_encontradas = df_facturas_buscar.filter(
                (col("codigo_afiliado") == codigo_afiliado) & 
                (col("nro_solben") == nro_solben) &
                (col("ruc_proveedor") == ruc_proveedor) &
                (col("tipo_impuesto") == tipo_impuesto))

            facturas_lists = facturas_encontradas.collect()

            for value in facturas_lists:
                factura_id = value["factura_id"]
                estado_id = value['id_estado']

                if estado_id in Constants.ESTADOS_VALIDOS:
                    if cantidad > 1:
                        observation: str = MessageHandler.message_attention_duplicate(self.message, value, system)
                        self.invoice_updater.update_invoices_detected(observation, factura_id, system)
                        print(f"Factura {factura_id} actualizada con la observación: {self.message} con estado {estado_id}")
                else:
                    print(f"El estado {estado_id} no esta contemplado")        


    def load_dataframes(self, system: str):
        path = PARQUET_ATTENTIONS_PATHS.get(system)
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            return None
        return self.spark.read.parquet(path) if path else None


    def createDataFrameInvoice(self, df_facturas_buscar: DataFrame) -> DataFrame:
        query_results = df_facturas_buscar.collect()
        data = [(row['codigo_afiliado'], row['nro_solben'], row['ruc_proveedor'], row['tipo_impuesto']) for row in query_results]
        schema = StructType([
            StructField("codigo_afiliado", StringType(), True),
            StructField("nro_solben", StringType(), True),
            StructField("ruc_proveedor", StringType(), True),
            StructField("tipo_impuesto", StringType(), True)
        ])
        return self.spark.createDataFrame(data, schema)