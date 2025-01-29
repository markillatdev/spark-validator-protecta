from typing import List
from config.spark_config import create_spark_session
from pyspark.sql.functions import col # type: ignore
from pyspark.sql import DataFrame, SparkSession
from config.database import PARQUET_INVOICES_PATHS
from utils.constants import Constants
from pyspark.sql.types import StructType, StructField, StringType
from config.db_connection import read_table_from_db
from utils.queries_handler import (
    db_table_validacion_facturas,
    db_table_validacion_facturas_with_ids
)

class testDataframeService:
    def __init__(self, invoiceIds: List[int], coreSystem: str):
        self.invoiceIds = invoiceIds
        self.coreSystem = coreSystem

    def res(self):
        spark = create_spark_session()
        #df_facturas_por_validar = read_table_from_db(spark, db_table_validacion_facturas(self.invoiceIds), self.coreSystem)
        #df_facturas_buscar = read_table_from_db(spark, db_table_validacion_facturas_with_ids(self.invoiceIds), self.coreSystem)

        df_facturas_por_validar = self.simulation_facturas_por_validar(spark)
        df_facturas_buscar = self.simulation_facturas_por_validar_with_ids(spark)

        for system in self.systems():
            df_liquidaciones = self.load_dataframes(system['name'], spark)
            
            df_facturas_filtradas = df_liquidaciones.join(
                df_facturas_por_validar.select("ruc_proveedor", "nro_factu").distinct(),
                on=["ruc_proveedor", "nro_factu"], 
                how="inner"
            )

            df_facturas_filtradas = df_facturas_filtradas.groupBy("ruc_proveedor", "nro_factu").count()
            self.buscar_duplicados(df_facturas_filtradas, df_facturas_buscar)

        return {
            "msg": "success",
            "success": True
        }
    
    def systems(self):
        return [
            {"name": Constants.SYSTEM_UNIX_SABSA, "load_dataframes": True},
            {"name": Constants.SYSTEM_UNIX_COBERTURA, "load_dataframes": True}
        ]

    def buscar_duplicados(self, df_facturas_filtradas: DataFrame, df_facturas_buscar: DataFrame):
        print("buscar_duplicados...")
        duplicados_list = df_facturas_filtradas.collect() 
        for row in duplicados_list:
            ruc_proveedor = row['ruc_proveedor']
            nro_factu = row['nro_factu']
            cantidad = row['count']
            
            facturas_encontradas = df_facturas_buscar.filter(
                (col("ruc_proveedor") == ruc_proveedor) & 
                (col("nro_factu") == nro_factu))

            facturas_lists = facturas_encontradas.collect()
            print("facturas_encontradas...")

            for value in facturas_lists:
                factura_id = value["factura_id"]
                print(factura_id)
                if cantidad > 1:
                    print(f"Factura Mock {factura_id} actualizada con la observación: Duplicado")

    def load_dataframes(self, system: str, spark: SparkSession):
        path = PARQUET_INVOICES_PATHS.get(system)
        return spark.read.parquet(path) if path else None

    def simulation_facturas_por_validar(self, spark: SparkSession):
        # Definir el esquema de la tabla simulada
        schema = StructType([
            StructField("ruc_proveedor", StringType(), True),
            StructField("nro_factu", StringType(), True)
        ])

        # Datos ficticios simulando la consulta
        data = [
            ("20525367747",	"FA01-0179521"),
            ("20525367747",	"FA01-0179522"),
            ("20525367747",	"FA01-0179523"),
            ("20525367747",	"FA01-0179182"),
            ("20100176964",	"F103-0055652"),
            ("20100178401",	"F129-0070290"),
            ("20100176964",	"F119-0006904"),
            ("20100176964",	"F119-0007309"),
            ("20525367747",	"FA01-0210105")
        ]

        # Crear el DataFrame de Spark
        df_facturas_por_validar = spark.createDataFrame(data, schema)

        return df_facturas_por_validar
    
    def simulation_facturas_por_validar_with_ids(self, spark: SparkSession):
              # Definir el esquema de la tabla simulada
        schema = StructType([
            StructField("factura_id", StringType(), True),
            StructField("cod_id", StringType(), True),
            StructField("ruc_proveedor", StringType(), True),
            StructField("nro_factu", StringType(), True)
        ])
         
        # Datos ficticios simulando la consulta
        data = [
           ("208770",	"50",	"20525367747",	"FA01-0179521"),
            ("208771",	"51",	"20525367747",	"FA01-0179522"),
            ("208772",	"52",	"20525367747",	"FA01-0179523"),
            ("208776",	"80",	"20525367747",	"FA01-0179182"),
            ("227815",	"27524",	"20100176964",	"F103-0055652"),
            ("231030",	"31966",	"20100178401",	"F129-0070290"),
            ("259611",	"71281",	"20100176964",	"F119-0006904"),
            ("278978",	"95692",	"20100176964",	"F119-0007309"),
            ("277324",	"98127",	"20525367747",	"FA01-0210105")
        ]

        # Crear el DataFrame de Spark
        df_facturas_por_validar = spark.createDataFrame(data, schema)

        return df_facturas_por_validar