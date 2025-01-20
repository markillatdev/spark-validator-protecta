from typing import List
from config.spark_config import create_spark_session
from utils.parquet_handler import load_or_create_parquet
from utils.constants import Constants
from dotenv import load_dotenv # type: ignore
load_dotenv()

class DataFrameLoader:


    def __init__(self, system: str):
        self.system = system


    def load_resources(self, year: int, db_table: str, resource_type: str):
        systems = [
            {"system": Constants.SYSTEM_SOLBEN_SABSA, "filename": f"{resource_type}/unix_sabsa/solben_sabsa_{resource_type}_{year}.parquet"},
            {"system": Constants.SYSTEM_SOLBEN_COBERTURA, "filename": f"{resource_type}/unix_cobertura/solben_cobertura_{resource_type}_{year}.parquet"}
        ]
        spark = create_spark_session()
        for resource in systems:
            load_or_create_parquet(spark, db_table, resource['filename'], resource['system'])


    def load_attention(self, year: int, db_table_liquidacion_ordenes: str):
        self.load_resources(year, db_table_liquidacion_ordenes, "attentions")


    def load_invoices(self, year: int, db_table_liquidacion_facturas: str):
        self.load_resources(year, db_table_liquidacion_facturas, "invoices")


    def load_data(self, years: List[int]) -> str:
        years_str = ", ".join(str(year) for year in years)
        years_text = "_".join(str(year) for year in years)
        db_table_liquidacion_ordenes, db_table_liquidacion_facturas = self.querys(years_str)
        self.load_attention(years_text, db_table_liquidacion_ordenes)
        self.load_invoices(years_text, db_table_liquidacion_facturas)
        return {"msg": "Datos cargados exitosamente"}


    def querys(self, years_str: str) -> str:
        db_table_liquidacion_ordenes = f"""
        (
            SELECT
            CONCAT(cliente, '-' , cod_titula, '-' , categoria) AS codigo_afiliado,
            tot_clini AS monto,
            nro_soli AS nro_solben,
            ruc AS ruc_proveedor
            FROM liquidacion 
            WHERE YEAR(proceso) IN ({years_str})
        ) AS subquery
        """
        db_table_liquidacion_facturas = f"""
        (
            SELECT
            ruc as ruc_proveedor,
            nro_factu
            FROM liquidacion 
            WHERE YEAR(proceso) IN ({years_str})
        ) AS subquery
        """
        return db_table_liquidacion_ordenes, db_table_liquidacion_facturas