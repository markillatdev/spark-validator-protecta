from typing import List
from config.spark_config import create_spark_session
from schemas.schema import responseBasicSchema
from utils.parquet_handler import load_or_create_parquet
from utils.constants import Constants
import os
import shutil
from dotenv import load_dotenv # type: ignore
load_dotenv()

class DataFrameLoader:


    def __init__(self, system: str):
        self.system = system


    def load_resources(self, year: int, db_table: str, resource_type: str, origen: str):
        directory = f"{resource_type}/{origen}"
        if not os.path.exists(os.getenv("STORAGE_PATH") + "/" + directory):
            os.makedirs(os.getenv("STORAGE_PATH") + "/" + directory)
        filename = f"{directory}/solben_{resource_type}_{year}.parquet"
        spark = create_spark_session()
        load_or_create_parquet(spark, db_table, filename, origen)


    def load_attention(self, year: int, db_table_liquidacion_ordenes: str, origen: str):
        self.load_resources(year, db_table_liquidacion_ordenes, "attentions", origen)


    def load_invoices(self, year: int, db_table_liquidacion_facturas: str, origen: str):
        self.load_resources(year, db_table_liquidacion_facturas, "invoices", origen)


    def load_data(self, years: List[int], origen: str) -> responseBasicSchema:
        years_str = ", ".join(str(year) for year in years)
        years_text = "_".join(str(year) for year in years)
        if not origen in {Constants.SYSTEM_UNIX_SABSA, Constants.SYSTEM_UNIX_COBERTURA, Constants.SYSTEM_SILUX_SABSA, Constants.SYSTEM_SILUX_COBERTURA}:
            return {"msg": f"No existe el sistema {origen}", "success": False}
        db_table_liquidacion_ordenes, db_table_liquidacion_facturas = self.querys(years_str, origen)
        if not db_table_liquidacion_ordenes or not db_table_liquidacion_facturas:
            return {"msg": "No se pudo realizar la carga", "success": False}
        self.load_attention(years_text, db_table_liquidacion_ordenes, origen)
        self.load_invoices(years_text, db_table_liquidacion_facturas, origen)
        return {"msg": "Datos cargados exitosamente", "success": True}


    def querys(self, years_str: str, origen: str) -> str:

        if origen in {Constants.SYSTEM_UNIX_SABSA, Constants.SYSTEM_UNIX_COBERTURA}:

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
        
        elif origen in {Constants.SYSTEM_SILUX_SABSA, Constants.SYSTEM_SILUX_COBERTURA}:

            db_table_liquidacion_ordenes = f"""
            (
                SELECT 
                l.codigo_afiliado,
                f.monto,
                CASE
                    WHEN ls.code_solben IS NULL OR ls.code_solben = "" THEN ls.nro_autoriza
                    ELSE ls.code_solben
                END as nro_solben,
                rg.ruc_proveedor
                FROM factura f
                INNER JOIN liqtempo l ON f.id = l.factura_id
                INNER JOIN liqtempo_solben ls ON ls.liqtempo_id = l.id
                INNER JOIN reporte_general rg ON l.id = rg.id_liqtempo
                WHERE l.id_estado IN (16, 17)
                AND YEAR(f.fecha_envio_iafa) IN ({years_str})
            ) AS subquery
            """

            db_table_liquidacion_facturas = f"""
            (
                SELECT 
                rg.ruc_proveedor,
                rg.nro_factu
                FROM factura f 
                INNER JOIN liqtempo l ON l.factura_id = f.id
                INNER JOIN reporte_general rg ON l.id = rg.id_liqtempo
                WHERE l.id_estado IN (16, 17)
                AND YEAR(f.fecha_envio_iafa) IN ({years_str})
            ) AS subquery
            """

            return db_table_liquidacion_ordenes, db_table_liquidacion_facturas

        else:
            return "", ""


    def destroy_dataframe(self, origen: str):
        if origen not in {Constants.SYSTEM_UNIX_SABSA, Constants.SYSTEM_UNIX_COBERTURA, Constants.SYSTEM_SILUX_SABSA, Constants.SYSTEM_SILUX_COBERTURA}:
            return {"msg": f"No existe el sistema {origen}", "success": False}
        resources = {"attentions", "invoices"}
        for resource in resources:
            directory = f"{resource}/{origen}"
            path = os.path.join(os.getenv("STORAGE_PATH"), directory)
            if not os.path.exists(path):
                return {"msg": f"No existe el directorio {path}", "success": False}
            shutil.rmtree(path)
        return {"msg": "Directorios eliminados exitosamente", "success": True}