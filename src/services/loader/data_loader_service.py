from typing import List
from config.spark_config import create_spark_session
from schemas.schema import responseBasicSchema
from utils.parquet_handler import load_or_create_parquet
from utils.constants import Constants
import os
import shutil
import logging
from dotenv import load_dotenv # type: ignore
load_dotenv()

logger = logging.getLogger(__name__)

class DataFrameLoader:


    def __init__(self, system: str):
        self.system = system


    def load_resources(self, year: int, db_table: str, resource_type: str, origen: str):
        try:
            directory = f"{resource_type}/{origen}"
            if not os.path.exists(os.getenv("STORAGE_PATH") + "/" + directory):
                os.makedirs(os.getenv("STORAGE_PATH") + "/" + directory)
            filename = f"{directory}/solben_{resource_type}_{year}.parquet"
            spark = create_spark_session()
            if spark is None:
                raise Exception("No se pudo crear la sesión de Spark")
            load_or_create_parquet(spark, db_table, filename, origen)
        except Exception as e:
            logger.error(f"Error cargando {resource_type}: {str(e)}")
            raise Exception(f"Error al cargar {resource_type}: {str(e)}")

    def load_attention(self, year: int, db_table_liquidacion_ordenes: str, origen: str):
        self.load_resources(year, db_table_liquidacion_ordenes, "attentions", origen)

    def load_invoices(self, year: int, db_table_liquidacion_facturas: str, origen: str):
        self.load_resources(year, db_table_liquidacion_facturas, "invoices", origen)

    def load_tax_type(self, year: int, db_table_liquidacion_tipo_impuesto: str, origen: str):
        self.load_resources(year, db_table_liquidacion_tipo_impuesto, "taxtypes", origen)

    def load_amount(self, year: int, db_table_liquidacion_importe: str, origen: str):
        self.load_resources(year, db_table_liquidacion_importe, "amounts", origen)

    def load_data(self, years: List[int], origen: str) -> responseBasicSchema:
        try:
            years_str = ", ".join(str(year) for year in years)
            years_text = "_".join(str(year) for year in years)
            if not origen in {Constants.SYSTEM_SOLBEN_SEMEFA, Constants.SYSTEM_SILUX_SEMEFA}:
                return {"msg": f"No existe el sistema {origen}", "success": False}
            db_table_liquidacion_ordenes, db_table_liquidacion_facturas, db_table_liquidacion_tipo_impuesto, db_table_liquidacion_importe = self.querys(years_str, origen)
            if not db_table_liquidacion_ordenes or not db_table_liquidacion_facturas or not db_table_liquidacion_tipo_impuesto or not db_table_liquidacion_importe:
                return {"msg": "No se pudo realizar la carga", "success": False}
            self.load_attention(years_text, db_table_liquidacion_ordenes, origen)
            self.load_invoices(years_text, db_table_liquidacion_facturas, origen)
            self.load_tax_type(years_text, db_table_liquidacion_tipo_impuesto, origen)
            self.load_amount(years_text, db_table_liquidacion_importe, origen)
            return {"msg": "Datos cargados exitosamente", "success": True}
        except Exception as e:
            logger.error(f"Error en load_data: {str(e)}")
            return {"msg": f"Error al cargar datos: {str(e)}", "success": False}


    def querys(self, years_str: str, origen: str) -> str:

        if origen == Constants.SYSTEM_SOLBEN_SEMEFA:

            db_table_liquidacion_ordenes = f"""
            (
                SELECT
                silux_factura_id AS factura_id,
                CONCAT(cliente, '-' , cod_titula, '-' , categoria) AS codigo_afiliado,
                tot_clini AS monto,
                nro_soli AS nro_solben,
                ruc AS ruc_proveedor
                FROM liquidacion 
                WHERE YEAR(proceso) IN ({years_str})
                AND frecuencia = 0
            ) AS subquery
            """

            db_table_liquidacion_facturas = f"""
            (
                SELECT
                silux_factura_id AS factura_id,
                ruc as ruc_proveedor,
                nro_factu
                FROM liquidacion 
                WHERE YEAR(proceso) IN ({years_str})
                AND frecuencia = 0
            ) AS subquery
            """

            db_table_liquidacion_tipo_impuesto = f"""
            (
                SELECT
                silux_factura_id AS factura_id,
                CONCAT(cliente, '-' , cod_titula, '-' , categoria) AS codigo_afiliado,
                nro_soli AS nro_solben,
                ruc AS ruc_proveedor,
                tipo_impuesto
                FROM liquidacion 
                WHERE YEAR(proceso) IN ({years_str})
                AND frecuencia = 0
            ) AS subquery
            """

            db_table_liquidacion_importe = f"""
            (
                SELECT
                silux_factura_id AS factura_id,
                CONCAT(cliente, '-' , cod_titula, '-' , categoria) AS codigo_afiliado,
                tot_clini AS monto,
                ruc AS ruc_proveedor,
                tipo_impuesto
                FROM liquidacion 
                WHERE YEAR(proceso) IN ({years_str})
                AND frecuencia = 0            
            ) AS subquery
            """

            return db_table_liquidacion_ordenes, db_table_liquidacion_facturas, db_table_liquidacion_tipo_impuesto, db_table_liquidacion_importe
        
        elif origen == Constants.SYSTEM_SILUX_SEMEFA:

            db_table_liquidacion_ordenes = f"""
            (
                SELECT 
                f.id AS factura_id,
                l.codigo_afiliado,
                f.monto,
                CASE
                    WHEN LENGTH(l.numero_segunda_solicitud) = 8
                    THEN l.numero_segunda_solicitud
                    ELSE l.numero_de_solben
                END AS nro_solben,
                fp.ruc_proveedor
                FROM factura f
                INNER JOIN liqtempo l ON l.factura_id = f.id
                INNER JOIN liqtempo_solben ls ON ls.liqtempo_id = l.id
                INNER JOIN factura_proveedor fp ON fp.factura_id = f.id
                WHERE f.id_estado IN (16, 17)
                AND YEAR(f.fecha_envio_iafa) IN ({years_str})
            ) AS subquery
            """

            db_table_liquidacion_facturas = f"""
            (
                SELECT
                f.id AS factura_id,
                fp.ruc_proveedor,
                fp.nro_factura_std as nro_factu
                FROM factura f 
                INNER JOIN factura_proveedor fp ON fp.factura_id = f.id
                WHERE f.id_estado IN (16, 17)
                AND YEAR(f.fecha_envio_iafa) IN ({years_str})
            ) AS subquery
            """

            db_table_liquidacion_tipo_impuesto = f"""
            (
                SELECT
                f.id AS factura_id,
                l.codigo_afiliado,
                CASE
                    WHEN LENGTH(l.numero_segunda_solicitud) = 8
                    THEN l.numero_segunda_solicitud
                    ELSE l.numero_de_solben
                END AS nro_solben,
                fp.ruc_proveedor,
                l.tipo_impuesto_id as tipo_impuesto 
                FROM factura f
                INNER JOIN liqtempo l ON l.factura_id = f.id
                INNER JOIN liqtempo_solben ls ON ls.liqtempo_id = l.id
                INNER JOIN factura_proveedor fp ON fp.factura_id = f.id
                WHERE f.id_estado IN (16, 17)
                AND YEAR(f.fecha_envio_iafa) IN ({years_str})
            ) AS subquery
            """

            db_table_liquidacion_importe = f"""
            (
                SELECT
                f.id AS factura_id,
                l.codigo_afiliado,
                f.monto,
                fp.ruc_proveedor,
                l.tipo_impuesto_id as tipo_impuesto 
                FROM factura f
                INNER JOIN liqtempo l ON l.factura_id = f.id
                INNER JOIN liqtempo_solben ls ON ls.liqtempo_id = l.id
                INNER JOIN factura_proveedor fp ON fp.factura_id = f.id
                WHERE f.id_estado IN (16, 17)
                AND YEAR(f.fecha_envio_iafa) IN ({years_str})
            ) AS subquery
            """

            return db_table_liquidacion_ordenes, db_table_liquidacion_facturas, db_table_liquidacion_tipo_impuesto, db_table_liquidacion_importe

        else:
            return "", ""


    def destroy_dataframe(self, origen: str):
        try:
            if origen not in {Constants.SYSTEM_SOLBEN_SEMEFA, Constants.SYSTEM_SILUX_SEMEFA}:
                return {"msg": f"No existe el sistema {origen}", "success": False}
            resources = {"attentions", "invoices", "tax_type", "amount"}
            for resource in resources:
                directory = f"{resource}/{origen}"
                path = os.path.join(os.getenv("STORAGE_PATH"), directory)
                if not os.path.exists(path):
                    return {"msg": f"No existe el directorio {path}", "success": False}
                shutil.rmtree(path)
            return {"msg": "Directorios eliminados exitosamente", "success": True}
        except Exception as e:
            logger.error(f"Error en destroy_dataframe: {str(e)}")
            return {"msg": f"Error al eliminar directorios: {str(e)}", "success": False}