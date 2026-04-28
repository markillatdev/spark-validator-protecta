from typing import List
from config.spark_config import get_spark_session
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

    def load_resources(self, year: str, db_table: str, origen: str):
        try:
            storage_path = os.getenv("STORAGE_PATH")
            if not storage_path:
                raise ValueError("STORAGE_PATH environment variable is not set")
            directory = origen
            resource_dir = os.path.join(storage_path, directory)
            os.makedirs(resource_dir, exist_ok=True)
            filename = os.path.join(resource_dir, f"{origen}_{year}.parquet")
            spark = get_spark_session()
            if spark is None:
                raise Exception("No se pudo crear la sesión de Spark")
            load_or_create_parquet(spark, db_table, filename, origen)
        except Exception as e:
            logger.exception("Error cargando recursos")
            raise Exception(f"Error al cargar: {str(e)}")

    def load_data(self, years: List[int], origen: str) -> responseBasicSchema:
        if origen not in {Constants.SYSTEM_SOLBEN_PROTECTA, Constants.SYSTEM_SILUX_PROTECTA}:
            return {"msg": f"No existe el sistema {origen}", "success": False}
        try:
            years_str = ", ".join(str(y) for y in years)
            years_key = "_".join(str(y) for y in years)
            db_table = self.querys(years_str, origen)
            if not db_table:
                return {"msg": "No se pudo realizar la carga", "success": False}
            self.load_resources(years_key, db_table, origen)
            return {"msg": "Datos cargados exitosamente", "success": True}
        except Exception as e:
            logger.exception("Error en load_data")
            return {"msg": f"Error al cargar datos: {str(e)}", "success": False}

    def querys(self, years_str: str, origen: str) -> str:
        if origen == Constants.SYSTEM_SOLBEN_PROTECTA:
            return f"""
            (
                SELECT
                    silux_factura_id AS factura_id,
                    cliente AS codigo_iafa,
                    ruc AS ruc_proveedor,
                    nro_factu,
                    CONCAT(cliente, '-', cod_titula, '-', categoria) AS codigo_afiliado,
                    fch_atencion,
                    nro_soli AS nro_solben,
                    tot_clini AS monto,
                    tipo_impuesto
                FROM liquidacion 
                WHERE YEAR(proceso) IN ({years_str})
                AND frecuencia = 0
            ) AS subquery
            """
        elif origen == Constants.SYSTEM_SILUX_PROTECTA:
            return f"""
            (
                SELECT
                    f.id AS factura_id,
                    l.codigo_iafa,
                    fp.ruc_proveedor, 
                    fp.nro_factura_std AS nro_factu,
                    l.codigo_afiliado,
                    l.fecha AS fch_atencion,
                    CASE
                        WHEN ls.code_solben IS NULL OR ls.code_solben = '' 
                            THEN ls.nro_autoriza
                        ELSE ls.code_solben
                    END AS nro_solben,
                    f.monto,
                    l.tipo_impuesto_id AS tipo_impuesto
                FROM factura f
                INNER JOIN liqtempo l 
                    ON l.factura_id = f.id
                INNER JOIN liqtempo_solben ls 
                    ON ls.liqtempo_id = l.id
                INNER JOIN factura_proveedor fp 
                    ON fp.factura_id = f.id
                WHERE f.id_estado IN (16, 17)
                AND YEAR(f.fecha_envio_iafa) IN ({years_str})
            ) AS subquery
            """
        return ""

    def destroy_dataframe(self, origen: str):
        if origen not in {Constants.SYSTEM_SOLBEN_PROTECTA, Constants.SYSTEM_SILUX_PROTECTA}:
            return {"msg": f"No existe el sistema {origen}", "success": False}
        try:
            storage_path = os.getenv("STORAGE_PATH")
            if not storage_path:
                return {"msg": "STORAGE_PATH no configurado", "success": False}
            removed = []
            path = os.path.join(storage_path, origen)
            if os.path.exists(path):
                shutil.rmtree(path)
            return {"msg": f"Directorios eliminados: {', '.join(removed) or 'ninguno'}", "success": True}
        except Exception as e:
            logger.exception("Error en destroy_dataframe")
            return {"msg": f"Error al eliminar directorios: {str(e)}", "success": False}