from config.database import PARQUET_INVOICES_PATHS
from config.spark_config import get_spark_session
from schemas.schema import responseBasicSchema

class CountDataframe:

    def __init__(self):
        self.spark = get_spark_session()

    def count_merged_records(self, repository: str) -> responseBasicSchema:
        path = PARQUET_INVOICES_PATHS.get(repository)
        if path:
            try:
                parquets = self.spark.read.parquet(path)
                count = parquets.count()
                return {
                    "msg": f"El sistema '{repository}' contiene un total de {count} registros.",
                    "success": True
                }
            except Exception as e:
                return {
                    "msg": f"Error al procesar los registros para el sistema '{repository}': {str(e)}",
                    "success": False
                }
        else:
            return {
                "msg": f"No se encontró una ruta válida para el sistema '{repository}'.",
                "success": False
            }