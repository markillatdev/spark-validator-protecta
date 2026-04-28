from core.celery_app import celery_app
from config.spark_config import get_spark_session
from config.db_connection import create_db_connection
from core.protecta_core import ProtectaCore
from services.validate.validation_service import ValidationService
from typing import List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@celery_app.task(bind=True)
def validate_duplicate_task(self, invoiceIds: List[int], system: str):
    try:
        logger.info(f"Iniciando validación para system={system}, invoices={invoiceIds}")
        spark = get_spark_session()
        if spark is None:
            return {"success": False, "msg": "No se pudo crear SparkSession", "total": 0}

        connection = create_db_connection(system)
        service = ValidationService(spark, connection, system)
        core = ProtectaCore(system)
        service.execute(core.systems(), invoiceIds)
        
        return {"success": True, "msg": "Validación completada exitosamente", "total": len(invoiceIds)}
    except Exception as e:
        logger.error(f"Error en validate_duplicate_task: {str(e)}")
        return {"success": False, "msg": f"Error: {str(e)}", "total": 0}

@celery_app.task(bind=True)
def update_reset_invoices_task(self, invoiceIds: List[int], system: str):
    try:
        logger.info(f"Iniciando reseteo de facturas para system={system}, invoices={invoiceIds}")
        core = ProtectaCore(system)
        core.execute_reset_invoices(invoiceIds)
        
        return {"success": True, "msg": "Reseteo de facturas completado", "total": len(invoiceIds)}
    except Exception as e:
        logger.error(f"Error en update_reset_invoices_task: {str(e)}")
        return {"success": False, "msg": f"Error: {str(e)}", "total": 0}
