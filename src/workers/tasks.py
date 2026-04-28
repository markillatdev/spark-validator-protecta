from core.celery_app import celery_app
from config.spark_config import get_spark_session
from config.db_connection import create_db_connection
from core.protecta_core import ProtectaCore
from typing import List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@celery_app.task(bind=True)
def validate_invoices_task(self, invoiceIds: List[int], system: str):
    try:
        logger.info(f"Iniciando validación de facturas para system={system}, invoices={invoiceIds}")
        spark = get_spark_session()
        if spark is None:
            return {"success": False, "msg": "No se pudo crear SparkSession", "total": 0}
        
        core = ProtectaCore(system)
        core.execute_invoices(invoiceIds)
        
        return {"success": True, "msg": "Validación de facturas completada", "total": len(invoiceIds)}
    except Exception as e:
        logger.error(f"Error en validate_invoices_task: {str(e)}")
        return {"success": False, "msg": f"Error: {str(e)}", "total": 0}

@celery_app.task(bind=True)
def validate_attention_task(self, invoiceIds: List[int], system: str):
    try:
        logger.info(f"Iniciando validación de atenciones para system={system}, invoices={invoiceIds}")
        spark = get_spark_session()
        if spark is None:
            return {"success": False, "msg": "No se pudo crear SparkSession", "total": 0}
        
        core = ProtectaCore(system)
        core.execute_attentions(invoiceIds)
        
        return {"success": True, "msg": "Validación de atenciones completada", "total": len(invoiceIds)}
    except Exception as e:
        logger.error(f"Error en validate_attention_task: {str(e)}")
        return {"success": False, "msg": f"Error: {str(e)}", "total": 0}

@celery_app.task(bind=True)
def validate_taxtypes_task(self, invoiceIds: List[int], system: str):
    try:
        logger.info(f"Iniciando validación de tipos de impuesto para system={system}, invoices={invoiceIds}")
        spark = get_spark_session()
        if spark is None:
            return {"success": False, "msg": "No se pudo crear SparkSession", "total": 0}
        
        core = ProtectaCore(system)
        core.execute_taxtypes(invoiceIds)
        
        return {"success": True, "msg": "Validación de tipos de impuesto completada", "total": len(invoiceIds)}
    except Exception as e:
        logger.error(f"Error en validate_taxtypes_task: {str(e)}")
        return {"success": False, "msg": f"Error: {str(e)}", "total": 0}

@celery_app.task(bind=True)
def validate_amounts_task(self, invoiceIds: List[int], system: str):
    try:
        logger.info(f"Iniciando validación de montos para system={system}, invoices={invoiceIds}")
        spark = get_spark_session()
        if spark is None:
            return {"success": False, "msg": "No se pudo crear SparkSession", "total": 0}
        
        core = ProtectaCore(system)
        core.execute_amounts(invoiceIds)
        
        return {"success": True, "msg": "Validación de montos completada", "total": len(invoiceIds)}
    except Exception as e:
        logger.error(f"Error en validate_amounts_task: {str(e)}")
        return {"success": False, "msg": f"Error: {str(e)}", "total": 0}

@celery_app.task(bind=True)
def update_invoices_unique_task(self, invoiceIds: List[int], system: str):
    try:
        logger.info(f"Iniciando actualización de facturas únicas para system={system}, invoices={invoiceIds}")
        core = ProtectaCore(system)
        core.execute_update_invoices(invoiceIds)
        
        return {"success": True, "msg": "Actualización de facturas únicas completada", "total": len(invoiceIds)}
    except Exception as e:
        logger.error(f"Error en update_invoices_unique_task: {str(e)}")
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
