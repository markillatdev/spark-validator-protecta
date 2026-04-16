from typing import List
from fastapi import HTTPException, status
from core.semefa_core import SemefaCore
from utils.constants import Constants
import logging

# Configuración del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SystemService:

    def __init__(self, system: str):
        self.system = system

    def operations_attention(self, invoiceIds: List[int]):
        try:
            if self.system == Constants.SYSTEM_SILUX_SEMEFA:
                sabsa = SemefaCore(self.system)
                sabsa.execute_attentions(invoiceIds)
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Sistema '{self.system}' no encontrado"
                )
            return {"msg": "La operación ha sido ejecutada", "success": True, "total": len(invoiceIds)}
        except HTTPException as ex:
            logger.error(f"HTTPException en operations_attention: {ex.detail}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ex.detail
            )    
        except Exception as ex:
            logger.error(f"Error inesperado en operations_attention: {str(ex)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Hubo un error"
            )    
        
    def operations_invoices(self, invoiceIds: List[int]):
        try:
            if self.system == Constants.SYSTEM_SILUX_SEMEFA:
                sabsa = SemefaCore(self.system)
                sabsa.execute_invoices(invoiceIds)
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Sistema '{self.system}' no encontrado"
                )
            return {"msg": "La operación ha sido ejecutada", "success": True, "total": len(invoiceIds)}
        except HTTPException as ex:
            logger.error(f"HTTPException en operations_invoices: {ex.detail}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ex.detail
            )    
        except Exception as ex:
            logger.error(f"Error inesperado en operations_invoices: {str(ex)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Hubo un error"
            )    
        
    def operations_update_invoices(self, invoiceIds: List[int]):
        try:
            if self.system == Constants.SYSTEM_SILUX_SEMEFA:
                sabsa = SemefaCore(self.system)
                sabsa.execute_update_invoices(invoiceIds)
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Sistema '{self.system}' no encontrado"
                )
            return {"msg": "La operación de actualizar facturas unicas ha sido ejecutada", "success": True, "total": len(invoiceIds)}
        except HTTPException as ex:
            logger.error(f"HTTPException en operations_invoices: {ex.detail}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ex.detail
            )    
        except Exception as ex:
            logger.error(f"Error inesperado en operations update invoices: {str(ex)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, 
                detail="Hubo un error"
            )
        
    def operations_update_reset_invoices(self, invoiceIds: List[int]):
        try:
            if self.system == Constants.SYSTEM_SILUX_SEMEFA:
                sabsa = SemefaCore(self.system)
                sabsa.execute_reset_invoices(invoiceIds)
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Sistema '{self.system}' no encontrado"
                )
            return {"msg": "La operación de resetear facturas ha sido ejecutada", "success": True, "total": len(invoiceIds)}
        except HTTPException as ex:
            logger.error(f"HTTPException en operations_invoices: {ex.detail}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ex.detail
            )    
        except Exception as ex:
            logger.error(f"Error inesperado en operations update reset invoices: {str(ex)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, 
                detail="Hubo un error"
            )