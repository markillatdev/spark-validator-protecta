from typing import List
from fastapi import HTTPException, status
from core.sabsa_core import SabsaCore
from core.cobertura_core import CoberturaCore
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
            if self.system == Constants.SYSTEM_SILUX_SABSA:
                sabsa = SabsaCore(self.system)
                sabsa.execute_attentions(invoiceIds)
            elif self.system == Constants.SYSTEM_SILUX_COBERTURA:
                cobertura = CoberturaCore(self.system)
                cobertura.execute_attentions(invoiceIds)
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
            if self.system == Constants.SYSTEM_SILUX_SABSA:
                sabsa = SabsaCore(self.system)
                sabsa.execute_invoices(invoiceIds)
            elif self.system == Constants.SYSTEM_SILUX_COBERTURA:
                cobertura = CoberturaCore(self.system)
                cobertura.execute_invoices(invoiceIds)
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
            if self.system == Constants.SYSTEM_SILUX_SABSA:
                sabsa = SabsaCore(self.system)
                sabsa.execute_update_invoices(invoiceIds)
            elif self.system == Constants.SYSTEM_SILUX_COBERTURA:
                cobertura = CoberturaCore(self.system)
                cobertura.execute_update_invoices(invoiceIds)
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