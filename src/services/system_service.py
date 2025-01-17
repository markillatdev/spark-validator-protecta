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

    def operations_attention(self):
        try:
            if self.system == Constants.SYSTEM_SABSA:
                sabsa = SabsaCore("sabsa_dev")
                sabsa.execute_attentions()
            elif self.system == Constants.SYSTEM_COBERTURA:
                cobertura = CoberturaCore(self.system)
                cobertura.execute_attentions()
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Sistema '{self.system}' no encontrado"
                )
            return {"msg": "La operación ha sido ejecutada"}
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
        
    def operations_invoices(self):
        try:
            if self.system == Constants.SYSTEM_SABSA:
                sabsa = SabsaCore("sabsa_dev")
                sabsa.execute_invoices()
            elif self.system == Constants.SYSTEM_COBERTURA:
                cobertura = CoberturaCore(self.system)
                cobertura.execute_invoices()
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Sistema '{self.system}' no encontrado"
                )
            return {"msg": "La operación ha sido ejecutada"}
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