from typing import List
from core.base_main import BaseMain
from utils.constants import Constants

class SabsaCore(BaseMain):
    
    def __init__(self, name: str):
        super().__init__(name)

    def systems(self) -> list:
        return [
            {"name": Constants.SYSTEM_SILUX_SABSA},
            {"name": Constants.SYSTEM_SILUX_COBERTURA},
            {"name": Constants.SYSTEM_SILUX_SABSA, "load_dataframes": True},
            {"name": Constants.SYSTEM_SILUX_COBERTURA, "load_dataframes": True},
            {"name": Constants.SYSTEM_UNIX_SABSA, "load_dataframes": True},
            {"name": Constants.SYSTEM_UNIX_COBERTURA, "load_dataframes": True}
        ]
    
    def execute_attentions(self, invoiceIds: List[int]):
        self.validate_attention(self.systems(), invoiceIds)

    def execute_invoices(self, invoiceIds: List[int]):
        self.validate_invoices(self.systems(), invoiceIds)

    def execute_update_invoices(self, invoiceIds: List[int]):
        self.update_invoices_unique(invoiceIds)

    def execute_reset_invoices(self, invoiceIds: List[int]):
        self.update_reset_invoices(invoiceIds)