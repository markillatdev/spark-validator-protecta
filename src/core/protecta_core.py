from typing import List
from core.base_main import BaseMain
from utils.constants import Constants

class ProtectaCore(BaseMain):
    
    def __init__(self, name: str):
        super().__init__(name)

    def systems(self) -> list:
        return [
            {"name": Constants.SYSTEM_SILUX_PROTECTA},
            {"name": Constants.SYSTEM_SILUX_PROTECTA, "load_dataframes": True},
            {"name": Constants.SYSTEM_SOLBEN_PROTECTA, "load_dataframes": True},
        ]
    
    def execute_reset_invoices(self, invoiceIds: List[int]):
        self.update_reset_invoices(invoiceIds)
