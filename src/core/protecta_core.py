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
    
    def execute_attentions(self, invoiceIds: List[int]):
        self.validate_attention(self.systems(), invoiceIds)

    def execute_invoices(self, invoiceIds: List[int]):
        self.validate_invoices(self.systems(), invoiceIds)

    def execute_taxtypes(self, invoiceIds: List[int]):
        self.validate_taxtype(self.systems(), invoiceIds)

    def execute_amounts(self, invoiceIds: List[int]):
        self.validate_amount(self.systems(), invoiceIds)
    
    def execute_update_invoices(self, invoiceIds: List[int]):
        self.update_invoices_unique(invoiceIds)

    def execute_reset_invoices(self, invoiceIds: List[int]):
        self.update_reset_invoices(invoiceIds)

    def execute_tasks(self, invoiceIds: List[int]):
        self.validate_duplicate(self.systems(), invoiceIds)