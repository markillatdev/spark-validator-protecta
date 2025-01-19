from typing import List
from core.base_main import BaseMain

class SabsaCore(BaseMain):
    
    def __init__(self, name):
        super().__init__(name)

    def resources_attentions(self) -> list:
        return [
            {"system": "sabsa_solben", "filename": "attentions/unix_sabsa/solben_sabsa_attentions_2024.parquet"}
        ]
    
    def resources_invoices(self) -> list:
        return [
            {"system": "sabsa_solben", "filename": "invoices/unix_sabsa/solben_sabsa_invoices_2024.parquet"}
        ]

    def systems(self) -> list:
        return [
            {"name": "sabsa_dev"},
            {"name": "unix_sabsa", "load_dataframes": True}
        ]
    
    def execute_attentions(self, invoiceIds: List[int]):
        self.validate_attention(self.resources_attentions(), self.systems(), invoiceIds)

    def execute_invoices(self, invoiceIds: List[int]):
        self.validate_invoices(self.resources_invoices(), self.systems(), invoiceIds)

    def execute_update_invoices(self, invoiceIds: List[int]):
        self.update_invoices_unique(invoiceIds)