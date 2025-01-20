from typing import List
from core.base_main import BaseMain

class CoberturaCore(BaseMain):
    
    def __init__(self, name: str):
        super().__init__(name)

    def resources_attentions(self) -> list:
        return [
            {"system": "sabsa_solben", "filename": "attentions/unix_sabsa/solben_sabsa_attentions_2024.parquet"},
            {"system": "sabsa_cobertura", "filename": "attentions/unix_cobertura/solben_cobertura_attentions_2024.parquet"}
        ]
    
    def resources_invoices(self) -> list:
        return [
            {"system": "sabsa_solben", "filename": "invoices/unix_sabsa/solben_sabsa_invoices_2024.parquet"},
            {"system": "sabsa_cobertura", "filename": "invoices/unix_cobertura/solben_cobertura_invoices_2024.parquet"}
        ]

    def systems(self) -> list:
        return [
            {"name": "silux_sabsa"},
            {"name": "silux_cobertura"},
            {"name": "unix_sabsa", "load_dataframes": True},
            {"name": "unix_cobertura", "load_dataframes": True}
        ]
    
    def execute_attentions(self):
        self.validate_attention(self.resources_attentions(), self.systems())

    def execute_invoices(self):
        self.validate_invoices(self.resources_invoices(), self.systems())

    def execute_update_invoices(self, invoiceIds: List[int]):
        self.update_invoices_unique(invoiceIds)