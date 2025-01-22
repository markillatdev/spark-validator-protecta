from typing import List
from core.base_main import BaseMain
from utils.constants import Constants

class CoberturaCore(BaseMain):
    
    def __init__(self, name: str):
        super().__init__(name)

    def resources_attentions(self) -> list:
        return [
            {"system": Constants.SYSTEM_SOLBEN_SABSA, "filename": "attentions/unix_sabsa/solben_sabsa_attentions_2024.parquet"},
            {"system": Constants.SYSTEM_SOLBEN_COBERTURA, "filename": "attentions/unix_cobertura/solben_cobertura_attentions_2024.parquet"}
        ]
    
    def resources_invoices(self) -> list:
        return [
            {"system": Constants.SYSTEM_SOLBEN_SABSA, "filename": "invoices/unix_sabsa/solben_sabsa_invoices_2024.parquet"},
            {"system": Constants.SYSTEM_SOLBEN_COBERTURA, "filename": "invoices/unix_cobertura/solben_cobertura_invoices_2024.parquet"}
        ]

    def systems(self) -> list:
        return [
            {"name": Constants.SYSTEM_SILUX_SABSA},
            {"name": Constants.SYSTEM_SILUX_COBERTURA},
            {"name": Constants.SYSTEM_UNIX_SABSA, "load_dataframes": True},
            {"name": Constants.SYSTEM_UNIX_COBERTURA, "load_dataframes": True}
        ]
    
    def execute_attentions(self, invoiceIds: List[int]):
        self.validate_attention(self.resources_attentions(), self.systems(), invoiceIds)

    def execute_invoices(self, invoiceIds: List[int]):
        self.validate_invoices(self.resources_invoices(), self.systems(), invoiceIds)

    def execute_update_invoices(self, invoiceIds: List[int]):
        self.update_invoices_unique(invoiceIds)