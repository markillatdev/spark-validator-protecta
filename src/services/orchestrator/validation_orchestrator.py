from typing import List, Dict
from pyspark.sql import SparkSession
from pymysql.connections import Connection
from services.validate.invoice_duplicate_service import InvoiceDuplicateHandler
from services.validate.attention_duplicate_service import AttentionDuplicateHandler
from services.validate.taxtype_duplicate_service import TaxTypeDuplicateHandler
from services.validate.amount_duplicate_service import AmountDuplicateHandler
from services.validate.update_service import InvoiceUpdate
from config.database import PARQUET_PATHS
import os

class ValidationOrchestrator:
    
    def __init__(self, spark: SparkSession, connection: Connection, coreSystem: str):
        self.spark = spark
        self.connection = connection
        self.coreSystem = coreSystem
        self.invoice_handler = InvoiceDuplicateHandler(spark, connection, coreSystem)
        self.attention_handler = AttentionDuplicateHandler(spark, connection, coreSystem)
        self.taxtype_handler = TaxTypeDuplicateHandler(spark, connection, coreSystem)
        self.amount_handler = AmountDuplicateHandler(spark, connection, coreSystem)
        self.invoice_updater = InvoiceUpdate(connection)
        self._dataframes_cache = {}
    
    def _load_parquet_once(self, system_name: str) -> object:
        if system_name not in self._dataframes_cache:
            path = PARQUET_PATHS.get(system_name)
            if path and os.path.exists(os.path.dirname(path)):
                self._dataframes_cache[system_name] = self.spark.read.parquet(path)
            else:
                self._dataframes_cache[system_name] = None
        return self._dataframes_cache[system_name]
    
    def execute_all_validations(self, systems_validate: List[Dict], invoiceIds: List[int]):
        for system in systems_validate:
            system_name = system['name']
            
            data_parquet = None
            if system.get("load_dataframes"):
                data_parquet = self._load_parquet_once(system_name)
            
            self.invoice_handler.procesar_facturas(data_parquet, system, invoiceIds)
            self.attention_handler.procesar_atenciones(data_parquet, system, invoiceIds)
            self.taxtype_handler.procesar_tipo_impuesto(data_parquet, system, invoiceIds)
            self.amount_handler.procesar_montos(data_parquet, system, invoiceIds)
        
        self.invoice_updater.update_invoices_unique("Factura unica", invoiceIds)
