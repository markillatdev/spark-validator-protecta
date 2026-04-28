from pyspark.sql import SparkSession, DataFrame
from pymysql.connections import Connection
from services.validate.attention_duplicate_service import AttentionDuplicateHandler
from services.validate.taxtype_duplicate_service import TaxTypeDuplicateHandler
from services.validate.invoice_duplicate_service import InvoiceDuplicateHandler
from services.validate.amount_duplicate_service import AmountDuplicateHandler
from services.validate.update_service import InvoiceUpdate
from config.database import PARQUET_PATHS
from typing import List
import os

class ValidationService:

    def __init__(self, spark: SparkSession, connection: Connection, coreSystem: str):
        self.spark = spark
        self.invoice = InvoiceDuplicateHandler(spark, connection, coreSystem)
        self.attention = AttentionDuplicateHandler(spark, connection, coreSystem)
        self.taxtype = TaxTypeDuplicateHandler(spark, connection, coreSystem)
        self.amount = AmountDuplicateHandler(spark, connection, coreSystem)
        self.invoice_updater = InvoiceUpdate(connection)

    def execute(self, systems_validate: List[str], invoiceIds: List[int]):
        for system in systems_validate:
            
            if (system.get("load_dataframes")):
                dataParquet = self.load_dataframes(system['name'])
            else:
                dataParquet = None

            self.invoice.procesar_facturas(dataParquet, system, invoiceIds)
            self.attention.procesar_atenciones(dataParquet, system, invoiceIds)
            self.taxtype.procesar_tipo_impuesto(dataParquet, system, invoiceIds)
            self.amount.procesar_montos(dataParquet, system, invoiceIds)
        self.invoice_updater.update_invoices_unique("Factura unica", invoiceIds)

    def load_dataframes(self, system: str):
        path = PARQUET_PATHS.get(system)
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            return None
        return self.spark.read.parquet(path) if path else None