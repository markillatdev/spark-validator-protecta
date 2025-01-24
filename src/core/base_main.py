from typing import List
from config.spark_config import create_spark_session
from config.db_connection import create_db_connection
from utils.parquet_handler import load_or_create_parquet
from services.validate.attention_duplicate_service import AttentionDuplicateHandler
from services.validate.invoice_duplicate_service import InvoiceDuplicateHandler
from services.validate.update_service import InvoiceUpdate
from dotenv import load_dotenv # type: ignore
from utils.queries_handler import (
    db_table_liquidacion_ordenes, 
    db_table_liquidacion_facturas
)
load_dotenv()

class BaseMain:


    def __init__(self, system: str):
        self.system = system


    def validate_attention(self, resources: List[str], systems_validate: List[str], invoiceIds: List[int]):
        spark = create_spark_session()
        for resource in resources:
            load_or_create_parquet(spark, db_table_liquidacion_ordenes, resource['filename'], resource['system'])

        connection = create_db_connection(self.system)
        attention = AttentionDuplicateHandler(spark, connection, self.system)
        attention.procesar_atenciones(systems_validate, invoiceIds)


    def validate_invoices(self, resources: List[str], systems_validate: List[str], invoiceIds: List[int]):
        spark = create_spark_session()     
        for resource in resources:
            load_or_create_parquet(spark, db_table_liquidacion_facturas, resource['filename'], resource['system'])

        connection = create_db_connection(self.system)
        invoice = InvoiceDuplicateHandler(spark, connection, self.system)
        invoice.procesar_facturas(systems_validate, invoiceIds)


    def update_invoices_unique(self, invoiceIds: List[int]):
        connection = create_db_connection(self.system)
        invoice = InvoiceUpdate(connection)
        invoice.update_invoices_unique("Factura unica", invoiceIds)

    def update_reset_invoices(self, invoiceIds: List[int]):
        connection = create_db_connection(self.system)
        invoice = InvoiceUpdate(connection)
        invoice.update_reset_invoices("", invoiceIds)
