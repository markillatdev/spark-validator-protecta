from typing import List
from config.spark_config import get_spark_session
from config.db_connection import create_db_connection
from services.validate.attention_duplicate_service import AttentionDuplicateHandler
from services.validate.invoice_duplicate_service import InvoiceDuplicateHandler
from services.validate.taxtype_duplicate_service import TaxTypeDuplicateHandler
from services.validate.amount_duplicate_service import AmountDuplicateHandler
from services.validate.update_service import InvoiceUpdate
from src.services.validate.validation_service import ValidationService
from dotenv import load_dotenv # type: ignore
load_dotenv()

class BaseMain:


    def __init__(self, system: str):
        self.system = system


    def validate_attention(self, systems_validate: List[str], invoiceIds: List[int]):
        spark = get_spark_session()
        connection = create_db_connection(self.system)
        attention = AttentionDuplicateHandler(spark, connection, self.system)
        attention.procesar_atenciones(systems_validate, invoiceIds)


    def validate_invoices(self, systems_validate: List[str], invoiceIds: List[int]):
        spark = get_spark_session()     
        connection = create_db_connection(self.system)
        invoice = InvoiceDuplicateHandler(spark, connection, self.system)
        invoice.procesar_facturas(systems_validate, invoiceIds)


    def validate_taxtype(self, systems_validate: List[str], invoiceIds: List[int]):
        spark = get_spark_session()     
        connection = create_db_connection(self.system)
        taxtype = TaxTypeDuplicateHandler(spark, connection, self.system)
        taxtype.procesar_tipo_impuesto(systems_validate, invoiceIds)    
        
    
    def validate_amount(self, systems_validate: List[str], invoiceIds: List[int]):
        spark = get_spark_session()     
        connection = create_db_connection(self.system)
        amount = AmountDuplicateHandler(spark, connection, self.system)
        amount.procesar_montos(systems_validate, invoiceIds)


    def update_invoices_unique(self, invoiceIds: List[int]):
        connection = create_db_connection(self.system)
        invoice = InvoiceUpdate(connection)
        invoice.update_invoices_unique("Factura unica", invoiceIds)


    def update_reset_invoices(self, invoiceIds: List[int]):
        connection = create_db_connection(self.system)
        invoice = InvoiceUpdate(connection)
        invoice.update_reset_invoices("", invoiceIds)


    def validate_duplicate(self, systems_validate: List[str], invoiceIds: List[int]):
        spark = get_spark_session()
        connection = create_db_connection(self.system)
        service = ValidationService(spark, connection, self.system)
        service.execute(systems_validate, invoiceIds)