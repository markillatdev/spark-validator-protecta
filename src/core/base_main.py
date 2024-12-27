from src.config.spark_config import create_spark_session
from src.config.db_connection import create_db_connection
from src.utils.parquet_handler import load_or_create_parquet
from src.handlers.attention_duplicate_handler import AttentionDuplicateHandler
from src.handlers.invoice_duplicate_handler import InvoiceDuplicateHandler
from dotenv import load_dotenv
from src.utils.queries_handler import (
    db_table_liquidacion_ordenes, 
    db_table_liquidacion_facturas
)
load_dotenv()

class BaseMain:

    def __init__(self, system):
        self.system = system

    def validate_attention(self, resources, systems_validate):
        spark = create_spark_session()
        for resource in resources:
            load_or_create_parquet(spark, db_table_liquidacion_ordenes, resource['filename'], resource['system'])

        connection = create_db_connection(self.system)
        attention = AttentionDuplicateHandler(spark, connection, self.system)
        attention.procesar_atenciones(systems_validate)

    def validate_invoices(self, resources, systems_validate):
        spark = create_spark_session()     
        for resource in resources:
            load_or_create_parquet(spark, db_table_liquidacion_facturas, resource['filename'], resource['system'])

        connection = create_db_connection(self.system)
        invoice = InvoiceDuplicateHandler(spark, connection, self.system)
        invoice.procesar_facturas(systems_validate)