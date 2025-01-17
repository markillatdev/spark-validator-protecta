from config.spark_config import create_spark_session
from config.db_connection import create_db_connection
from utils.parquet_handler import load_or_create_parquet
from services.validate.attention_duplicate_service import AttentionDuplicateHandler
from services.validate.invoice_duplicate_service import InvoiceDuplicateHandler
from dotenv import load_dotenv # type: ignore
from utils.queries_handler import (
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