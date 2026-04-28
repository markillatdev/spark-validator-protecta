from typing import List
from config.db_connection import create_db_connection
from services.validator.update_service import InvoiceUpdate
from dotenv import load_dotenv # type: ignore
load_dotenv()

class BaseMain:

    def __init__(self, system: str):
        self.system = system

    def update_reset_invoices(self, invoiceIds: List[int]):
        connection = create_db_connection(self.system)
        invoice = InvoiceUpdate(connection)
        invoice.update_reset_invoices("", invoiceIds)