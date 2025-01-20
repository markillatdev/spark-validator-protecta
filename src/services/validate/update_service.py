from typing import List
from utils.constants import Constants
from utils.queries_handler import (
    update_factura,
    update_factura_unique,
    update_spark_processing)

class InvoiceUpdate:

    def __init__(self, connection):
        self.connection = connection


    def update_invoices_detected(self, message: str, factura_id: int, system: str):
        cursor = self.connection.cursor()
        try:
            cursor.execute(update_factura, (
                message, 2, 3,
                1 if system == Constants.SYSTEM_SILUX_SABSA else 0,
                1 if system == Constants.SYSTEM_SILUX_COBERTURA else 0,
                1 if system == Constants.SYSTEM_UNIX_SABSA else 0,
                1 if system == Constants.SYSTEM_UNIX_COBERTURA else 0,
                factura_id
            ))
            self.connection.commit()
        except Exception as e:
            print(f"Error actualizando factura {factura_id}: {e}")


    def update_invoices_unique(self, message: str, invoiceIds: List[int]):
        try:
            query = update_factura_unique(invoiceIds)
            params = [message] + invoiceIds
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()
        except Exception as e:
            print(f"Error actualizando: {e}")


    def update_spark_processing(self, invoiceIds: List[int]):
        cursor = self.connection.cursor()
        try:
            params = invoiceIds
            cursor.execute(update_spark_processing(invoiceIds), params)
            self.connection.commit()
        except Exception as e:
            print(f"Error actualizando: {e}")
            self.connection.rollback()
