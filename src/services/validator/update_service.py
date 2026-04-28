from typing import List
from pymysql.connections import Connection
from utils.constants import Constants
from utils.queries_handler import (
    update_factura,
    update_factura_unique,
    update_reset_invoices,
    update_spark_processing,
    insert_factura_validacion_duplicados,
    get_factura_validacion_id)

class InvoiceUpdate:

    def __init__(self, connection: Connection):
        self.connection = connection


    def update_invoices_detected(self, message: str, factura_id: int, system: str, factura_ids: List[int]):
        cursor = self.connection.cursor()
        try:
            cursor.execute(update_factura, (
                message, 2, 3,
                1 if system == Constants.SYSTEM_SILUX_PROTECTA else 0,
                1 if system == Constants.SYSTEM_SOLBEN_PROTECTA else 0,
                factura_id
            ))
            if factura_ids:
                cursor.execute(get_factura_validacion_id, (factura_id,))
                result = cursor.fetchone()
                factura_validacion_id = result[0] if result else None

                data = [(fid, factura_validacion_id) for fid in factura_ids]
                query, values = insert_factura_validacion_duplicados(data)

                for v in values:
                    cursor.execute(query, v)
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

    
    def update_reset_invoices(self, message: str, invoiceIds: List[int]):
        try:
            query = update_reset_invoices(invoiceIds)
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
