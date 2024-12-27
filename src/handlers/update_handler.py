from src.utils.queries_handler import (
    update_factura,
    update_factura_unique,
    update_spark_processing)

class InvoiceUpdate:

    def __init__(self, connection):
        self.connection = connection


    def update_invoices_detected(self, message, factura_id, system):
        cursor = self.connection.cursor()
        try:
            cursor.execute(update_factura, (
                message, 2, 3,
                1 if system == "silux_sabsa" else 0,
                1 if system == "silux_cobertura" else 0,
                1 if system == "unix_sabsa" else 0,
                1 if system == "unix_cobertura" else 0,
                factura_id
            ))
            self.connection.commit()
        except Exception as e:
            print(f"Error actualizando factura {factura_id}: {e}")


    def update_invoices_unique(self, message):
        cursor = self.connection.cursor()
        try:
            cursor.execute(update_factura_unique, (
                message, 2, 2, 0, 0, 0, 0
            ))
            self.connection.commit()
        except Exception as e:
            print(f"Error actualizando: {e}")


    def update_spark_processing(self):
        cursor = self.connection.cursor()
        try:
            cursor.execute(update_spark_processing)
            self.connection.commit()
        except Exception as e:
            print(f"Error actualizando: {e}")
