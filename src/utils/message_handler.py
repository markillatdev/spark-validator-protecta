class MessageHandler:  

    @staticmethod
    def message_attention_duplicate(message: str, value: object, system: str) -> str:
        return f"{message}: La Atencion {value.nro_solben} se encuentra duplicado en: {system}, monto: {value.monto}, codigo afiliado: {value.codigo_afiliado}, clinica: {value.ruc_proveedor}"
    
    @staticmethod
    def message_invoice_duplicate(message: str, value: object, system: str) -> str:
        return f"{message}: La factura {value.nro_factu} se encuentra duplicado en: {system}, clinica: {value.ruc_proveedor}"
    
    @staticmethod
    def message_taxtype_duplicate(message: str, value: object, system: str) -> str:
        return f"{message}: El tipo de impuesto {value.tipo_impuesto} se encuentra duplicado en: {system}, codigo afiliado: {value.codigo_afiliado}, atencion {value.nro_solben}, clinica: {value.ruc_proveedor}"
    
    @staticmethod
    def message_amounts_duplicate(message: str, value: object, system: str) -> str:
        return f"{message}: El monto {value.monto} se encuentra duplicado en: {system}, codigo afiliado: {value.codigo_afiliado}, tipo impuesto {value.tipo_impuesto}, clinica: {value.ruc_proveedor}"    