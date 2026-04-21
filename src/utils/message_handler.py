class MessageHandler:

    @staticmethod
    def message_case_1(message: str, value: object, system: str) -> str:
        return (
            f"{message}: "
            f"Misma IAFAS {value.codigo_iafa}, IPRESS {value.ruc_proveedor}, Nro. factura {value.nro_factu}, paciente {value.codigo_afiliado}, "
            f"fecha de atención {value.fch_atencion}, Nro. orden {value.nro_solben}, importe {value.monto}. "
            f"Registrado en {system}, clínica {value.ruc_proveedor}"
        )

    @staticmethod
    def message_case_2(message: str, value: object, system: str) -> str:
        return (
            f"{message}: "
            f"Misma IAFAS {value.codigo_iafa}, IPRESS {value.ruc_proveedor}, paciente {value.codigo_afiliado}, fecha de atención {value.fch_atencion}, "
            f"Nro. orden {value.nro_solben}, importe {value.monto}. "
            f"Registrado en {system}, clínica {value.ruc_proveedor}"
        )

    @staticmethod
    def message_case_3(message: str, value: object, system: str) -> str:
        return (
            f"{message}: "
            f"Misma IAFAS {value.codigo_iafa}, IPRESS {value.ruc_proveedor}, paciente {value.codigo_afiliado}, fecha de atención {value.fch_atencion}, "
            f"Nro. orden {value.nro_solben}, mismo tipo de impuesto {value.tipo_impuesto}, "
            f"Registrado en {system}, clínica {value.ruc_proveedor}"
        )

    @staticmethod
    def message_case_4(message: str, value: object, system: str) -> str:
        return (
            f"{message}: "
            f"Misma IAFAS {value.codigo_iafa}, IPRESS {value.ruc_proveedor}, paciente {value.codigo_afiliado}, fecha de atención {value.fch_atencion}, "
            f"importe {value.monto}, mismo tipo de impuesto {value.tipo_impuesto}, "
            f"Registrado en {system}, clínica {value.ruc_proveedor}"
        )