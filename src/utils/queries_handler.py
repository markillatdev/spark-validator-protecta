#### Attention Duplicate ####

from typing import List

def db_table_validacion_ordenes_with_ids(invoiceIds: List[int]) -> str:
    ids = ', '.join(map(str, invoiceIds)) 
    query = f"""
            (
                SELECT 
                fv.factura_id,
                l.codigo_afiliado,
                f.monto,
                CASE
                    WHEN ls.code_solben IS NULL OR ls.code_solben = "" THEN ls.nro_autoriza
                    ELSE ls.code_solben
                END as nro_solben,
                rg.ruc_proveedor
                FROM factura_validaciones fv
                INNER JOIN factura f ON f.id = fv.factura_id
                INNER JOIN liqtempo l ON f.id = l.factura_id
                INNER JOIN liqtempo_solben ls ON ls.liqtempo_id = l.id
                INNER JOIN reporte_general rg ON l.id = rg.id_liqtempo
                WHERE fv.factura_id IN ({ids})
            ) AS subquery"""
    return query


db_table_medden_ordenes = """
(
    SELECT 
    l.codigo_afiliado,
    rg.mto_fact AS monto,
    CASE
        WHEN ls.code_solben IS NULL OR ls.code_solben = "" THEN ls.nro_autoriza
        ELSE ls.code_solben
    END as nro_solben,
    rg.ruc_proveedor
    FROM reporte_general rg
    INNER JOIN liqtempo l ON l.id = rg.id_liqtempo
    INNER JOIN liqtempo_solben ls ON ls.liqtempo_id = l.id
    WHERE l.id_estado IN (13, 15, 16, 17, 18)
    AND YEAR(l.created_at) = (SELECT YEAR(NOW()))
) AS subquery"""

#### Invoice Duplicate ####

def db_table_validacion_facturas_with_ids(invoiceIds: List[int]) -> str:
    ids = ', '.join(map(str, invoiceIds)) 
    query = f"""
            (
                SELECT 
                fv.factura_id,
                rg.ruc_proveedor, 
                rg.nro_factu
                FROM factura_validaciones fv
                INNER JOIN factura f ON f.id = fv.factura_id
                INNER JOIN liqtempo l ON l.factura_id = f.id
                INNER JOIN reporte_general rg ON l.id = rg.id_liqtempo
                WHERE fv.factura_id IN ({ids})
            ) AS subquery
            """
    return query


db_table_medden_facturas = """
(
    SELECT 
    rg.ruc_proveedor, 
    rg.nro_factu
    FROM reporte_general rg
    INNER JOIN liqtempo l ON l.id = rg.id_liqtempo
    INNER JOIN liqtempo_solben ls ON ls.liqtempo_id = l.id
    WHERE l.id_estado IN (13, 15, 16, 17, 18)
    AND YEAR(l.created_at) = (SELECT YEAR(NOW()))
) AS subquery
"""

## Update

update_factura = """
    UPDATE factura_validaciones
    SET 
        observacion = %s,
        estado_validacion_factura_id = %s,
        tipo_validacion_factura_id = %s,
        silux_sabsa = %s,
        silux_cobertura = %s,
        unix_sabsa = %s,
        unix_cobertura = %s,
        updated_at = NOW()
    WHERE factura_id = %s
"""


def update_factura_unique(invoiceIds: List[int]) -> str:
    placeholders = ', '.join(['%s'] * len(invoiceIds)) 
    query = f"""
        UPDATE factura_validaciones
        SET 
            observacion = %s,
            estado_validacion_factura_id = 2,
            tipo_validacion_factura_id = 2,
            silux_sabsa = 0,
            silux_cobertura = 0,
            unix_sabsa = 0,
            unix_cobertura = 0,
            updated_at = NOW()
        WHERE estado_validacion_factura_id = 1 
        AND tipo_validacion_factura_id = 1
        AND factura_id IN ({placeholders})
    """
    return query


def update_reset_invoices(invoiceIds: List[int]) -> str:
    placeholders = ', '.join(['%s'] * len(invoiceIds)) 
    query = f"""
        UPDATE factura_validaciones
        SET 
            observacion = %s,
            estado_validacion_factura_id = 1,
            tipo_validacion_factura_id = 1,
            silux_sabsa = 0,
            silux_cobertura = 0,
            unix_sabsa = 0,
            unix_cobertura = 0,
            spark_process = 0,
            updated_at = NOW()
        WHERE factura_id IN ({placeholders})
    """
    return query


def update_spark_processing(invoiceIds: List[int]) -> str:
    placeholders = ', '.join(['%s'] * len(invoiceIds)) 
    query = f"""
        UPDATE factura_validaciones fv
        SET 
            fv.spark_process = true,
            updated_at = NOW()
        WHERE fv.estado_validacion_factura_id = 1 
        AND fv.tipo_validacion_factura_id = 1
        AND fv.spark_process = false
        AND fv.factura_id IN ({placeholders})
    """
    return query
