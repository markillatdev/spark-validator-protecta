from typing import List

#### Invoice Duplicate ####

def db_table_validacion_facturas_with_ids(invoiceIds: List[int]) -> str:
    ids = ', '.join(map(str, invoiceIds)) 
    query = f"""
            (
                SELECT 
                fv.factura_id,
                f.id_estado,
                l.codigo_iafa,
                fp.ruc_proveedor, 
                fp.nro_factura_std as nro_factu,
                l.codigo_afiliado,
                l.fecha AS fch_atencion,
                CASE
                    WHEN ls.code_solben IS NULL OR ls.code_solben = "" THEN ls.nro_autoriza
                    ELSE ls.code_solben
                END as nro_solben,
                f.monto
                FROM factura f
                INNER JOIN liqtempo l ON l.factura_id = f.id
                INNER JOIN liqtempo_solben ls ON ls.liqtempo_id = l.id
                INNER JOIN factura_validaciones fv ON fv.factura_id = f.id
                INNER JOIN factura_proveedor fp ON fp.factura_id = f.id
                WHERE fv.factura_id IN ({ids})
            ) AS subquery
            """
    return query


db_table_medden_facturas = """
(
    SELECT
    f.id AS factura_id,
    l.codigo_iafa,
    fp.ruc_proveedor, 
    fp.nro_factura_std as nro_factu,
    l.codigo_afiliado,
    l.fecha AS fch_atencion,
    CASE
        WHEN ls.code_solben IS NULL OR ls.code_solben = "" THEN ls.nro_autoriza
        ELSE ls.code_solben
    END as nro_solben,
    f.monto
    FROM factura f
    INNER JOIN liqtempo l ON l.factura_id = f.id
    INNER JOIN liqtempo_solben ls ON ls.liqtempo_id = l.id
    INNER JOIN factura_proveedor fp ON fp.factura_id = f.id
    WHERE f.id_estado IN (9, 10, 13, 15, 16, 17, 18)
    AND YEAR(f.created_at) BETWEEN YEAR(NOW()) - 1 AND YEAR(NOW())
) AS subquery
"""

#### Attention Duplicate ####

def db_table_validacion_ordenes_with_ids(invoiceIds: List[int]) -> str:
    ids = ', '.join(map(str, invoiceIds)) 
    query = f"""
            (
                SELECT 
                fv.factura_id,
                f.id_estado,
                l.codigo_iafa,
                fp.ruc_proveedor,
                l.codigo_afiliado,
                l.fecha AS fch_atencion,
                CASE
                    WHEN ls.code_solben IS NULL OR ls.code_solben = "" THEN ls.nro_autoriza
                    ELSE ls.code_solben
                END as nro_solben,
                f.monto
                FROM factura f 
                INNER JOIN factura_validaciones fv ON fv.factura_id = f.id
                INNER JOIN factura_proveedor fp ON fp.factura_id = f.id
                INNER JOIN liqtempo l ON l.factura_id = f.id
                INNER JOIN liqtempo_solben ls ON ls.liqtempo_id = l.id
                WHERE fv.factura_id IN ({ids})
            ) AS subquery"""
    return query


db_table_medden_ordenes = """
(
    SELECT
    f.id AS factura_id,
    l.codigo_iafa,
    fp.ruc_proveedor,
    l.codigo_afiliado,
    l.fecha AS fch_atencion,
    CASE
        WHEN ls.code_solben IS NULL OR ls.code_solben = "" THEN ls.nro_autoriza
        ELSE ls.code_solben
    END as nro_solben,
    f.monto AS monto
    FROM factura f
    INNER JOIN factura_proveedor fp ON fp.factura_id = f.id
    INNER JOIN liqtempo l ON l.factura_id = f.id
    INNER JOIN liqtempo_solben ls ON ls.liqtempo_id = l.id
    WHERE l.id_estado IN (9, 10, 13, 15, 16, 17, 18)
    AND YEAR(l.created_at) BETWEEN YEAR(NOW()) - 1 AND YEAR(NOW())
) AS subquery"""

#### TaxType Duplicate ####

def db_table_validacion_taxtypes_with_ids(invoiceIds: List[int]) -> str:
    ids = ', '.join(map(str, invoiceIds)) 
    query = f"""
            (
                SELECT 
                fv.factura_id,
                f.id_estado,
                l.codigo_iafa,
                fp.ruc_proveedor,
                l.codigo_afiliado,
                l.fecha AS fch_atencion,
                CASE
                    WHEN ls.code_solben IS NULL OR ls.code_solben = "" THEN ls.nro_autoriza
                    ELSE ls.code_solben
                END as nro_solben,
                l.tipo_impuesto_id as tipo_impuesto
                FROM factura f 
                INNER JOIN factura_validaciones fv ON fv.factura_id = f.id
                INNER JOIN factura_proveedor fp ON fp.factura_id = f.id
                INNER JOIN liqtempo l ON l.factura_id = f.id
                INNER JOIN liqtempo_solben ls ON ls.liqtempo_id = l.id
                WHERE fv.factura_id IN ({ids})
            ) AS subquery"""
    return query

db_table_medden_impuestos = """
(
    SELECT
    f.id AS factura_id,
    l.codigo_iafa,
    fp.ruc_proveedor,
    l.codigo_afiliado,
    l.fecha AS fch_atencion,
    CASE
        WHEN ls.code_solben IS NULL OR ls.code_solben = "" THEN ls.nro_autoriza
        ELSE ls.code_solben
    END as nro_solben,
    l.tipo_impuesto_id as tipo_impuesto
    FROM factura f
    INNER JOIN factura_proveedor fp ON fp.factura_id = f.id
    INNER JOIN liqtempo l ON l.factura_id = f.id
    INNER JOIN liqtempo_solben ls ON ls.liqtempo_id = l.id
    WHERE l.id_estado IN (9, 10, 13, 15, 16, 17, 18)
    AND YEAR(l.created_at) BETWEEN YEAR(NOW()) - 1 AND YEAR(NOW())
) AS subquery"""

#### Amount Duplicate ####

def db_table_validacion_amount_with_ids(invoiceIds: List[int]) -> str:
    ids = ', '.join(map(str, invoiceIds)) 
    query = f"""
            (
                SELECT 
                fv.factura_id,
                f.id_estado,
                l.codigo_iafa,
                fp.ruc_proveedor,
                l.codigo_afiliado,
                l.fecha AS fch_atencion,
                f.monto,
                l.tipo_impuesto_id as tipo_impuesto
                FROM factura f 
                INNER JOIN factura_validaciones fv ON fv.factura_id = f.id
                INNER JOIN factura_proveedor fp ON fp.factura_id = f.id
                INNER JOIN liqtempo l ON l.factura_id = f.id
                WHERE fv.factura_id IN ({ids})
            ) AS subquery"""
    return query

db_table_medden_montos = """
(
    SELECT
    f.id AS factura_id,
    l.codigo_iafa,
    fp.ruc_proveedor,
    l.codigo_afiliado,
    l.fecha AS fch_atencion,
    f.monto as monto,
    l.tipo_impuesto_id as tipo_impuesto
    FROM factura f
    INNER JOIN factura_proveedor fp ON fp.factura_id = f.id
    INNER JOIN liqtempo l ON l.factura_id = f.id
    WHERE l.id_estado IN (9, 10, 13, 15, 16, 17, 18)
    AND YEAR(l.created_at) BETWEEN YEAR(NOW()) - 1 AND YEAR(NOW())
) AS subquery"""

## Update

update_factura = """
    UPDATE factura_validaciones
    SET 
        observacion = %s,
        estado_validacion_factura_id = %s,
        tipo_validacion_factura_id = %s,
        silux_protecta = %s,
        solben_protecta = %s,
        updated_at = NOW()
    WHERE factura_id = %s
"""

def insert_factura_validacion_duplicados(data):
    query = """
    INSERT INTO factura_validacion_duplicados (
        factura_id, 
        factura_validacion_id,
        created_at,
        updated_at
    )
    SELECT %s, %s, NOW(), NOW()
    WHERE NOT EXISTS (
        SELECT 1
        FROM factura_validacion_duplicados
        WHERE factura_id = %s
          AND factura_validacion_id = %s
    )
    """
    values = [(fid, fvid, fid, fvid) for fid, fvid in data]
    return query, values


def update_factura_unique(invoiceIds: List[int]) -> str:
    placeholders = ', '.join(['%s'] * len(invoiceIds)) 
    query = f"""
        UPDATE factura_validaciones
        SET 
            observacion = %s,
            estado_validacion_factura_id = 2,
            tipo_validacion_factura_id = 2,
            silux_protecta = 0,
            solben_protecta = 0,
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
            silux_protecta = 0,
            solben_protecta = 0,
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


get_factura_validacion_id = """
    SELECT id FROM factura_validaciones WHERE factura_id = %s
"""
