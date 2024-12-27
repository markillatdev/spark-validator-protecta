#### Attention Duplicate ####

db_table_validacion_ordenes = """
(
    SELECT 
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
    WHERE fv.spark_process = true
    AND fv.estado_validacion_factura_id = 1 
    AND fv.tipo_validacion_factura_id = 1
    AND f.id_estado IN (9, 10)
) AS subquery"""

db_table_validacion_ordenes_with_ids = """
(
    SELECT 
    DISTINCT
    fv.id,
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
    WHERE fv.spark_process = true
    AND fv.estado_validacion_factura_id = 1 
    AND fv.tipo_validacion_factura_id = 1
    AND f.id_estado IN (9, 10)
) AS subquery"""

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
) AS subquery"""

db_table_liquidacion_ordenes = """
(
    SELECT
    CONCAT(cliente, '-' , cod_titula, '-' , categoria) AS codigo_afiliado,
    tot_clini AS monto,
    nro_soli AS nro_solben,
    ruc AS ruc_proveedor
    FROM liquidacion 
    WHERE YEAR(proceso) = 2024
) AS subquery
"""

#### Invoice Duplicate ####

db_table_liquidacion_facturas = """
(
    SELECT
    ruc as ruc_proveedor,
    nro_factu
    FROM liquidacion 
    WHERE YEAR(proceso) = 2024
) AS subquery
"""

db_table_validacion_facturas = """
(
    SELECT 
    rg.ruc_proveedor,
    rg.nro_factu 
    FROM factura_validaciones fv
    INNER JOIN factura f ON f.id = fv.factura_id
    INNER JOIN liqtempo l ON l.factura_id = f.id
    INNER JOIN reporte_general rg ON l.id = rg.id_liqtempo
    WHERE fv.spark_process = true
    AND fv.estado_validacion_factura_id = 1 
    AND fv.tipo_validacion_factura_id = 1
    AND f.id_estado IN (9, 10)
) AS subquery
"""

db_table_validacion_facturas_with_ids = """
(
    SELECT 
    DISTINCT
    fv.id, 
    fv.factura_id,
    rg.ruc_proveedor, 
    rg.nro_factu
    FROM factura_validaciones fv
    INNER JOIN factura f ON f.id = fv.factura_id
    INNER JOIN liqtempo l ON l.factura_id = f.id
    INNER JOIN reporte_general rg ON l.id = rg.id_liqtempo
    WHERE fv.spark_process = true
    AND fv.estado_validacion_factura_id = 1 
    AND fv.tipo_validacion_factura_id = 1
    AND f.id_estado IN (9, 10)
) AS subquery
"""

db_table_medden_facturas = """
(
    SELECT 
    rg.ruc_proveedor, 
    rg.nro_factu 
    FROM reporte_general rg
    INNER JOIN liqtempo l ON l.id = rg.id_liqtempo
    INNER JOIN liqtempo_solben ls ON ls.liqtempo_id = l.id
    WHERE l.id_estado IN (13, 15, 16, 17, 18)
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
        unix_cobertura = %s
    WHERE factura_id = %s
"""

update_factura_unique = """
    UPDATE factura_validaciones
    SET 
        observacion = %s,
        estado_validacion_factura_id = %s,
        tipo_validacion_factura_id = %s,
        silux_sabsa = %s,
        silux_cobertura = %s,
        unix_sabsa = %s,
        unix_cobertura = %s
    WHERE spark_process = true
    AND estado_validacion_factura_id = 1 
    AND tipo_validacion_factura_id = 1
"""

update_spark_processing = """
    UPDATE factura_validaciones fv
    SET fv.spark_process = true 
    WHERE fv.estado_validacion_factura_id = 1 
    AND fv.tipo_validacion_factura_id = 1
    AND fv.spark_process = false
"""

#####

db_table_liquidacion_ordenes_silux = """
(
    SELECT
    CONCAT(cliente, '-' , cod_titula, '-' , categoria) AS codigo_afiliado,
    tot_clini AS monto,
    nro_soli AS nro_solben,
    ruc AS ruc_proveedor
    FROM liquidacion 
    WHERE YEAR(proceso) = 2024
    AND origen IN (2, 4)
) AS subquery
"""

db_table_liquidacion_ordenes_silux_with_ids = """
(
    SELECT 
    DISTINCT
    fv.id,
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
    WHERE fv.spark_process = true
    AND fv.estado_validacion_factura_id = 1 
    AND fv.tipo_validacion_factura_id = 1
    AND f.id_estado IN (13, 15, 16, 17)
) AS subquery"""