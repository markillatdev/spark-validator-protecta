from src.config.spark_config import create_spark_session
from src.utils.parquet_handler import load_or_create_parquet
from dotenv import load_dotenv
load_dotenv()

def load_resources(year, db_table, resource_type):
    systems = [
        {"system": "sabsa_solben", "filename": f"{resource_type}/unix_sabsa/solben_sabsa_{resource_type}_{year}.parquet"},
        {"system": "sabsa_cobertura", "filename": f"{resource_type}/unix_cobertura/solben_cobertura_{resource_type}_{year}.parquet"}
    ]
    spark = create_spark_session()
    for resource in systems:
        load_or_create_parquet(spark, db_table, resource['filename'], resource['system'])
                               
def load_attention(year, db_table_liquidacion_ordenes):
    load_resources(year, db_table_liquidacion_ordenes, "attentions")

def load_invoices(year, db_table_liquidacion_facturas):
    load_resources(year, db_table_liquidacion_facturas, "invoices")

def load_data():
    
    years_2009_2014 = [
        2009, 2010, 2012, 2013, 2014
    ]

    years_2015_2019 = [
        2015, 2016, 2017, 2018, 2019
    ]

    years_2020_2023 = [
        2020, 2021, 2022, 2023
    ]

    years_2024 = [
        2024
    ]

    years_range = [
        years_2009_2014,
        years_2015_2019,
        years_2020_2023,
        years_2024
    ]

    for years in years_range:
        years_str = ", ".join(str(year) for year in years)
        db_table_liquidacion_ordenes, db_table_liquidacion_facturas = querys(years_str)
        years_text = "_".join(str(year) for year in years)
        load_attention(years_text, db_table_liquidacion_ordenes)
        load_invoices(years_text, db_table_liquidacion_facturas)


def querys(years_str):
    db_table_liquidacion_ordenes = f"""
    (
        SELECT
        CONCAT(cliente, '-' , cod_titula, '-' , categoria) AS codigo_afiliado,
        tot_clini AS monto,
        nro_soli AS nro_solben,
        ruc AS ruc_proveedor
        FROM liquidacion 
        WHERE YEAR(proceso) IN ({years_str})
    ) AS subquery
    """
    db_table_liquidacion_facturas = f"""
    (
        SELECT
        ruc as ruc_proveedor,
        nro_factu
        FROM liquidacion 
        WHERE YEAR(proceso) IN ({years_str})
    ) AS subquery
    """
    return db_table_liquidacion_ordenes, db_table_liquidacion_facturas


if __name__ == "__main__":
    load_data()