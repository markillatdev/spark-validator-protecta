from config.spark_config import get_spark_session
import os

# Obtener la sesión de Spark singleton
spark = get_spark_session()

df_solben_sabsa = spark.read.parquet(f'{os.getenv("STORAGE_PATH")}/attentions/solben_protecta/solben_sabsa_attentions_2024.parquet')

# Cargar archivo Parquet y Unir los DataFrames
df_antiguas = spark.read.parquet(f'{os.getenv("STORAGE_PATH")}/attentions/solben_protecta/*.parquet')

# Unir los DataFrames
df_facturas_completas = df_antiguas.union(df_solben_sabsa)

# Filtrar solo los registros presentes en df_solben_sabsa
filtered_df = df_facturas_completas.join(
    df_solben_sabsa.select("codigo_afiliado", "monto", "nro_solben", "ruc_proveedor").distinct(),
    on=["codigo_afiliado", "monto", "nro_solben", "ruc_proveedor"],
    how="inner"
)

# Buscar duplicados
duplicados = filtered_df.groupBy("codigo_afiliado", "monto", "nro_solben", "ruc_proveedor") \
    .count() \
    .filter("count > 1")

# Mostrar duplicados
duplicados.show(truncate=False)

# Guardar los duplicados en un archivo CSV, sobrescribiendo si ya existe
duplicados.write.mode('overwrite').csv(f'{os.getenv("STORAGE_PATH")}/duplicados.csv', header=True)
