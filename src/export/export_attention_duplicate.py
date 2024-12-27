from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Validacion Facturas") \
    .config("spark.jars", "/home/markillat/Documentos/mysqls/mysql-connector-java-8.0.29.jar") \
    .getOrCreate()

df_solben_sabsa = spark.read.parquet("/home/markillat/Documentos/almacenamiento/attentions/unix_sabsa/solben_sabsa_attentions_2024.parquet")

# Cargar archivo Parquet y Unir los DataFrames
df_antiguas = spark.read.parquet("/home/markillat/Documentos/almacenamiento/attentions/unix_sabsa/*.parquet")

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
duplicados.write.mode('overwrite').csv("/home/markillat/Documentos/almacenamiento/duplicados.csv", header=True)

# Detener la sesión de Spark
spark.stop()
