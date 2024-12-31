from src.config.db_connection import read_table_from_db
import os

def load_or_create_parquet(spark, db_table, filename, system):
    storage_path = os.getenv("STORAGE_PATH")
    parquet_path = os.path.join(storage_path, filename)
    try:
        file_parquet = spark.read.parquet(parquet_path)
    except Exception as e:
        print(f"Archivos Parquet no encontrados, cargando datos desde la base de datos de {system}...")
        file_parquet = read_table_from_db(spark, db_table, system)
        file_parquet.write.parquet(parquet_path)