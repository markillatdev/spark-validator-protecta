from src.config.database import DATABASE_CONFIG

def load_or_create_parquet(spark, db_table, filename, system):
    try:
        file_parquet = spark.read.parquet(f"/home/markillat/Documentos/almacenamiento/{filename}")
    except Exception as e:
        print(f"Archivos Parquet no encontrados, cargando datos desde la base de datos de {system}...")
        config = DATABASE_CONFIG.get(system)
        file_parquet = spark.read.format("jdbc").options(
            url=config['url'],
            driver="com.mysql.cj.jdbc.Driver",
            dbtable=db_table,
            user=config['user'],
            password=config['password']
        ).load()
        file_parquet.write.parquet(f"/home/markillat/Documentos/almacenamiento/{filename}")