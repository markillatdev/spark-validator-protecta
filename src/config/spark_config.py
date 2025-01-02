from pyspark.sql import SparkSession

# Crear la sesión de Spark, incluyendo el JAR del driver JDBC
def create_spark_session():
    try:
        spark = SparkSession.builder \
            .appName("Validacion Facturas") \
            .config("spark.jars", '/opt/spark/jars/mysql-connector-java-8.0.29.jar') \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()
        print("SparkSession creada exitosamente.")
        return spark
    except Exception as e:
        print("Error al crear SparkSession:", e)
        return None