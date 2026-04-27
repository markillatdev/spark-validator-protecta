from pyspark.sql import SparkSession

_spark_session = None

def get_spark_session():
    global _spark_session
    if _spark_session is None:
        try:
            _spark_session = SparkSession.builder \
                .appName("Validacion Facturas") \
                .config("spark.jars", '/opt/spark/jars/mysql-connector-java-8.0.29.jar') \
                .config("spark.executor.memory", "4g") \
                .config("spark.driver.memory", "4g") \
                .getOrCreate()
            print("SparkSession creada exitosamente.")
        except Exception as e:
            print("Error al crear SparkSession:", e)
            return None
    return _spark_session

def stop_spark_session():
    global _spark_session
    if _spark_session is not None:
        _spark_session.stop()
        _spark_session = None
        print("SparkSession detenida.")