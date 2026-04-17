from pyspark.sql import SparkSession
from config.db_connection import read_table_from_db
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_or_create_parquet(spark: SparkSession, db_table: str, filename: str, system: str):
    storage_path = os.getenv("STORAGE_PATH")
    if not storage_path:
        logger.error("STORAGE_PATH environment variable is not set.")
        raise ValueError("STORAGE_PATH environment variable is not set.")
    
    parquet_path = os.path.join(storage_path, filename)
    try:
        file_parquet = spark.read.parquet(parquet_path)
        logger.info(f"Parquet file loaded successfully from {parquet_path}")
    except Exception as e:
        logger.warning(f"Parquet file not found at {parquet_path}, loading data from the {system} database. Error: {str(e)}")
        try:
            file_parquet = read_table_from_db(spark, db_table, system)
            file_parquet.write.parquet(parquet_path)
            logger.info(f"Parquet file created successfully at {parquet_path}")
        except Exception as db_error:
            logger.error(f"Error reading from database for {system}: {str(db_error)}")
            raise Exception(f"Error de conexión a base de datos {system}: {str(db_error)}")