import pymysql
from config.database import DATABASE_CONFIG
from pyspark.sql import DataFrame, SparkSession
from pymysql.connections import Connection

def create_db_connection(system: str) -> Connection:
    config = DATABASE_CONFIG.get(system)
    if config is None:
        return None
    connection = pymysql.connect(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config["password"],
        database=config["database"]
    )
    return connection
    
def read_table_from_db(spark: SparkSession, db_table: str, system: str) -> DataFrame:
    config = DATABASE_CONFIG.get(system)
    if config is None:
        return None
    return spark.read.format("jdbc").options(
        url=config['url'],
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=db_table,
        user=config['user'],
        password=config['password']
    ).load()