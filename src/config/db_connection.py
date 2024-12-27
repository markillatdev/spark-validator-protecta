import pymysql # type: ignore
from src.config.database import DATABASE_CONFIG
from pyspark.sql import DataFrame

def create_db_connection(system):
    config = DATABASE_CONFIG.get(system)
    connection = pymysql.connect(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config["password"],
        database=config["database"]
    )
    return connection
    
def read_table_from_db(spark, db_table, system) -> DataFrame:
    config = DATABASE_CONFIG.get(system)
    return spark.read.format("jdbc").options(
        url=config['url'],
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=db_table,
        user=config['user'],
        password=config['password']
    ).load()