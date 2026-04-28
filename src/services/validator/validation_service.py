from pyspark.sql import SparkSession
from pymysql.connections import Connection
from typing import List, Dict
from services.orchestrator.validation_orchestrator import ValidationOrchestrator

class ValidationService:

    def __init__(self, spark: SparkSession, connection: Connection, coreSystem: str):
        self.spark = spark
        self.connection = connection
        self.coreSystem = coreSystem
        self.orchestrator = ValidationOrchestrator(spark, connection, coreSystem)

    def execute(self, systems_validate: List[Dict], invoiceIds: List[int]):
        self.orchestrator.execute_all_validations(systems_validate, invoiceIds)