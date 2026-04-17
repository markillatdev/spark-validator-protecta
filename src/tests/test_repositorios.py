import pytest
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv # type: ignore
load_dotenv()

@pytest.fixture(scope="module")
def spark():
    """Fixture para inicializar SparkSession."""
    return SparkSession.builder.master("local[*]").appName("TestApp").getOrCreate()

def test_count_records_in_parquet_files(spark):
    """Test para contar registros en archivos Parquet."""
    storage_path = os.getenv("STORAGE_PATH")
    
    # Validar que STORAGE_PATH está configurado
    assert storage_path, "La variable de entorno STORAGE_PATH no está configurada."
    storage_path = os.path.join(storage_path, "attentions", "solben_semefa")

    # Verificar que el directorio existe
    assert os.path.exists(storage_path), f"El directorio {storage_path} no existe."

    # Recorre cada archivo en el directorio
    parquet_files = [f for f in os.listdir(storage_path) if f.endswith(".parquet")]
    assert parquet_files, "No se encontraron archivos Parquet en el directorio."

    for file in parquet_files:
        path = os.path.join(storage_path, file)
        try:
            df = spark.read.parquet(path)
            record_count = df.count()
            print(f"File {file} has {record_count} records.")
            # Agrega una condición esperada para el test, por ejemplo:
            assert record_count >= 0, f"El archivo {file} tiene un conteo inválido."
        except Exception as e:
            pytest.fail(f"Error al procesar el archivo {file}: {e}")
