import pytest
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
load_dotenv()

@pytest.fixture(scope="module")
def spark():
    """Fixture para inicializar SparkSession."""
    return SparkSession.builder.master("local[*]").appName("TestApp").getOrCreate()


def test_show_parquet_schema_and_sample(spark):
    """Test para visualizar la estructura y datos de archivos Parquet."""
    storage_path = os.getenv("STORAGE_PATH")
    assert storage_path, "La variable de entorno STORAGE_PATH no está configurada."

    base_path = storage_path
    subdirs = ["attentions", "invoices", "taxtypes", "amounts"]

    for subdir in subdirs:
        subdir_path = os.path.join(base_path, subdir)
        if not os.path.exists(subdir_path):
            print(f"\n=== {subdir.upper()} - Directorio no encontrado: {subdir_path} ===")
            continue

        systems = ["solben_protecta", "silux_protecta"]
        for system in systems:
            system_path = os.path.join(subdir_path, system)
            if not os.path.exists(system_path):
                continue

            print(f"\n{'='*60}")
            print(f"=== {subdir.upper()} / {system} ===")
            print(f"{'='*60}")

            parquet_files = [f for f in os.listdir(system_path) if f.endswith(".parquet")]
            if not parquet_files:
                print("No se encontraron archivos Parquet.")
                continue

            for file in parquet_files:
                path = os.path.join(system_path, file)
                try:
                    df = spark.read.parquet(path)
                    print(f"\n--- Archivo: {file} ---")
                    print(f"Registros: {df.count()}")
                    print(f"\nSchema:")
                    df.printSchema()
                    print(f"\nDatos de ejemplo (5 filas):")
                    df.show(5, truncate=False)
                except Exception as e:
                    print(f"Error al procesar {file}: {e}")
