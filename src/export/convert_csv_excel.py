import pandas as pd
import glob
import os

# Directorio donde están los archivos CSV
csv_directory = "/home/markillat/Documentos/almacenamiento/duplicados_all.csv"

# Patrón para buscar los archivos CSV
csv_files = glob.glob(os.path.join(csv_directory, "*.csv"))

# Lista para almacenar los DataFrames
dataframes = []

# Leer cada archivo CSV y agregarlo a la lista
for csv_file in csv_files:
    print(f"Leyendo archivo: {csv_file}")
    df = pd.read_csv(csv_file)
    dataframes.append(df)

# Combinar todos los DataFrames en uno solo
if dataframes:
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Guardar el DataFrame combinado en un archivo Excel
    excel_file_path = "/home/markillat/Documentos/almacenamiento/duplicados_all.xlsx"
    combined_df.to_excel(excel_file_path, index=False, engine='openpyxl')
    print(f"Archivo Excel guardado en: {excel_file_path}")
else:
    print("No se encontraron archivos CSV para procesar.")
