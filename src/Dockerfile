# Usa la imagen base oficial de Spark con soporte para PySpark
FROM bitnami/spark:3.5.1

# Cambiar al usuario root para tener permisos adecuados
USER root

# Crear el directorio de apt y limpiar la caché
RUN apt-get update && apt-get install -y python3-pip curl wget && apt-get clean

# Instalar PySpark y otras dependencias de Python
RUN pip3 install py4j
RUN pip3 install pyspark

# Descargar el conector JDBC de MySQL y moverlo al directorio de Spark
RUN wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.30.tar.gz -P /tmp/ && \
    tar -xvzf /tmp/mysql-connector-java-8.0.30.tar.gz -C /tmp/ && \
    mv /tmp/mysql-connector-java-8.0.30/mysql-connector-java-8.0.30.jar /opt/bitnami/spark/jars/ && \
    rm -rf /tmp/*

# Establecer variables de entorno para Python
ENV PYSPARK_PYTHON=python3

# Establece el directorio de trabajo
WORKDIR /app

# Copiar scripts a la imagen
COPY spark/spark-docker/validate_invoice_duplicate_all.py /app/validate_invoice_duplicate.py
COPY spark/spark-docker/validate_attention_import_duplicate.py /app/validate_attention_import_duplicate.py

# Volver al usuario predeterminado de Spark para continuar con la ejecución de Spark
USER 1001
