# Base image
FROM ubuntu:20.04

# Cambiar al usuario root para permisos adecuados
USER root

# Configurar las variables de entorno para evitar prompts interactivos
ENV DEBIAN_FRONTEND=noninteractive

# Instalar dependencias necesarias
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    software-properties-common openjdk-11-jdk wget tar python3 python3-pip procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configurar JAVA_HOME para que Spark lo encuentre
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Descargar e instalar Apache Spark
ENV SPARK_VERSION=3.5.3
RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz \
    && tar -xzf spark-$SPARK_VERSION-bin-hadoop3.tgz -C /opt \
    && ln -s /opt/spark-$SPARK_VERSION-bin-hadoop3 /opt/spark \
    && rm spark-$SPARK_VERSION-bin-hadoop3.tgz

# Descargar el conector JDBC de MySQL y moverlo al directorio de Spark
RUN wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.29.tar.gz -P /tmp/ && \
    tar -xvzf /tmp/mysql-connector-java-8.0.29.tar.gz -C /tmp/ && \
    mv /tmp/mysql-connector-java-8.0.29/mysql-connector-java-8.0.29.jar /opt/spark/jars/ && \
    rm -rf /tmp/*

# Configurar variables de entorno para Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV SPARK_LOCAL_DIRS=/opt/spark/work
ENV SPARK_CONF_DIR=/opt/spark/conf

# Configurar variables de entorno para Python
ENV PYSPARK_PYTHON=python3

# Instalar PySpark, PyMySQL, dotenv, Fastapi, uvicorn y pydantic
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install pyspark pymysql python-dotenv fastapi uvicorn pydantic python-jose

# Instalar utilidades de red
RUN apt update && apt install -y iputils-ping    

# Establecer el directorio de trabajo
WORKDIR /src

# Copiar toda la carpeta src al directorio de trabajo en el contenedor
COPY . /src

# Establecer la variable de entorno STORAGE_PATH
ENV STORAGE_PATH=/src/repositories

# Exponer los puertos necesarios
EXPOSE 8080 7077 4040

# Volver al usuario predeterminado para ejecutar Spark
#USER root

CMD ["uvicorn", "main:app", "--host=0.0.0.0", "--reload"]
