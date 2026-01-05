#Python
FROM python:3.12-slim

#Work folder of container
WORKDIR /WeatherApiDE

# System dependencies
RUN apt-get update && apt-get install -y \
    procps \
    curl \
    postgresql-client \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Spark
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3.tgz \
    | tar -xz -C /opt && \
    ln -s /opt/spark-4.0.1-bin-hadoop3 /opt/spark

# Download PostgreSQL JDBC driver
RUN curl -L \
  https://jdbc.postgresql.org/download/postgresql-42.6.0.jar \
  -o /opt/spark/jars/postgresql-42.6.0.jar

# Ensure permissions
RUN chmod 644 /opt/spark/jars/postgresql-42.6.0.jar

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=/WeatherApiDE/src

# Install dbt (choose your adapter)
RUN pip install --no-cache-dir dbt-postgres

#Copy dependencies
COPY python/requirements.txt python/requirements.txt
RUN pip install --no-cache-dir -r python/requirements.txt

#Copy and run entrypoint script
COPY python/init.sh python/init.sh
RUN chmod +x python/init.sh

#Copy source and data files
COPY src src
COPY data data
COPY dbt dbt

#Define entrypoint
ENTRYPOINT ["bash","/WeatherApiDE/python/init.sh"]
