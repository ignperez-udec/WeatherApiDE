FROM apache/spark:4.0.1

# Set Spark home explicitly
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Install curl (image is slim)
USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Download PostgreSQL JDBC driver
RUN curl -L \
  https://jdbc.postgresql.org/download/postgresql-42.6.0.jar \
  -o /opt/spark/jars/postgresql-42.6.0.jar

# Ensure permissions
RUN chmod 644 /opt/spark/jars/postgresql-42.6.0.jar

# Set workdir
WORKDIR /WeatherApiDE

# Copy project code
COPY src src

ENV PYTHONPATH=/WeatherApiDE/src