FROM apache/spark:3.4.1

ADD https://jdbc.postgresql.org/download/postgresql-42.6.0.jar /opt/bitnami/spark/jars/

COPY src src

CMD ["bash", "-c", "tail -f /dev/null"]