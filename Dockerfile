# Airflow image with Java, PySpark and the PostgreSQL JDBC driver.

FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL -o /opt/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
    && chmod 644 /opt/postgresql.jar

USER airflow
RUN pip install --no-cache-dir pyspark==3.5.3 minio psycopg2-binary requests