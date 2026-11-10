FROM apache/airflow:2.11.2-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir pyspark "snowflake-connector-python[pandas]" faker
