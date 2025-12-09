FROM apache/airflow:3.1.3-python3.10

USER airflow
RUN pip install --no-cache-dir \
    dbt-core \
    dbt-duckdb \
    duckdb \
    pandas
