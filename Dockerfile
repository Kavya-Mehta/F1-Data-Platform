FROM apache/airflow:2.8.1-python3.10

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    && apt-get clean

USER airflow

RUN pip install --no-cache-dir \
    fastf1==3.4.4 \
    psycopg2-binary==2.9.9 \
    pandas==2.1.4 \
    python-dotenv==1.0.0 \
    "dbt-postgres==1.5.9" \
    "dbt-core==1.5.9" \
    great-expectations==0.18.8 \
    sqlalchemy