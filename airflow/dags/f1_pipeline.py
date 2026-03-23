from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'kavya',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='f1_data_pipeline',
    description='End-to-end F1 data pipeline: extract, transform, test',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@weekly',
    catchup=False,
    tags=['f1', 'dbt', 'etl'],
) as dag:

    extract_f1_data = BashOperator(
        task_id='extract_f1_data',
        bash_command='cd /opt/airflow/project && pip install -r extract/requirements.txt -q && python extract/load_f1_data.py',
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/project/f1_analytics && pip install dbt-postgres==1.7.13 -q && dbt run --profiles-dir .',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/project/f1_analytics && dbt test --profiles-dir .',
    )

    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command='cd /opt/airflow/project/f1_analytics && dbt snapshot --profiles-dir .',
    )

    extract_f1_data >> dbt_run >> dbt_test >> dbt_snapshot
