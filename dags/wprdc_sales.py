from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="wprdc_sales",
    schedule="0 9 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    ingest = BashOperator(
        task_id="ingest",
        bash_command="python /opt/airflow/ingest.py",
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
         retries=0,
        bash_command=(
            "/home/airflow/dbt-venv/bin/dbt build "
            "--project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/dbt"
        ),
    )

    ingest >> dbt_build