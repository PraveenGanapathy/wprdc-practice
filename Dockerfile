FROM apache/airflow:3.3.2-python3.12

COPY requirements-dbt.txt /tmp/requirements-dbt.txt

RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install -r /tmp/requirements-dbt.txt
