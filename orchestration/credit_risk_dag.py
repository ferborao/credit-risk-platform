from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/home/fernando/credit-risk-platform"

default_args = {
    "owner": "fernando",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="credit_risk_pipeline",
    description="Pipeline completo de riesgo de crédito: Bronze → Silver → Gold",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["credit-risk", "data-engineering"],
) as dag:

    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"""
            cd {PROJECT_ROOT} &&
            source .venv/bin/activate &&
            python pipelines/bronze/ingest_freddie_mac.py
        """,
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command=f"""
            cd {PROJECT_ROOT} &&
            source .venv/bin/activate &&
            python pipelines/silver/transform_loans.py
        """,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
            cd {PROJECT_ROOT}/transform &&
            source {PROJECT_ROOT}/.venv/bin/activate &&
            dbt run
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
            cd {PROJECT_ROOT}/transform &&
            source {PROJECT_ROOT}/.venv/bin/activate &&
            dbt test
        """,
    )

    # Definimos el orden de ejecución
    bronze_ingestion >> silver_transform >> dbt_run >> dbt_test