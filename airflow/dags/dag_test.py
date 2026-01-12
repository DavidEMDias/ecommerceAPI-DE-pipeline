from airflow.operators.python import PythonOperator
from airflow import DAG
import os, time, requests, logging
from datetime import datetime, timedelta
from src.data_utils import create_elt_schemas



DEFAULT_ARGS = {
    "owner": "data",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ELT_API_TO_BI",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    tags=["api", "postgres", "dbt", "bi"],
) as dag:
    
    create_objects = PythonOperator(
        task_id="create_schemas_and_tables",
        python_callable=create_elt_schemas
        )
