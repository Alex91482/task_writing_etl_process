from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from util.minio_service import MinioService


MINIO_CONN_ID = "minio_config"
POSTGRES_CONN_ID = "postgres_config"
FILE_PATTERN = "*.xlsx"
BUCKET_NAME = "terminals"
BUCKET_ARCHIVE = "terminals-archive"
TARGET_TABLE = "bank.terminals"
HISTORY_TABLE = "bank.terminals_history"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 16),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def process_report_wrapper():
    sql = """
    
    """


with DAG(
    'report_processing',
    default_args=default_args,
    schedule='@once',
    catchup=False,
    tags={'report', 'processing'},
) as dag:


    start = PythonOperator(task_id='start', python_callable=lambda: print("Task created for execution report"))
    end = PythonOperator(task_id='end', python_callable=lambda: print("Execution completed report"))

    process_new_file = PythonOperator(
        task_id='process_file',
        python_callable=process_report_wrapper
    )

    start >> process_new_file >> end