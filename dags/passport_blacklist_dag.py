import json
import io
import logging

import pandas as pd

from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.exceptions import AirflowException

MINIO_CONN_ID = "minio_config"
FILE_PATTERN = "*.xlsx"
BUCKET_NAME = "passport-blacklist"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 16),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def config_loader():
    """
    Этот метод это костыли, все конфиги можно задать через ui airflow
    """
    path = "../minio_con.json"
    try:
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)
            Variable.set(MINIO_CONN_ID, config, serialize_json=True)
            logging.info("MinIO config loaded successfully")
    except Exception as e:
        logging.error(f"Failed to load MinIO config: {str(e)}")
        raise AirflowException(f"Failed to load MinIO config: {str(e)}")


def process_file():
    """
    Функция для обработки файлов
    """
    try:
        minio_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        files = minio_hook.list_keys(bucket_name=BUCKET_NAME)

        if not files:
            logging.info(f"No files found in {BUCKET_NAME} bucket.")
            return

        xlsx_files =  [f for f in files if f.endswith(".xlsx")]
        for xlsx_file in xlsx_files:
            logging.info(f"Processing {xlsx_file}")
            obj = minio_hook.get_key(bucket_name=BUCKET_NAME, key=xlsx_file)
            file_bytes = obj.get()["Body"].read()

            df = pd.read_excel(io.BytesIO(file_bytes))

            for i, row in df.head().iterrows():
                logging.info(f"Row {i}: {dict(row)}")

    except Exception as e:
        raise AirflowException(f"File processing failed: {str(e)}")


with DAG(
    'minio_file_processing',
    default_args=default_args,
    schedule='@once',
    catchup=False,
    tags={'minio', 'processing'},
) as dag:
    # Сенсор для проверки новых файлов (ждет появления хотя бы одного файла)
    check_for_new_files = S3KeySensor(
        task_id='check_for_new_files',
        bucket_key=FILE_PATTERN,
        bucket_name=BUCKET_NAME,
        aws_conn_id=MINIO_CONN_ID,
        poke_interval=60,  # Проверка каждые 60 секунд
        timeout=3600,  # Таймаут 1 час
        mode='poke',
        wildcard_match=True,
    )

    process_new_file = PythonOperator(
        task_id='process_file',
        python_callable=process_file
    )

    check_for_new_files >> process_new_file