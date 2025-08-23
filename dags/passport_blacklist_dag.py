from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.python import PythonOperator
from util.minio_service import MinioService


MINIO_CONN_ID = "minio_config"
POSTGRES_CONN_ID = "postgres_config"
FILE_PATTERN = "*.xlsx"
BUCKET_NAME = "passport-blacklist"
BUCKET_ARCHIVE = "passport-blacklist-archive"
TARGET_TABLE = "bank.passport_blacklist"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 16),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

default_schema = {
    "date": "string",
    "passport": "string"
}

minio_service = MinioService(
    minio_conn_id=MINIO_CONN_ID,
    postgres_conn_id=POSTGRES_CONN_ID
)


def process_file_wrapper() -> None:
    """
    Обертка функция для обработки файлов
    """
    # получили данные из inio
    df = minio_service.process_file_general_xlsx(
        bucket_name=BUCKET_NAME,
        bucket_name_archive=BUCKET_ARCHIVE,
        schema=default_schema
    )
    # преобразовали данные
    df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y', errors='coerce').dt.strftime('%Y-%m-%d')
    df['passport'] = df['passport'].str.replace(' ', '')
    df = df.rename(columns={
        "passport": "passport_num",
        "date": "received_dt"
    })
    # сохранение данных в базон
    minio_service.save_to_postgres(df=df, target_table=TARGET_TABLE)


with DAG(
    'passport_minio_file_processing',
    default_args=default_args,
    schedule='@once',
    catchup=False,
    tags={'minio', 'processing'},
) as dag:
    # Сенсор для проверки новых файлов (ждет появления хотя бы одного файла)
    check_for_new_files = S3KeySensor(
        task_id='check_for_new_files_passport',
        bucket_key=FILE_PATTERN,
        bucket_name=BUCKET_NAME,
        aws_conn_id=MINIO_CONN_ID,
        poke_interval=60,  # Проверка каждые 60 секунд
        timeout=3600,  # Таймаут 1 час
        mode='poke',
        wildcard_match=True,
    )

    start = PythonOperator(task_id='start', python_callable=lambda: print("Task created for execution passport blacklist"))
    end = PythonOperator(task_id='end', python_callable=lambda: print("Execution completed passport blacklist"))

    process_new_file = PythonOperator(
        task_id='process_file',
        python_callable=process_file_wrapper
    )

    start >> check_for_new_files >> process_new_file >> end