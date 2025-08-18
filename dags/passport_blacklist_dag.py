from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.standard.operators.python import PythonOperator

from airflow.exceptions import AirflowException
import logging


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 16),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def process_file(file_key: str):
    """
    Функция для обработки файла
    :param file_key:
    :return:
    """
    try:
        minio_hook = S3Hook(conn_id=MINIO_CONN_ID)


        logging.info(f"Processing file: {file_key}")
        logging.info(f"File size: {stat.size} bytes")
        logging.info(f"Last modified: {stat.last_modified}")

    except Exception as e:
        logging.error(f"Error processing file {file_key}: {str(e)}")
        raise AirflowException(f"File processing failed: {str(e)}")


def get_latest_file(context):
    """
    Получает ключ последнего файла, вызвавшего срабатывание сенсора
    """
    ti = context['ti']
    s3_keys = ti.xcom_pull(key='s3_keys', task_ids='check_for_new_files')
    if s3_keys:
        return s3_keys[0]  # Берем первый подходящий файл
    return None


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