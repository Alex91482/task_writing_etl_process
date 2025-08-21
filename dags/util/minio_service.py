import io
import logging

import pandas as pd

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException


class MinioService:
    """
    Класс предназначенный для работы с minio
    """

    def __init__(self, minio_conn_id: str, postgres_conn_id: str):
        self.minio_conn_id = minio_conn_id
        self.postgres_conn_id = postgres_conn_id

    def process_file_general_xlsx(self, bucket_name: str, bucket_name_archive: str):
        """
        Функция для обработки файлов
        """
        try:
            minio_hook = S3Hook(aws_conn_id=self.minio_conn_id)
            files = minio_hook.list_keys(bucket_name=bucket_name)

            if not files:
                logging.info(f"No files found in {bucket_name} bucket.")
                return

            xlsx_files = [f for f in files if f.endswith(".xlsx")]
            for xlsx_file in xlsx_files:
                logging.info(f"Processing {xlsx_file}")
                obj = minio_hook.get_key(bucket_name=bucket_name, key=xlsx_file)
                file_bytes = obj.get()["Body"].read()

                df = pd.read_excel(io.BytesIO(file_bytes))

                self.save_to_postgres(df=df)

                xlsx_file_name_archive = f"{xlsx_file}.archive"
                # Копируем файл в новый бакет с новым именем
                minio_hook.copy_object(
                    source_bucket_key=xlsx_file,
                    source_bucket_name=bucket_name,
                    dest_bucket_key=xlsx_file_name_archive,
                    dest_bucket_name=bucket_name_archive
                )

                logging.info("File copied successfully")

                # Удаляем исходный файл
                minio_hook.delete_objects(
                    bucket=bucket_name,
                    keys=[xlsx_file]
                )

                logging.info("Original file deleted")

        except Exception as e:
            raise AirflowException(f"File processing failed: {str(e)}")

    def save_to_postgres(self, df: pd.DataFrame):
        """
        Метод отвечающий за сохранение данных в postgres
        :param df: фрейм данных с паспортами
        """
        for i, row in df.head().iterrows():
            logging.info(f"Row {i}: {dict(row)}")

