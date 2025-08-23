import io
import logging

import pandas as pd

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException


class MinioService:
    """
    Класс предназначенный для работы с minio
    """

    __DAMAGED_BUCKET = "damaged"

    def __init__(self, minio_conn_id: str, postgres_conn_id: str):
        self.minio_conn_id = minio_conn_id
        self.postgres_conn_id = postgres_conn_id

    def process_file_general_xlsx(self, bucket_name: str, bucket_name_archive: str, schema: dict) -> pd.DataFrame:
        """
        Функция для обработки файлов
        """
        try:
            minio_hook = S3Hook(aws_conn_id=self.minio_conn_id)
            files = minio_hook.list_keys(bucket_name=bucket_name)

            # добавить логику на случай отсутствия схемы
            # Создаем пустой DataFrame по схеме
            empty_data = {col: [] for col in schema.keys()}
            df_reference = pd.DataFrame.from_dict(empty_data, orient='columns')
            # Применяем типы данных
            df_reference = df_reference.astype(schema)

            if not files:
                logging.info(f"No files found in {bucket_name} bucket.")
                return df_reference

            result = df_reference
            file_to_archive = []

            xlsx_files = [f for f in files if f.endswith(".xlsx")]
            for xlsx_file in xlsx_files:
                logging.info(f"Processing {xlsx_file}")
                obj = minio_hook.get_key(bucket_name=bucket_name, key=xlsx_file)
                file_bytes = obj.get()["Body"].read()

                df = pd.read_excel(io.BytesIO(file_bytes))

                if set(df.columns) != set(df_reference.columns):
                    logging.error("Несоответствие схем")
                    self.copy_and_delete(
                        minio_hook=minio_hook,
                        file_name=xlsx_file,
                        file_name_new=xlsx_file,
                        bucket_name=bucket_name,
                        bucket_carrying=self.__DAMAGED_BUCKET
                    )

                else:
                    result = pd.concat([result, df])
                    file_to_archive.append(xlsx_file)

            for archive_file in file_to_archive:
                xlsx_file_name_archive = f"{archive_file}.archive"
                self.copy_and_delete(
                    minio_hook=minio_hook,
                    file_name=archive_file,
                    file_name_new=xlsx_file_name_archive,
                    bucket_name=bucket_name,
                    bucket_carrying=bucket_name_archive
                )

            return result

        except Exception as e:
            raise AirflowException(f"File processing failed: {str(e)}")

    def copy_and_delete(self, minio_hook: S3Hook, file_name: str, file_name_new: str, bucket_name: str, bucket_carrying: str) -> None:
        """
        :param minio_hook: хук для минио
        :param file_name: название файла который нам прислали
        :param file_name_new: новое имя файла
        :param bucket_name: ключ к бакету где хранится файл который прислали
        :param bucket_carrying: ключ к бакету в который нужно переместить файл
        """
        minio_hook.copy_object(
            source_bucket_key=file_name,
            source_bucket_name=bucket_name,
            dest_bucket_key=file_name_new,
            dest_bucket_name=bucket_carrying
        )
        logging.info("File copied successfully")
        # Удаляем исходный файл
        minio_hook.delete_objects(
            bucket=bucket_name,
            keys=[file_name]
        )
        logging.info("Original file deleted")

    def save_to_postgres(self, df: pd.DataFrame, target_table: str):
        """
        Метод отвечающий за сохранение данных в postgres
        :param df: фрейм данных с паспортами
        :param target_table: таблица в которую требуется сохранить данные
        """
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        try:
            # Сохраняем DataFrame в базу
            pg_hook.insert_rows(
                table=target_table,
                rows=df.values.tolist(),
                target_fields=df.columns.tolist(),
                replace=False
            )
            
            logging.info(f"Successfully saved {len(df)} rows to DB")

        except Exception as e:
            logging.error(f"Error saving to PostgreSQL: {str(e)}")
            raise AirflowException(f"File processing failed: {str(e)}")
