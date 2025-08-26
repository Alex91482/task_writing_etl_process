from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
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

default_schema = {
    "terminal_id": "string",
    "terminal_type": "string",
    "terminal_city": "string",
    "terminal_address": "string"
}

minio_service = MinioService(
    minio_conn_id=MINIO_CONN_ID,
    postgres_conn_id=POSTGRES_CONN_ID
)


def process_file_wrapper() -> None:
    """
    Обертка функция для обработки файлов
    """
    data_dict = minio_service.process_file_general_xlsx(
        bucket_name=BUCKET_NAME,
        bucket_name_archive=BUCKET_ARCHIVE,
        schema=default_schema,
        file_format="xlsx"
    )

    if len(data_dict) == 0:
        return

    for key, df in data_dict.items():
        prefix, file_date = minio_service.parse_filename_regex(filename=key)
        # сохраняем в базон
        process_terminals_incremental(
            df_new=df,
            target_table=TARGET_TABLE,
            history_table=HISTORY_TABLE,
            date_receipt_values=file_date
        )
        # сохраняем метаданные
        minio_service.save_to_postgres_metadata(
            file_create_dt=file_date,
            file_name=key,
            category_type=prefix
        )


def process_terminals_incremental(df_new, target_table: str, history_table: str, date_receipt_values: str) -> None:
    """
    Меод для сохранения данных в бд с учетом ведения исторических данных
    :param df_new: новая партия данных для загрузки в бд
    :param target_table: основная таблица с данными
    :param history_table: историческая таблица
    :param date_receipt_values: дата составления списка данных
    """
    pg_hook = minio_service.get_postgres_hook()
    conn = pg_hook.get_conn()

    try:
        with conn.cursor() as cursor:
            # Получаем текущее состояние из БД
            cursor.execute(f"SELECT * FROM {target_table}")
            current_records = cursor.fetchall()
            current_columns = [desc[0] for desc in cursor.description]
            current_df = pd.DataFrame(current_records, columns=current_columns)

            # Преобразуем в множества для сравнения
            current_ids = set(current_df['terminal_id'])
            new_ids = set(df_new['terminal_id'])

            # Определяем изменения
            deleted_ids = current_ids - new_ids  # Удаленные терминалы
            new_terminal_ids = new_ids - current_ids  # Новые терминалы
            existing_ids = current_ids & new_ids  # Существующие терминалы

            # Обрабатываем удаленные терминалы
            for terminal_id in deleted_ids:
                terminal_data = current_df[current_df['terminal_id'] == terminal_id].iloc[0]
                cursor.execute(f"""
                                INSERT INTO {history_table} 
                                (terminal_id, terminal_type, terminal_city, terminal_address, valid_to, operation_type, date_receipt_values)
                                VALUES 
                                (%s, %s, %s, %s, %s, 'DEL', %s)
                            """, (
                    terminal_data['terminal_id'],
                    terminal_data['terminal_type'],
                    terminal_data['terminal_city'],
                    terminal_data['terminal_address'],
                    date_receipt_values,
                    date_receipt_values
                ))
                # Удаляем из основной таблицы
                cursor.execute(f"DELETE FROM {target_table} WHERE terminal_id = %s", (terminal_id,))

            # Обрабатываем новые терминалы
            new_terminals = df_new[df_new['terminal_id'].isin(new_terminal_ids)]
            for _, terminal in new_terminals.iterrows():
                cursor.execute(f"""
                                INSERT INTO {target_table} 
                                (terminal_id, terminal_type, terminal_city, terminal_address, create_dt, update_dt, date_receipt_values)
                                VALUES 
                                (%s, %s, %s, %s, %s, %s, %s)
                            """, (
                    terminal['terminal_id'],
                    terminal['terminal_type'],
                    terminal['terminal_city'],
                    terminal['terminal_address'],
                    datetime.now(),
                    datetime.now(),
                    date_receipt_values
                ))

                cursor.execute(f"""
                                INSERT INTO {history_table} 
                                (terminal_id, terminal_type, terminal_city, terminal_address, valid_from, operation_type, date_receipt_values)
                                VALUES 
                                (%s, %s, %s, %s, %s, 'INS', %s)
                            """, (
                    terminal['terminal_id'],
                    terminal['terminal_type'],
                    terminal['terminal_city'],
                    terminal['terminal_address'],
                    date_receipt_values,
                    date_receipt_values
                ))

            # 6. Обрабатываем измененные терминалы
            for terminal_id in existing_ids:
                # TODO: перепроверить эту хренову проверку
                current_data = current_df[current_df['terminal_id'] == terminal_id].iloc[0]
                new_data = df_new[df_new['terminal_id'] == terminal_id].iloc[0]

                # Проверяем, изменились ли данные
                if not current_data.equals(new_data):
                    # Закрываем предыдущую версию в истории
                    cursor.execute(f"""
                                    UPDATE {history_table} 
                                    SET valid_to = %s 
                                    WHERE terminal_id = %s AND valid_to = '9999-12-31 23:59:59'
                                """, (datetime.now(), terminal_id))

                    # Добавляем новую версию в историю
                    cursor.execute(f"""
                                    INSERT INTO {history_table} 
                                    (terminal_id, terminal_type, terminal_city, terminal_address, operation_type, valid_from, date_receipt_values)
                                    VALUES 
                                    (%s, %s, %s, %s, 'UP', %s, %s)
                                """, (
                        new_data['terminal_id'],
                        new_data['terminal_type'],
                        new_data['terminal_city'],
                        new_data['terminal_address'],
                        current_data['date_receipt_values'],
                        date_receipt_values
                    ))

                    # Обновляем основную таблицу
                    cursor.execute(f"""
                                    UPDATE {target_table} 
                                    SET terminal_type = %s, terminal_city = %s, terminal_address = %s, update_dt = %s
                                    WHERE terminal_id = %s
                                """, (
                        new_data['terminal_type'],
                        new_data['terminal_city'],
                        new_data['terminal_address'],
                        datetime.now(),
                        terminal_id
                    ))

            conn.commit()
            print(f"Processed: {len(new_terminals)} new, {len(existing_ids)} existing, {len(deleted_ids)} deleted")

    except Exception as e:
        conn.rollback()
        raise AirflowException(f"File processing failed: {str(e)}")
    finally:
        conn.close()


with DAG(
    'terminal_minio_file_processing',
    default_args=default_args,
    schedule='@once',
    catchup=False,
    tags={'minio', 'processing'},
) as dag:
    # Сенсор для проверки новых файлов (ждет появления хотя бы одного файла)
    check_for_new_files = S3KeySensor(
        task_id='check_for_new_files_terminal',
        bucket_key=FILE_PATTERN,
        bucket_name=BUCKET_NAME,
        aws_conn_id=MINIO_CONN_ID,
        poke_interval=60,  # Проверка каждые 60 секунд
        timeout=3600,  # Таймаут 1 час
        mode='poke',
        wildcard_match=True,
    )

    start = PythonOperator(task_id='start', python_callable=lambda: print("Task created for execution terminals"))
    end = PythonOperator(task_id='end', python_callable=lambda: print("Execution completed terminals"))

    process_new_file = PythonOperator(
        task_id='process_file',
        python_callable=process_file_wrapper
    )

    start >> check_for_new_files >> process_new_file >> end