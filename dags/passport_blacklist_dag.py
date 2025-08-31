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
BUCKET_NAME = "passport-blacklist"
BUCKET_ARCHIVE = "passport-blacklist-archive"
TARGET_TABLE = "bank.dwh_fact_passport_blacklist"
HISTORY_TABLE = "bank.dwh_fact_passport_blacklist_hist"

default_args = {
    'owner': 'airflow',
    'wait_for_downstream': True,
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
    # получили данные из minio
    data_dict = minio_service.process_file_general_xlsx(
        bucket_name=BUCKET_NAME,
        bucket_name_archive=BUCKET_ARCHIVE,
        schema=default_schema,
        file_format="xlsx"
    )
    if len(data_dict) == 0:
        return

    for key, df in data_dict.items():
        # преобразовали данные
        df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y', errors='coerce')

        # Находим максимальную дату
        max_date = df['date'].max()
        disabling_old_records(max_date=max_date)

        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        df['passport'] = df['passport'].str.replace(' ', '')
        df = df.rename(columns={
            "passport": "passport_num",
            "date": "received_dt"
        })
        # сохраняем метаданные
        prefix, file_date = minio_service.parse_filename_regex(filename=key)
        minio_service.save_to_postgres_metadata(
            file_create_dt=file_date,
            file_name=key,
            category_type=prefix
        )
        #
        process_passport_incremental(df_new=df, target_table=TARGET_TABLE, history_table=HISTORY_TABLE)


def disabling_old_records(max_date: datetime) -> None:
    # Извлекаем месяц и год
    current_start_month = max_date.replace(day=1)
    pg_hook = minio_service.get_postgres_hook()

    try:
        # SQL для очистки данных не из текущего месяца
        cleanup_sql = f"""
                INSERT INTO {HISTORY_TABLE} (passport_num, received_dt, valid_from, valid_to, operation_type, changed_at)
                SELECT passport_num as passport_num
                    , received_dt as received_dt
                    , effective_from as valid_from
                    , NOW() as valid_to
                    , 'DEL' as operation_type
                    , NOW() as changed_at
                FROM {TARGET_TABLE} 
                WHERE received_dt < '{current_start_month}' and deleted_flg = false;

                UPDATE {TARGET_TABLE} 
                SET effective_to = %s
                WHERE received_dt < '{current_start_month}' and deleted_flg = false;
                """

        pg_hook.run(sql=cleanup_sql, parameters=(datetime.now(),))

    except Exception as e:
        raise AirflowException(f"File processing failed: {str(e)}")


def process_passport_incremental(df_new, target_table: str, history_table: str) -> None:
    """
    Меод для сохранения данных в бд с учетом ведения исторических данных
    :param df_new: новая партия данных для загрузки в бд
    :param target_table: основная таблица с данными
    :param history_table: историческая таблица
    """
    pg_hook = minio_service.get_postgres_hook()
    conn = pg_hook.get_conn()

    try:
        with conn.cursor() as cursor:
            # Получаем текущее состояние из БД
            cursor.execute(f"select * from {target_table} where effective_to = '9999-12-31 23:59:59' and deleted_flg = false;")
            current_records = cursor.fetchall()
            current_columns = [desc[0] for desc in cursor.description]
            current_df = pd.DataFrame(current_records, columns=current_columns)

            # Преобразуем в множества для сравнения
            current_ids = set(current_df['passport_num'])
            new_ids = set(df_new['passport_num'])

            # Определяем изменения
            deleted_ids = current_ids - new_ids  # Удаленные паспорта
            new_passport_ids = new_ids - current_ids  # Новые паспорта
            existing_ids = current_ids & new_ids  # Существующие паспорта

            # Обрабатываем удаленные паспорта
            for passport_num in deleted_ids:
                passport_data = current_df[current_df['passport_num'] == passport_num].iloc[0]
                cursor.execute(f"""
                                INSERT INTO {history_table} 
                                (passport_num, received_dt, valid_to, operation_type)
                                VALUES 
                                (%s, %s, %s, 'DEL')
                            """, (
                    passport_data['passport_num'],
                    passport_data['received_dt'],
                    datetime.now()
                ))
                # Мягкое удаление из основной таблицы
                cursor.execute(f"""
                    UPDATE {target_table} 
                    SET effective_to = %s, deleted_flg = true
                    WHERE passport_num = %s AND effective_to = '9999-12-31 23:59:59
                """, (datetime.now(), passport_num))

            # Обрабатываем новые паспорта
            new_passport = df_new[df_new['passport_num'].isin(new_passport_ids)]
            for _, passport in new_passport.iterrows():
                cursor.execute(f"""
                                INSERT INTO {target_table} 
                                (passport_num, received_dt, effective_from)
                                VALUES 
                                (%s, %s, %s)
                            """, (
                    passport['passport_num'],
                    passport['received_dt'],
                    datetime.now()
                ))

                cursor.execute(f"""
                    INSERT INTO {history_table} 
                    (passport_num, received_dt, valid_to, operation_type)
                    VALUES 
                    (%s, %s, %s, 'INS')
                """, (
                    passport['passport_num'],
                    passport['received_dt'],
                    datetime.now()
                ))

            # Обрабатываем измененные паспорта
            for passport_num in existing_ids:
                current_data = current_df[current_df['passport_num'] == passport_num].iloc[0]
                new_data = df_new[df_new['passport_num'] == passport_num].iloc[0]

                # Проверяем, изменились ли данные
                if not current_data.equals(new_data):
                    # Закрываем предыдущую версию в истории
                    cursor.execute(f"""
                                    UPDATE {history_table} 
                                    SET valid_to = %s 
                                    WHERE passport_num = %s AND valid_to = '9999-12-31 23:59:59'
                                """, (datetime.now(), passport_num))

                    # Добавляем новую версию в историю
                    cursor.execute(f"""
                                        INSERT INTO {history_table} 
                                        (passport_num, received_dt, valid_to, operation_type)
                                        VALUES 
                                        (%s, %s, %s, 'UP')
                                    """, (
                        new_data['passport_num'],
                        new_data['received_dt'],
                        datetime.now()
                    ))

                    # Обновляем основную таблицу
                    cursor.execute(f"""
                                    UPDATE {target_table} 
                                    SET received_dt = %s
                                    WHERE passport_num = %s
                                """, (
                        new_data['received_dt'],
                        passport_num
                    ))

            conn.commit()
            print(f"Processed: {len(new_passport)} new, {len(existing_ids)} existing, {len(deleted_ids)} deleted")

    except Exception as e:
        conn.rollback()
        raise AirflowException(f"File processing failed: {str(e)}")
    finally:
        conn.close()


with DAG(
    'passport_minio_file_processing',
    default_args=default_args,
    schedule='30 6 * * *',
    max_active_runs=3,
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
        timeout=43200,  # Таймаут 12 часов
        mode='reschedule',
        soft_fail=True,
        wildcard_match=True,
    )

    start = PythonOperator(task_id='start', python_callable=lambda: print("Task created for execution passport blacklist"))
    end = PythonOperator(task_id='end', python_callable=lambda: print("Execution completed passport blacklist"))

    process_new_file = PythonOperator(
        task_id='process_file',
        python_callable=process_file_wrapper
    )

    start >> check_for_new_files >> process_new_file >> end