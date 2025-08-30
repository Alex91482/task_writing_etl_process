import logging
from datetime import date, datetime, timedelta
import calendar
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from util.minio_service import MinioService


MINIO_CONN_ID = "minio_config"
POSTGRES_CONN_ID = "postgres_config"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 16),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

minio_service = MinioService(
    minio_conn_id=MINIO_CONN_ID,
    postgres_conn_id=POSTGRES_CONN_ID
)


def create_report() -> None:
    hook = minio_service.get_postgres_hook()

    sql1 = """
            select max(br.report_period)
            from bank.rep_fraud br
            ;
        """
    max_report_date = hook.get_first(sql=sql1)
    print(f'max_report_date: {str(max_report_date)}')
    if None in max_report_date:
        max_report_date = (date.min,)

    sql2 = f"""
        select distinct t.file_create_dt
        from (
            select bm.file_create_dt as file_create_dt,
                count(*) as count_files
            from bank.meta_data_file bm
            where bm.file_create_dt > %s
            group by bm.file_create_dt
        ) t
        where t.count_files >= 3
        ;
    """
    result_date = hook.get_records(sql=sql2, parameters=(max_report_date[0],))
    print(f'result date: {str(result_date)}')

    sql3 = f"""
            insert into bank.rep_fraud (event_dt, passport, fio, phone, event_type, report_dt, report_period)
            with 
                actual_terminal as (
                    select t.terminal_id as terminal_id
                        , t.terminal_type as terminal_type
                        , t.terminal_city as terminal_city
                    from bank.terminals_history t 
                    where t.date_receipt_values = %s
                        and t.operation_type != 'DEL'
                ),
                actual_transaction as (
                    select *
                    from bank.transactions t 
                    where date(t.trans_date) = %s
                ),
                actual_blacklist_passport as (
                    select *
                    from bank.passport_blacklist p 
                    where p.received_dt between %s and %s
                ),
                expired_or_blocked_passport as (
                    select 
                        t.trans_date as event_dt,
                        c.passport_num as passport,
                        concat(c.last_name, ' ', c.first_name, ' ', coalesce(c.patronymic, '')) as fio,
                        c.phone,
                        'Просроченный/заблокированный паспорт' as event_type,
                        CURRENT_DATE as report_dt
                    from actual_transaction t
                        inner join bank.cards cd on t.card_num = cd.card_num
                        inner join  bank.accounts a on cd.account = a.account
                        inner join  bank.clients c on a.client = c.client_id
                        left join actual_blacklist_passport bp on c.passport_num = bp.passport_num
                    where t.oper_result = 'SUCCESS' 
                        and (c.passport_valid_to < t.trans_date 
                            or 
                            (bp.id is not null and bp.deleted_flg = false and bp.effective_from <= t.trans_date and bp.effective_to > t.trans_date))
                ),
                invalid_contract as (
                    select 
                        t.trans_date as event_dt,
                        c.passport_num as passport,
                        concat(c.last_name, ' ', c.first_name, ' ', coalesce(c.patronymic, '')) as fio,
                        c.phone,
                        'Недействующий договор' as event_type,
                        CURRENT_DATE as report_dt
                    from actual_transaction t
                        inner join bank.cards cd on t.card_num = cd.card_num
                        inner join bank.accounts a on cd.account = a.account
                        inner join bank.clients c on a.client = c.client_id
                    where t.oper_result = 'SUCCESS' and  a.valid_to < t.trans_date
                ),
                operations_in_different_cities as (
                    select t.trans_date as event_dt,
                        c.passport_num as passport,
                        concat(c.last_name, ' ', c.first_name, ' ', coalesce(c.patronymic, '')) as fio,
                        c.phone,
                        'Операции в разных городах' as event_type,
                        CURRENT_DATE as report_dt
                    from (
                        select t.card_num, 
                            th.terminal_city, 
                            t.trans_date,
                            LAG(th.terminal_city) over (partition by t.card_num order by t.trans_date) as prev_city,
                            LAG(t.trans_date) over (partition by t.card_num  order by t.trans_date) as prev_date
                        from actual_transaction t
                            inner join actual_terminal th on t.terminal = th.terminal_id
                    ) t
                        inner join bank.cards cd on t.card_num = cd.card_num
                        inner join bank.accounts a on cd.account = a.account
                        inner join bank.clients c on a.client = c.client_id
                    where t.prev_city is not null
                        and t.terminal_city <> t.prev_city
                        and t.trans_date <= t.prev_date + INTERVAL '1 hour'
                ),
                selecting_the_amount as (
                    select t.trans_date as event_dt,
                        c.passport_num as passport,
                        concat(c.last_name, ' ', c.first_name, ' ', coalesce(c.patronymic, '')) as fio,
                        c.phone,
                        'Подбор суммы' as event_type,
                        CURRENT_DATE as report_dt
                    from (
                        select at.trans_date,
                            at.card_num,
                            at.amt,
                            at.oper_result,
                            LAG(at.amt) over (partition by at.card_num order by at.trans_date) as prev_amt,
                            LAG(at.oper_result) over (partition by at.card_num order by at.trans_date) as prev_result,
                            LAG(at.trans_date) over (partition by at.card_num order by at.trans_date) as prev_date,
                            LAG(at.amt, 2) over (partition by at.card_num order by at.trans_date) as prev_amt2,
                            LAG(at.oper_result, 2) over (partition by at.card_num order by at.trans_date) as prev_result2,
                            LAG(at.trans_date, 2) over (partition by at.card_num order by at.trans_date) as prev_date2
                        from actual_transaction at
                        where oper_result in ('SUCCESS', 'REJECT')
                    ) t
                        inner join bank.cards cd on t.card_num = cd.card_num
                        inner join bank.accounts a on cd.account = a.account
                        inner join bank.clients c on a.client = c.client_id
                    where oper_result = 'SUCCESS'
                        and prev_result = 'REJECT'
                        and prev_result2 = 'REJECT'
                        and prev_amt > amt
                        and prev_amt2 > prev_amt
                        and trans_date <= prev_date2 + INTERVAL '20 minutes'
                ),
                fraud_cases AS (
                    (select * from expired_or_blocked_passport)
                    union all
                    (select * from invalid_contract)
                    union all 
                    (select * from operations_in_different_cities)
                    union all
                    (select * from selecting_the_amount)
                )
                select distinct on (passport, event_dt, event_type)
                    event_dt,
                    passport,
                    fio,
                    phone,
                    event_type,
                    report_dt,
                    %s
                from fraud_cases
                order by passport, event_dt, event_type
                ;
        """

    for date_tuple in result_date:
        input_date = date_tuple[0]
        #input_date = datetime.strptime(actual_date, '%Y-%m-%d').date()
        # Дата начала месяца (для паспортов)
        beginning_month = input_date.replace(day=1)
        # Дата конца месяца (для паспортов)
        last_day = calendar.monthrange(input_date.year, input_date.month)[1]
        end_month = input_date.replace(day=last_day)

        hook.run(sql=sql3, parameters=(input_date, input_date, beginning_month, end_month, input_date))

    logging.info("Create report success")


with DAG(
    'report_processing',
    default_args=default_args,
    schedule='0 7 * * *',
    max_active_runs=3,
    catchup=False,
    tags={'report', 'processing'},
) as dag:

    start = PythonOperator(task_id='start', python_callable=lambda: print("Task created for execution report"))
    end = PythonOperator(task_id='end', python_callable=lambda: print("Execution completed report"))

    process_report = PythonOperator(
        task_id='process_report',
        python_callable=create_report
    )

    start >> process_report >> end