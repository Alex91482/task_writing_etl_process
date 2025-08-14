from datetime import datetime

from airflow.models import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 9, 12, 0, 0)
}


def hello_world():
    print('hello world')


with DAG(
    dag_id='hello_world',
    default_args=default_args,
    schedule='@once'
) as dag:

    test_python = PythonOperator(task_id='test_python', python_callable=hello_world)
    test_bash = BashOperator(task_id='test_bash', bash_command='echo Hello World!')


test_python >> test_bash