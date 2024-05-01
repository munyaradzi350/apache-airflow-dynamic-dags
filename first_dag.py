from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import datetime as dt

def print_world():
    print('world')

default_args = {
    'owner': 'Munyah',
    'start_date': dt.datetime(2024, 4, 28),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG('airflow_tut_v02',
         default_args=default_args,
         schedule_interval='0 * * * *',
         ) as dag:
    print_hello = BashOperator(task_id='print_hello',
                               bash_command='echo "hello"')
    
    sleep = BashOperator(task_id='sleep',
                         bash_command='sleep 5')
    
    print_world = PythonOperator(task_id='print_world',
                                 python_callable=print_world)
    

    print_hello >> sleep >> print_world
