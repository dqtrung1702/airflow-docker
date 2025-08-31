
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='my_1st_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=None,
    catchup=False,
)

task1 = BashOperator(
    task_id='Hello',
    bash_command='echo "Hello cô gái"',
    dag=dag,
)

def greet(name):
    print(f"greeting, {name} from DATA")
    return f"greeting sent to {name}"

task2 = PythonOperator(
    task_id="Greet",
    python_callable=greet,
    op_kwargs={'name':'Trung'},
    dag=dag,
)

task1 >> task2