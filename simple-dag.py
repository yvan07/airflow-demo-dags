from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),  # Réduit à 1 minute vu la fréquence d'exécution
}

def print_hello():
    print("Hello from Airflow!")

dag = DAG(
    'simple_hello_world',
    default_args=default_args,
    description='A simple Airflow DAG running every 5 minutes',
    schedule_interval='*/5 * * * *',  # Expression cron pour toutes les 5 minutes
    catchup=False
)

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)
