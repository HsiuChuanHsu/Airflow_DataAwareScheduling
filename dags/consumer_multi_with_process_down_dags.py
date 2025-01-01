
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta

db_name = 'test'
schema_name = 'test'

DATASET_CONFIGS = {
    'dataset1': Dataset(f"postgresql://localhost:5432/{db_name}/{schema_name}/dataset1"),
    'dataset2': Dataset(f"postgresql://localhost:5432/{db_name}/{schema_name}/dataset2"),
    'dataset3': Dataset(f"postgresql://localhost:5432/{db_name}/{schema_name}/dataset3"),
    'dataset4': Dataset(f"postgresql://localhost:5432/{db_name}/{schema_name}/dataset4")
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 1. Basic Python Task DAG
def print_hello():
    print("Hello from Airflow!")
    return "Hello"

def print_date():
    print(f"Current date is: {datetime.now()}")
    return "Date Printed"

with DAG(
    'kafka_tasks_multi_with_process_downstream_1',
    default_args=default_args,
    description='A simple DAG with Python tasks',
    schedule=[DATASET_CONFIGS['dataset1'], DATASET_CONFIGS['dataset4']],  # This DAG is triggered by updates to orders_dataset
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['kafka', 'multi-message', 'process']
) as dag1:

    start = DummyOperator(task_id='start')
    
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )
    
    date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
    )
    
    end = DummyOperator(task_id='end')
    
    start >> hello_task >> date_task >> end


with DAG(
    'kafka_tasks_multi_with_process_downstream_2',
    default_args=default_args,
    description='A simple DAG with Python tasks',
    schedule=[DATASET_CONFIGS['dataset2'], DATASET_CONFIGS['dataset3']],  # This DAG is triggered by updates to orders_dataset
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['kafka', 'multi-message', 'process']
) as dag1:

    start = DummyOperator(task_id='start')
    
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )
    
    date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
    )
    
    end = DummyOperator(task_id='end')
    
    start >> hello_task >> date_task >> end