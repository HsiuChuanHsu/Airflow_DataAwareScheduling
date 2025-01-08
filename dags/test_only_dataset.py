from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
# from confluent_kafka import Consumer, KafkaException, KafkaError

import json
from datetime import datetime, timedelta
import os
import logging
from typing import Set, List

from load_operator import LoadOperator
from kafka_tools import ConsumerTool, KafkaError, KafkaException


# Define datasets for different types
# dataset1_dataset = Dataset("file:///opt/airflow/files/dataset1_data.json")
# dataset2_dataset = Dataset("file:///opt/airflow/files/dataset2_data.json")
DATASET_CONFIGS = {
    'dataset3': Dataset("file:///opt/airflow/files/dataset3_data.json"),
    'dataset4': Dataset("file:///opt/airflow/files/dataset4_data.json")
}

EMAIL_POOL = [
    "alert1@example.com",
    "alert2@example.com",
    "alert3@example.com",
    "monitoring@example.com",
    
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': "notification@example.com",  # 使用隨機選擇的 email
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
    'kafka_tasks_multi_with_process_downstream_3',
    default_args=default_args,
    description='A simple DAG with Python tasks',
    schedule=[DATASET_CONFIGS['dataset3']],  # This DAG is triggered by updates to orders_dataset
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
    'kafka_tasks_multi_with_process_downstream_4',
    default_args=default_args,
    description='A simple DAG with Python tasks',
    schedule=[DATASET_CONFIGS['dataset4']],  # This DAG is triggered by updates to orders_dataset
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['kafka', 'multi-message', 'process']
) as dag1:

    start = DummyOperator(task_id='start')
    
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
        # outlets=[DATASET_CONFIGS['dataset4']]
    )
    
    date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
        inlets=[DATASET_CONFIGS['dataset3']]
    )    
    end = DummyOperator(task_id='end')
    
    start >> hello_task >> date_task >> end