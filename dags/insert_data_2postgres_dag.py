from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from confluent_kafka import Consumer, KafkaException, KafkaError
from airflow.operators.dummy import DummyOperator
import json
from datetime import datetime, timedelta
import os, sys
import logging
from typing import Set
from importlib import import_module

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
load_operator = import_module('load_operator')
LoadOperator = getattr(
    load_operator, 'LoadOperator'
)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create table SQL
CREATE_HISTORY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {{ params.schema_name }}.{{ params.table_name }} (
    id SERIAL PRIMARY KEY,
    dag_run_id VARCHAR(250),
    execution_date TIMESTAMP,
    data_type VARCHAR(100),
    message_timestamp TIMESTAMP,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    message_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_{{ params.table_name }}_execution_date 
    ON {{ params.schema_name }}.{{ params.table_name }}(execution_date);
CREATE INDEX IF NOT EXISTS idx_{{ params.table_name }}_data_type 
    ON {{ params.schema_name }}.{{ params.table_name }}(data_type);
"""

# 創建 DAG
with DAG(
    'create_sql_table',
    default_args=default_args,
    description='Consume from Kafka and trigger processing with history tracking',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['postgresql']
) as dag:

    start = DummyOperator(task_id='start')

    # 設置共用參數
    schema_name = 'public'
    table_name = 'dataset_trigger_history'
    
    # 使用 PostgresOperator 創建表
    create_table_task = PostgresOperator(
        task_id='create_history_table',
        postgres_conn_id='postgres_default',
        sql=CREATE_HISTORY_TABLE_SQL,
        params={
            'schema_name': schema_name,
            'table_name': table_name
        }
    )
    

    # 設置任務依賴
    start >> create_table_task 
    # consume_task >> process_task