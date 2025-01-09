from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow import __version__ as airflow_version
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
import psycopg2.extras
import requests

import json
from datetime import datetime, timedelta
import os
import logging
from typing import Set, List

from load_operator import LoadOperator
from kafka_tools import ConsumerTool, KafkaError, KafkaException

# Define datasets for different types

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



# SQL 定義
# SQL 定義
CREATE_HISTORY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {{ params.schema_name }}.{{ params.table_name }} (
    id SERIAL PRIMARY KEY,
    dag_run_id VARCHAR(250),
    execution_date TIMESTAMP,
    db_name VARCHAR(100),
    schema_name VARCHAR(100),
    table_name VARCHAR(100),
    message_timestamp TIMESTAMP,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    message_data JSONB,
    api_url TEXT,
    api_status_code INTEGER,
    api_success BOOLEAN,
    api_error_message TEXT,
    api_response JSONB,
    airflow_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_{{ params.table_name }}_table_name
    ON {{ params.schema_name }}.{{ params.table_name }}(table_name);
CREATE INDEX IF NOT EXISTS idx_{{ params.table_name }}_schema_name
    ON {{ params.schema_name }}.{{ params.table_name }}(schema_name);
"""

INSERT_HISTORY_SQL = """
    INSERT INTO {schema_name}.{table_name} 
        (dag_run_id, execution_date, db_name, schema_name, table_name, 
         message_timestamp, kafka_partition, kafka_offset, message_data,
         api_url, api_status_code, api_success, api_error_message, api_response,
         airflow_version)
    VALUES 
        (%s, %s::timestamp, %s, %s, %s, %s::timestamp, %s, %s, %s::jsonb,
         %s, %s, %s, %s, %s::jsonb, %s)
"""

class BatchKafkaOperator(BaseOperator):
    def __init__(
        self, 
        batch_size: int = 10,  # 每次處理的最大消息數
        timeout: float = 30.0,  # 輪詢超時時間
        topics: List[str] = None,
        bootstrap_servers: str = 'broker:29092',
        group_id: str = 'airflow_consumer_group_batch',
        **kwargs
    ):
        super().__init__(**kwargs)
        self.batch_size = batch_size
        self.timeout = timeout
        self.topics = topics or ['test-topic_batch']  # 預設主題
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.current_dataset = None

    def execute(self, context):
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
            # 如果真的需要 SASL 認證，則添加以下設定
            'security.protocol': 'PLAINTEXT',
        }

        consumer = ConsumerTool(conf)
        subscribe_response = consumer.subscribe(self.topics)  # 使用 subscribe 方法，需要傳入 list 格式
        
        if not subscribe_response['success']:
            logging.error(f"Failed to subscribe to topics {self.topics}: {subscribe_response['msg']}")
            return None

        try:
            processed_count = 0
            messages_processed = []

            # 持續消費消息直到達到批次大小或超時
            while processed_count < self.batch_size:
                msg = consumer.poll(timeout=self.timeout)
                
                if msg is None:
                    logging.info("No more messages to process")
                    break
                    
                if msg.error():
                    error_code = msg.error().code()
                    if error_code == KafkaError._PARTITION_EOF:
                        logging.info("Reached end of partition")
                    else:
                        logging.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    # Parse the message value
                    message_data = json.loads(msg.value().decode('utf-8'))
                    
                    # 只處理 ObjectCreated:Copy 事件
                    if message_data.get('EventName') == 's3:ObjectCreated:Copy':
                        records = message_data.get('Records', [])
                        for record in records:
                            user_metadata = record.get('s3', {}).get('object', {}).get('userMetadata', {})
                            api_url = user_metadata.get('X-Amz-Meta-Api')
                            api_id = user_metadata.get('X-Amz-Meta-Id')
                            
                            if api_url and api_id:
                                messages_processed.append({
                                    'api_url': api_url,
                                    'api_id': api_id,
                                    'event_time': record.get('eventTime'),
                                    'bucket': record.get('s3', {}).get('bucket', {}).get('name'),
                                    'object_key': record.get('s3', {}).get('object', {}).get('key'),
                                    'partition': msg.partition(),
                                    'offset': msg.offset(),
                                    'raw_data': message_data  # 保存完整的原始數據
                                })
                                processed_count += 1
                                logging.info(f"Found valid message: API URL={api_url}, ID={api_id}")
                    
                except json.JSONDecodeError as e:
                    logging.error(f"Error decoding message: {e}")
                    continue
                except Exception as e:
                    logging.error(f"Error processing message: {e}")
                    continue

            if not messages_processed:
                raise AirflowSkipException("No valid messages were processed")

            logging.info(f"Batch processing completed. Found {len(messages_processed)} valid messages.")
            return messages_processed
        
        finally:
            consumer.close()


def call_api(**context):
    """
    Call the API with the processed messages and prepare history records
    Record both successful and failed API calls with details
    """
    task_instance = context['task_instance']
    dag_run = context['dag_run']
    messages = task_instance.xcom_pull(task_ids='consume_kafka')
    
    if not messages:
        logging.info("No messages to process")
        return
    
    api_results = []
    history_records = []
    
    for message in messages:
        api_url = message.get('api_url')
        api_id = message.get('api_id')
        full_url = f"{api_url}definitionif={api_id}"
        
        # 準備基本的歷史記錄數據
        history_record = {
            'dag_run_id': dag_run.run_id,
            'execution_date': dag_run.execution_date.isoformat(),
            'db_name': message.get('bucket'),
            'schema_name': 'public',
            'table_name': message.get('object_key'),
            'message_timestamp': message.get('event_time'),
            'kafka_partition': message.get('partition'),
            'kafka_offset': message.get('offset'),
            'message_data': json.dumps(message.get('raw_data')),
            'api_url': full_url,
            'airflow_version': airflow_version
        }
        
        try:
            response = requests.get(full_url, timeout=30)  # 添加超時設置
            response_data = None
            
            try:
                response_data = response.json()
            except json.JSONDecodeError:
                response_data = {'raw_response': response.text}
            
            # 更新歷史記錄與 API 結果
            history_record.update({
                'api_status_code': response.status_code,
                'api_success': response.ok,  # 根據狀態碼判斷（200-299 為 True）
                'api_error_message': None,
                'api_response': json.dumps(response_data)
            })
            
            api_results.append({
                'url': full_url,
                'status': response.status_code,
                'success': response.ok,
                'response': response_data,
                'event_time': message.get('event_time'),
                'bucket': message.get('bucket'),
                'object_key': message.get('object_key')
            })
            
            logging.info(f"API call completed - URL: {full_url}, Status: {response.status_code}")
            
        except requests.RequestException as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            logging.error(f"Error calling API {full_url}: {error_msg}")
            
            # 更新歷史記錄與錯誤信息
            history_record.update({
                'api_status_code': getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                'api_success': False,
                'api_error_message': error_msg,
                'api_response': json.dumps({
                    'error_type': type(e).__name__,
                    'error_details': str(e)
                })
            })
            
            api_results.append({
                'url': full_url,
                'error': error_msg,
                'success': False,
                'event_time': message.get('event_time')
            })
        
        finally:
            # 添加歷史記錄
            history_records.append(history_record)
            logging.debug(f"Prepared history record for: {full_url}")
    
    if history_records:
        task_instance.xcom_push(key='history_records', value=history_records)
        logging.info(f"Pushed {len(history_records)} history records to XCom")
    else:
        logging.warning("No history records were created")
    
    return api_results

def insert_history_records(**context):
    """
    Insert history records into PostgreSQL with API call results
    """
    task_instance = context['task_instance']
    history_records = task_instance.xcom_pull(key='history_records', task_ids='call_api')
    
    if not history_records:
        logging.info("No history records to insert")
        return
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    schema_name = 'public'
    table_name = 'minio_events_history'
    
    insert_sql = INSERT_HISTORY_SQL.format(
        schema_name=schema_name,
        table_name=table_name
    )
    
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            for record in history_records:
                cur.execute(insert_sql, (
                    record['dag_run_id'],
                    record['execution_date'],
                    record['db_name'],
                    record['schema_name'],
                    record['table_name'],
                    record['message_timestamp'],
                    record['kafka_partition'],
                    record['kafka_offset'],
                    record['message_data'],
                    record.get('api_url'),
                    record.get('api_status_code'),
                    record.get('api_success'),
                    record.get('api_error_message'),
                    record.get('api_response'),
                    record['airflow_version']
                ))
        conn.commit()
    
    logging.info(f"Successfully inserted {len(history_records)} history records")

# Kafka 主題配置
KAFKA_TOPICS = {
    'batch_topics': ['minio-events'],  # 可以設定多個主題
    'bootstrap_servers': 'broker:29092',
    'group_id': 'airflow_minio_consumer_group_batch'
}

with DAG(
    'consumer_minio_dags',
    default_args=default_args,
    description='Consume from Kafka and trigger processing',
    schedule_interval='*/3 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['kafka', 'minio', 'process']
) as dag:
    # 創建歷史記錄表
    create_history_table = PostgresOperator(
        task_id='create_history_table',
        postgres_conn_id='postgres_default',
        sql=CREATE_HISTORY_TABLE_SQL,
        params={
            'schema_name': 'public',
            'table_name': 'minio_events_history'
        }
    )

    consume_task = BatchKafkaOperator(
        task_id='consume_kafka',
        batch_size=5,  # 每次處理5條消息
        timeout=30,   # 5秒超時
        topics=KAFKA_TOPICS['batch_topics'],
        bootstrap_servers=KAFKA_TOPICS['bootstrap_servers'],
        group_id=KAFKA_TOPICS['group_id']
    )

    api_call_task = PythonOperator(
        task_id='call_api',
        python_callable=call_api,
        provide_context=True
    )
    
    # 插入歷史記錄任務
    insert_history = PythonOperator(
        task_id='insert_history',
        python_callable=insert_history_records,
        provide_context=True
    )
    
    # 設定任務依賴關係
    create_history_table >> consume_task >> api_call_task >> insert_history