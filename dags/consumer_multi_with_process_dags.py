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

class BatchDynamicOutletKafkaOperator(BaseOperator):
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
            # 'sasl.mechanism': 'SCRAM-SHA-512',
            # 'sasl.username': 'your-username',
            # 'sasl.password': 'your-password'
        }

        consumer = ConsumerTool(conf)
        subscribe_response = consumer.subscribe(self.topics)  # 使用 subscribe 方法，需要傳入 list 格式
        
        if not subscribe_response['success']:
            logging.error(f"Failed to subscribe to topics {self.topics}: {subscribe_response['msg']}")
            return None

        try:
            processed_count = 0
            active_datasets: Set[str] = set()  # 追蹤需要觸發的資料集
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
                    db_name = message_data.get('db_name')
                    schema_name = message_data.get('schema_name')
                    table_name = message_data.get('table_name')
                    
                    # 記錄處理信息
                    logging.info(f"Processed message {processed_count + 1} of type: {table_name}")
                    logging.info(f"Message timestamp: {message_data.get('timestamp')}")
                    logging.info(f"Partition: {msg.partition()}, Offset: {msg.offset()}")
                    
                    # 添加到活動數據集集合
                    active_datasets.add(table_name)
                    messages_processed.append({
                        'db_name': db_name,
                        'schema_name': schema_name,
                        'table_name': table_name,
                        'timestamp': message_data.get('timestamp'),
                        'partition': msg.partition(),
                        'offset': msg.offset(),
                        'raw_data': message_data  # 添加原始數據
                    })
                    
                    processed_count += 1
                    
                except json.JSONDecodeError as e:
                    logging.error(f"Error decoding message: {e}")
                    continue
                except Exception as e:
                    logging.error(f"Error processing message: {e}")
                    continue

            if processed_count == 0:
                raise AirflowSkipException("No valid messages were processed")

            logging.info(f"Batch processing completed. Processed {processed_count} messages.")
            logging.info(f"Active datasets: {active_datasets}")           
            return messages_processed
        
        except AirflowSkipException:
            raise  # 重新拋出 AirflowSkipException
        except Exception as e:
            logging.error(f"Error in batch processing: {e}")
            return None
        finally:
            # consumer.commit()
            consumer.close()

class DatasetOutletOperator(BaseOperator):
    """負責處理數據並設置 outlets 的 Operator"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        # 從上游任務獲取消息
        task_instance = context['task_instance']
        messages = task_instance.xcom_pull(task_ids='consume_kafka')
        
        if not messages:
            logging.warning("No messages to process")
            self.outlets = []
            return None
            
        # 建立 Dataset URLs 
        dataset_urls = []
        for msg in messages:
            try:
                db_name = msg.get('db_name')
                schema_name = msg.get('schema_name')
                table_name = msg.get('table_name')

                if all(x is not None for x in [db_name, schema_name, table_name]):
                    url = f"postgresql://localhost:5432/{db_name}/{schema_name}/{table_name}"
                    dataset = Dataset(url)
                    dataset_urls.append(dataset)
                    logging.info(f"New Dataset: {dataset}")

            except Exception as e:
                logging.error(f"Error processing message {msg}")

        # 識別需要觸發的數據集
        active_datasets = {msg['table_name'] for msg in messages}
        
        # 設置 outlets
        self.outlets = dataset_urls
        
        logging.info(f"Processing completed. Total messages: {len(messages)}")
        logging.info(f"Active datasets: {active_datasets}")
        
        return {
            'messages_processed': len(messages),
            'active_datasets': list(active_datasets),
            'airflow_version': airflow_version  # 添加 Airflow 版本
        }
    
# Create table SQL
# Create table SQL
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
    airflow_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_{{ params.table_name }}_table_name
    ON {{ params.schema_name }}.{{ params.table_name }}(table_name);
CREATE INDEX IF NOT EXISTS idx_{{ params.table_name }}_schema_name
    ON {{ params.schema_name }}.{{ params.table_name }}(schema_name);
"""

INSERT_HISTORY_SQL = f"""
    INSERT INTO {{schema_name}}.{{table_name}} 
        (dag_run_id, execution_date, db_name, schema_name, table_name, 
         message_timestamp, kafka_partition, kafka_offset, message_data, airflow_version)
    VALUES 
        (%s, %s::timestamp, %s, %s, %s, %s::timestamp, %s, %s, %s::jsonb, %s)
"""

def store_history_records(
    schema_name, 
    table_name, 
    consume_task_id='consume_kafka',
    postgres_conn_id='postgres_default',
    **context
):
    """Store message history records in PostgreSQL"""
    task_instance = context['task_instance']
    messages = task_instance.xcom_pull(task_ids=consume_task_id)
    
    if not messages:
        logging.warning("No messages to process")
        return
    
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    try:
        insert_sql = INSERT_HISTORY_SQL.format(
            schema_name=schema_name,
            table_name=table_name
        )
        
        # 確保數據類型正確轉換
        insert_data = [
            (
                str(context['dag_run'].run_id),
                context['execution_date'].isoformat(),
                str(msg['db_name']),
                str(msg['schema_name']),
                str(msg['table_name']),
                msg['timestamp'].isoformat() if isinstance(msg.get('timestamp'), datetime) else msg.get('timestamp'),
                int(msg['partition']),
                int(msg['offset']),
                json.dumps(dict(msg.get('raw_data', {}))),
                str(airflow_version)
            )
            for msg in messages
        ]
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, insert_sql, insert_data)
            conn.commit()
            
        logging.info(f"Stored {len(insert_data)} records in history table")
        
    except Exception as e:
        logging.error(f"Error storing history records: {e}")
        raise


# Kafka 主題配置
KAFKA_TOPICS = {
    'batch_topics': ['test-topic_batch'],  # 可以設定多個主題
    'bootstrap_servers': 'broker:29092',
    'group_id': 'airflow_consumer_group_batch'
}

with DAG(
    'consumer_multi_with_process_dags',
    default_args=default_args,
    description='Consume from Kafka and trigger processing',
    schedule_interval='*/3 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['kafka', 'multi-message', 'process']
) as dag:

    consume_task = BatchDynamicOutletKafkaOperator(
        task_id='consume_kafka',
        batch_size=5,  # 每次處理5條消息
        timeout=30,   # 5秒超時
        topics=KAFKA_TOPICS['batch_topics'],
        bootstrap_servers=KAFKA_TOPICS['bootstrap_servers'],
        group_id=KAFKA_TOPICS['group_id']
    )

    # 處理數據並設置 outlets 的任務
    process_task = DatasetOutletOperator(
        task_id='process_and_set_outlets'
    )

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
    
    # 使用 LoadOperator 存儲歷史記錄
    store_history_task = PythonOperator(
        task_id='store_into_history',
        python_callable=store_history_records,
        op_kwargs={
            'schema_name': schema_name,
            'table_name': table_name,
            'consume_task_id': 'consume_kafka',  # 可自訂
            'postgres_conn_id': 'postgres_default'  # 可自訂
        },
        provide_context=True,
        dag=dag
    )

    # 設置任務依賴
    consume_task >> process_task
    consume_task >> create_table_task >> store_history_task
