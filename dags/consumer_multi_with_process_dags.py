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
    'dataset1': Dataset("file:///opt/airflow/files/dataset1_data.json"),
    'dataset2': Dataset("file:///opt/airflow/files/dataset2_data.json")
}

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
                    data_type = message_data.get('data_type')
                    
                    if data_type in DATASET_CONFIGS:
                        # 記錄處理信息
                        logging.info(f"Processed message {processed_count + 1} of type: {data_type}")
                        logging.info(f"Message timestamp: {message_data.get('timestamp')}")
                        logging.info(f"Partition: {msg.partition()}, Offset: {msg.offset()}")
                        
                        # 添加到活動數據集集合
                        active_datasets.add(data_type)
                        messages_processed.append({
                            'data_type': data_type,
                            'timestamp': message_data.get('timestamp'),
                            'partition': msg.partition(),
                            'offset': msg.offset(),
                            'raw_data': message_data  # 添加原始數據
                        })
                        
                        processed_count += 1
                    else:
                        logging.warning(f"Unknown data type: {data_type}")
                        continue

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
            
        # 識別需要觸發的數據集
        active_datasets = {msg['data_type'] for msg in messages}
        
        # 設置 outlets
        self.outlets = [DATASET_CONFIGS[dataset_type] for dataset_type in active_datasets]
        
        logging.info(f"Processing completed. Total messages: {len(messages)}")
        logging.info(f"Active datasets: {active_datasets}")
        
        return {
            'messages_processed': len(messages),
            'active_datasets': list(active_datasets)
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

# Insert SQL for LoadOperator
INSERT_HISTORY_SQL = """
INSERT INTO {schema_name}.{table_name} 
(dag_run_id, execution_date, data_type, message_timestamp, 
 kafka_partition, kafka_offset, message_data)
SELECT 
    %(dag_run_id)s as dag_run_id,
    %(execution_date)s::timestamp as execution_date,
    %(data_type)s as data_type,
    %(message_timestamp)s::timestamp as message_timestamp,
    %(kafka_partition)s as kafka_partition,
    %(kafka_offset)s as kafka_offset,
    %(message_data)s::jsonb as message_data
WHERE NOT EXISTS (
    SELECT 1 
    FROM {schema_name}.{table_name}
    WHERE dag_run_id = %(dag_run_id)s
    AND data_type = %(data_type)s
    AND kafka_partition = %(kafka_partition)s
    AND kafka_offset = %(kafka_offset)s
);
"""  

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
    store_history_task = LoadOperator(
        task_id='store_into_history',
        schema_name=schema_name,
        table_name=table_name,
        select_sql='',  # 不需要 select
        insert_sql=INSERT_HISTORY_SQL,
        delete_sql='',  # 不需要 delete
        src_conn_id='postgres_default',
        tar_conn_id='postgres_default'
    )

    # 設置任務依賴
    consume_task >> process_task
    consume_task >> create_table_task >> store_history_task


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
    schedule=[DATASET_CONFIGS['dataset1']],  # This DAG is triggered by updates to orders_dataset
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
    schedule=[DATASET_CONFIGS['dataset2']],  # This DAG is triggered by updates to orders_dataset
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