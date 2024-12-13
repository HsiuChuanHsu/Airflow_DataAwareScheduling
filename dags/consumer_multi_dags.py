from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import BaseOperator
from confluent_kafka import Consumer, KafkaException, KafkaError
from airflow.operators.dummy import DummyOperator
import json
from datetime import datetime, timedelta
import os
import logging
from typing import Set

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
        **kwargs
    ):
        super().__init__(**kwargs)
        self.batch_size = batch_size
        self.timeout = timeout
        self.current_dataset = None

    def execute(self, context):
        conf = {
            'bootstrap.servers': 'broker:29092',
            'group.id': 'airflow_consumer_group_batch',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000
        }

        consumer = Consumer(conf)
        consumer.subscribe(['test-topic_batch'])

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
                            'offset': msg.offset()
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

            # 設置需要觸發的數據集
            self.outlets = [
                DATASET_CONFIGS[dataset_type] 
                for dataset_type in active_datasets
            ]

            logging.info(f"Batch processing completed. Processed {processed_count} messages.")
            logging.info(f"Active datasets: {active_datasets}")
            
            return {
                'messages_processed': len(messages_processed),
                'messages_details': messages_processed,
                'active_datasets': list(active_datasets)
            }

        except Exception as e:
            logging.error(f"Error in batch processing: {e}")
            self.outlets = []
            return None
        finally:
            consumer.commit()
            consumer.close()
            
with DAG(
    'kafka_consumer_multi_dag',
    default_args=default_args,
    description='Consume from Kafka and trigger processing',
    schedule_interval='*/3 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['kafka', 'multi-message']
) as dag:

    consume_task = BatchDynamicOutletKafkaOperator(
        task_id='consume_kafka',
        batch_size=5,  # 每次處理5條消息
        timeout=30,   # 5秒超時
    )



# 1. Basic Python Task DAG
def print_hello():
    print("Hello from Airflow!")
    return "Hello"

def print_date():
    print(f"Current date is: {datetime.now()}")
    return "Date Printed"

with DAG(
    'kafka_tasks_multi_downstream_1',
    default_args=default_args,
    description='A simple DAG with Python tasks',
    schedule=[DATASET_CONFIGS['dataset1']],  # This DAG is triggered by updates to orders_dataset
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['kafka', 'multi-message']
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
    'kafka_tasks_multi_downstream_2',
    default_args=default_args,
    description='A simple DAG with Python tasks',
    schedule=[DATASET_CONFIGS['dataset2']],  # This DAG is triggered by updates to orders_dataset
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['kafka', 'multi-message']
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