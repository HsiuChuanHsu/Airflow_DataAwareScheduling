from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import BaseOperator
from confluent_kafka import Consumer, KafkaException, KafkaError
from airflow.operators.dummy import DummyOperator
import json
from datetime import datetime, timedelta
import os
import logging

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

# class DatasetSelector:
#     def __init__(self):
#         self.dataset1 = dataset1_dataset
#         self.dataset2 = dataset2_dataset

#     def get_dataset(self, data_type):
#         if data_type == 'dataset1':
#             return [self.dataset1]
#         # elif data_type == 'dataset2':
#         #     return [self.dataset2]
#         return []

# dataset_selector = DatasetSelector()

# def get_dataset_for_type(data_type: str):
#     """Return the appropriate dataset based on type"""
#     # if data_type == 'dataset1':
#     #     return [dataset1_dataset]
#     if data_type == 'dataset2':
#         return [dataset2_dataset]
#     return []

class DynamicOutletKafkaOperator(BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.current_dataset = None

    def execute(self, context):
        conf = {
            'bootstrap.servers': 'broker:29092',
            'group.id': 'airflow_consumer_group2',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000
        }

        consumer = Consumer(conf)
        consumer.subscribe(['test-topic'])

        try:
            msg = consumer.poll(timeout=10.0)
            
            if msg is None:
                logging.info("No message received")
                self.outlets = []
                return None
                
            if msg.error():
                error_code = msg.error().code()
                if error_code == KafkaError._PARTITION_EOF:
                    logging.info("Reached end of partition")
                else:
                    logging.info(f"Consumer error: {msg.error()}")
                self.outlets = []
                return None

            # Parse the message value
            message_data = json.loads(msg.value().decode('utf-8'))
            data_type = message_data.get('data_type')
            
            # Determine file path and process data
            if data_type in DATASET_CONFIGS:
                file_path = f'/opt/airflow/files/{data_type}_data.json'
                
                # Save data to file
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                with open(file_path, 'w') as f:
                    json.dump(message_data, f)
                
                logging.info(f"Processed message of type: {data_type}")
                logging.info(f"Message timestamp: {message_data.get('timestamp')}")
                logging.info(f"Partition: {msg.partition()}, Offset: {msg.offset()}")
                
                # 設置對應的 dataset
                self.outlets = [DATASET_CONFIGS[data_type]]
                return data_type
            else:
                logging.info(f"Unknown data type: {data_type}")
                self.outlets = []
                return None

        except Exception as e:
            logging.error(f"Error processing message: {e}")
            self.outlets = []
            return None
        finally:
            consumer.commit()
            consumer.close()

with DAG(
    'kafka_consumer_dag',
    default_args=default_args,
    description='Consume from Kafka and trigger processing',
    schedule_interval='*/1 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['kafka']
) as dag:

    consume_task = DynamicOutletKafkaOperator(
        task_id='consume_kafka',
    )



# 1. Basic Python Task DAG
def print_hello():
    print("Hello from Airflow!")
    return "Hello"

def print_date():
    print(f"Current date is: {datetime.now()}")
    return "Date Printed"

with DAG(
    'kafka_tasks_downstream_1',
    default_args=default_args,
    description='A simple DAG with Python tasks',
    schedule=[DATASET_CONFIGS['dataset1']],  # This DAG is triggered by updates to orders_dataset
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['kafka']
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
    'kafka_tasks_downstream_2',
    default_args=default_args,
    description='A simple DAG with Python tasks',
    schedule=[DATASET_CONFIGS['dataset2']],  # This DAG is triggered by updates to orders_dataset
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['kafka']
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