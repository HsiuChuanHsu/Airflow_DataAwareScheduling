from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, KafkaException
from airflow.operators.dummy import DummyOperator
import json
from datetime import datetime, timedelta
import os

# Define datasets for different types
dataset1_dataset = Dataset("file:///opt/airflow/files/dataset1_data.json")
dataset2_dataset = Dataset("file:///opt/airflow/files/dataset2_data.json")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

class DatasetSelector:
    def __init__(self):
        self.dataset1 = dataset1_dataset
        self.dataset2 = dataset2_dataset

    def get_dataset(self, data_type):
        if data_type == 'dataset1':
            return [self.dataset1]
        elif data_type == 'dataset2':
            return [self.dataset2]
        return []

dataset_selector = DatasetSelector()

def consume_from_kafka(**context):
    """Consume messages from Kafka and save to appropriate files"""
    conf = {
        'bootstrap.servers': 'broker:29092',
        'group.id': 'airflow_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe(['test-topic'])

    try:
        msg = consumer.poll(timeout=10.0)
        
        if msg is None:
            print("No message received")
            return None
            
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                print("Reached end of partition")
                return None
            else:
                print(f"Consumer error: {msg.error()}")
                return None

        # Parse the message value
        message_data = json.loads(msg.value().decode('utf-8'))
        data_type = message_data.get('data_type')
        
        # Determine file path based on data type
        if data_type == 'dataset1':
            file_path = '/opt/airflow/files/dataset1_data.json'
        elif data_type == 'dataset2':
            file_path = '/opt/airflow/files/dataset2_data.json'
        else:
            print(f"Unknown data type: {data_type}")
            return None

        # Save data to file
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            json.dump(message_data, f)
        
        print(f"Processed message of type: {data_type}")
        return data_type

    except Exception as e:
        print(f"Error processing message: {e}")
        return None
    finally:
        consumer.close()

with DAG(
    'kafka_consumer_dag',
    default_args=default_args,
    description='Consume from Kafka and trigger processing',
    schedule_interval='*/1 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    consume_task = PythonOperator(
        task_id='consume_kafka',
        python_callable=consume_from_kafka,
        outlets=dataset_selector.get_dataset('dataset1') + dataset_selector.get_dataset('dataset2')
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
    schedule=[dataset1_dataset],  # This DAG is triggered by updates to orders_dataset
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
    schedule=[dataset2_dataset],  # This DAG is triggered by updates to orders_dataset
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