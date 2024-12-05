from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# Define the dataset
csv_dataset = Dataset("/opt/airflow/files/orders.csv")

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

def check_for_file(**context):
    """Check if CSV file exists and is not empty"""
    file_path = '/opt/airflow/files/orders.csv'
    
    if not os.path.exists(file_path):
        return False
        
    if os.path.getsize(file_path) == 0:
        return False
        
    # Check if file is being written to
    try:
        with open(file_path, 'r') as f:
            first_line = f.readline()
        return True
    except:
        return False

# def generate_sql_statements(**context):
#     """Generate SQL insert statements from CSV data"""
#     records = context['task_instance'].xcom_pull(key='csv_records')
    
#     sql_statements = []
#     for record in records:
#         sql = f"""
#         INSERT INTO test_orders 
#             (customer_name, product_name, order_date, quantity, unit_price, total_amount, status)
#         VALUES (
#             '{record['customer_name']}',
#             '{record['product_name']}',
#             '{record['order_date']}',
#             {record['quantity']},
#             {record['unit_price']},
#             {record['quantity'] * record['unit_price']},
#             '{record['status']}'
#         );
#         """
#         sql_statements.append(sql)
    
#     return "\n".join(sql_statements)

# DAG for monitoring and processing CSV files
with DAG(
    'csv_file_processor',
    default_args=default_args,
    description='Monitor and process CSV files',
    schedule_interval='*/5 * * * *',  # Check every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['csv', 'file_processing']
) as dag_monitor:

    # Check if new CSV file exists
    check_file = PythonOperator(
        task_id='check_for_csv',
        python_callable=check_for_file,
        outlets=[csv_dataset]
    )
    check_file
    # # Generate SQL
    # generate_sql = PythonOperator(
    #     task_id='generate_sql',
    #     python_callable=generate_sql_statements,
    #     outlets=[csv_dataset]
    # )


    # Set up task dependencies
    # check_file >> generate_sql



# DAG triggered by CSV updates    
def print_hello():
    print("Hello from Airflow!")
    return "Hello"

def print_date():
    print(f"Current date is: {datetime.now()}")
    return "Date Printed"

with DAG(
    'file_python_tasks_downstream',
    default_args=default_args,
    description='A simple DAG with Python tasks',
    schedule=[csv_dataset],  # This DAG is triggered by updates to orders_dataset
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['csv', 'file_processing']
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
