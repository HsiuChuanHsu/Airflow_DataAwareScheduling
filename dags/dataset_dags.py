from airflow import DAG, Dataset
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random

# Define the dataset
orders_dataset = Dataset("postgresql://localhost:5434/airflow?table=test_orders")

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# DAG 1: Updates test_orders table
def generate_order_data():
    """Generate random order data"""
    products = [
        ('Laptop', 999.99),
        ('Smartphone', 599.99),
        ('Tablet', 299.99),
        ('Monitor', 399.99),
        ('Keyboard', 79.99)
    ]
    customers = ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown', 'Charlie Davis']
    statuses = ['pending', 'processing', 'completed', 'shipped']
    
    product, price = random.choice(products)
    quantity = random.randint(1, 5)
    total = price * quantity
    
    return {
        'customer': random.choice(customers),
        'product': product,
        'quantity': quantity,
        'unit_price': price,
        'total': total,
        'status': random.choice(statuses)
    }

with DAG(
    'update_orders_table',
    default_args=default_args,
    description='Update test_orders table',
    schedule_interval='*/2 * * * *',  # Run every 10 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sqltable']
) as dag_update:

    # # Task to insert new order
    # insert_order = PostgresOperator(
    #     task_id='insert_new_order',
    #     postgres_conn_id='postgres_default',
    #     sql="""
    #     INSERT INTO test_orders 
    #         (customer_name, product_name, order_date, quantity, unit_price, total_amount, status)
    #     VALUES(
    #         '{{ task_instance.xcom_pull(task_ids='generate_order_data')['customer'] }}',
    #         '{{ task_instance.xcom_pull(task_ids='generate_order_data')['product'] }}',
    #         CURRENT_DATE,
    #         {{ task_instance.xcom_pull(task_ids='generate_order_data')['quantity'] }},
    #         {{ task_instance.xcom_pull(task_ids='generate_order_data')['unit_price'] }},
    #         {{ task_instance.xcom_pull(task_ids='generate_order_data')['total'] }},
    #         '{{ task_instance.xcom_pull(task_ids='generate_order_data')['status'] }}'
    #     )
    #     """,
    #     outlets=[orders_dataset]  # Mark this dataset as updated
    # )

    # Task to generate random order data
    generate_data = PythonOperator(
        task_id='generate_order_data',
        python_callable=generate_order_data,
        outlets=[orders_dataset]  # Mark this dataset as updated
    )

    generate_data #>> insert_order



# 1. Basic Python Task DAG
def print_hello():
    print("Hello from Airflow!")
    return "Hello"

def print_date():
    print(f"Current date is: {datetime.now()}")
    return "Date Printed"

with DAG(
    'python_tasks_downstream',
    default_args=default_args,
    description='A simple DAG with Python tasks',
    schedule=[orders_dataset],  # This DAG is triggered by updates to orders_dataset
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sqltable']
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
    'python_tasks_downstream_1',
    default_args=default_args,
    description='A simple DAG with Python tasks',
    schedule=[orders_dataset],  # This DAG is triggered by updates to orders_dataset
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sqltable']
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
    'python_tasks_downstream_2',
    default_args=default_args,
    description='A simple DAG with Python tasks',
    schedule=[orders_dataset],  # This DAG is triggered by updates to orders_dataset
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sqltable']
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
    'python_tasks_downstream_3',
    default_args=default_args,
    description='A simple DAG with Python tasks',
    schedule=[orders_dataset],  # This DAG is triggered by updates to orders_dataset
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sqltable']
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