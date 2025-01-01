from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.dataset import DatasetModel
from airflow.models.dataset import DatasetEvent
from airflow.utils.session import create_session
from datetime import datetime, timedelta
import json

def list_datasets():
    try:
        # with create_session() as session:
        #     datasets = session.query(DatasetModel).all()
        #     for dataset in datasets:
        #         dataset_info = {
        #             "URI": dataset.uri,
        #             "Created at": str(dataset.created_at),
        #             "Updated at": str(dataset.updated_at),
        #             "Extra info": dataset.extra
        #         }
        #         print("----------------------------------------")
        #         print(json.dumps(dataset_info, indent=2))
                
        #         # 顯示消費者 DAGs
        #         consuming_dags = [{"dag_id": dag.dag_id} for dag in dataset.consuming_dags]
        #         print("\nConsuming DAGs:")
        #         print(json.dumps(consuming_dags, indent=2))
                
        #         # 顯示生產者 Tasks
        #         producing_tasks = [{
        #             "task_id": task.task_id,
        #             "dag_id": task.dag_id
        #         } for task in dataset.producing_tasks]
        #         print("\nProducing Tasks:")
        #         print(json.dumps(producing_tasks, indent=2))
        #         print("----------------------------------------\n")
        with create_session() as session:
            events = session.query(DatasetEvent).all()
            for event in events:
                print(f"Dataset: {event.uri}")
                print(f"Updated by DAG: {event.source_dag_id}")
                print(f"Updated by Task: {event.source_task_id}")
                print(f"Timestamp: {event.timestamp}")
    except Exception as e:
        print(f"Error occurred: {str(e)}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'list_all_datasets',
    default_args=default_args,
    description='Lists all datasets in Airflow DB',
    schedule_interval=timedelta(days=1),  # 每天執行一次
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['utility', 'datasets'],
) as dag:

    list_datasets_task = PythonOperator(
        task_id='list_datasets',
        python_callable=list_datasets,
        # mask_secrets=False,
    )