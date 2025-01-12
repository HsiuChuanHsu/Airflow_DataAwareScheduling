from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import os


# DAG triggered by CSV updates    
def print_hello():
    print("Hello from Airflow!")
    return "Hello"

def print_date():
    print(f"Current date is: {datetime.now()}")
    return "Date Printed"


class SimpleScheduleConfig:
    """簡化的三級動態調度配置"""
    
    SCHEDULE_CONFIGS = {
        # 高負載時段 (00:00-01:59, 10:00-10:59)
        # 平均每小時 121 次請求
        "HIGH_LOAD": {
            "hours": ["0-1", "10"],
            "interval": "*/15 * * * *",  # 每15分鐘執行一次
            "batch_size": None,
            "timeout": 60              # 60秒超時
        },
        
        # 中負載時段 (02:00-09:59, 11:00-12:59, 21:00-23:59)
        # 平均每小時 53 次請求
        "MEDIUM_LOAD": {
            "hours": ["2-9", "11-12", "21-23"],
            "interval": "*/30 * * * *", # 每30分鐘執行一次
            "batch_size": None,
            "timeout": 90              # 90秒超時
        },
        
        # 低負載時段 (13:00-20:59)
        # 平均每小時 17 次請求
        "LOW_LOAD": {
            "hours": ["13-20"],
            "interval": "* 1 * * *",    # 每 60 分鐘執行一次
            "batch_size": None,
            "timeout": 120             # 120秒超時
        }
    }
    
    @classmethod
    def get_current_config(cls):
        """根據當前時間獲取對應的調度配置"""
        current_hour = datetime.now().hour
        
        # 檢查每個負載等級的時段
        for load_level, config in cls.SCHEDULE_CONFIGS.items():
            for hour_range in config["hours"]:
                if "-" in hour_range:
                    start_hour, end_hour = map(int, hour_range.split("-"))
                    if start_hour <= current_hour <= end_hour:
                        return load_level, config
                elif int(hour_range) == current_hour:
                    return load_level, config
        
        # 預設使用中等負載配置
        return "MEDIUM_LOAD", cls.SCHEDULE_CONFIGS["MEDIUM_LOAD"]

def create_dynamic_dag():
    """創建動態調度的 Kafka 消費 DAG"""
    
    load_level, config = SimpleScheduleConfig.get_current_config()
    
    dag = DAG(
        'simplified_dynamic_scheduled',
        default_args={
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date': datetime(2024, 1, 1),
            'email_on_failure': True,
            'retries': 1
        },
        description=f'Simplified dynamic Kafka consumer ({load_level})',
        schedule_interval=config['interval'],
        catchup=False,
        tags=['kafka', 'dynamic-schedule', load_level]
    )
    
    with dag:
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
    
    return dag

# 創建 DAG 實例
dag = create_dynamic_dag()

