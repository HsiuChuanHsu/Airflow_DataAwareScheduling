import os
import sys
import logging
from typing import Iterable
from contextlib import closing
from psycopg2.errors import ConnectionException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from importlib import import_module
import json
from datetime import datetime
from typing import List, Dict, Any
import dateutil.parser 

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
db_tools = import_module('db_tools')
exec_load_in_one_transaction = getattr(
    db_tools, 'exec_load_in_one_transaction'
)

class LoadOperator(BaseOperator):
    """
    Insert data from source db to target db
    """

    template_fields: Iterable[str] = (
        'schema_name',
        'table_name',
        'select_sql',
        'insert_sql',
        'delete_sql',
        'src_conn_id',
        'tar_conn_id',
    )

    template_ext: Iterable[str] = {
        ".hql",
        ".sql",
    }

    ui_color = '#bbdefb'

    @apply_defaults
    def __init__(
        self,
        schema_name: str,
        table_name: str,
        select_sql: str,
        insert_sql: str,
        delete_sql: str,
        src_conn_id: str,
        tar_conn_id: str,
        *args, **kwargs):
            
        super(LoadOperator, self).__init__(*args, **kwargs)
        self.schema_name = schema_name
        self.table_name = table_name
        self.select_sql = select_sql
        self.insert_sql = insert_sql
        self.delete_sql = delete_sql
        self.src_conn_id = src_conn_id
        self.tar_conn_id = tar_conn_id

    def process_kafka_messages(self, messages: List[Dict[str, Any]], context) -> List[Dict[str, Any]]:
        """處理來自 Kafka 的消息，轉換為資料庫參數格式"""
        dag_run = context.get('dag_run')
        execution_date = dag_run.execution_date
        dag_run_id = dag_run.run_id

        processed_messages = []
        for message in messages:
            # 使用 dateutil.parser 來解析 ISO 格式時間字符串
            timestamp_str = message.get('timestamp')
            try:
                if timestamp_str:
                    message_timestamp = dateutil.parser.parse(timestamp_str)
                else:
                    message_timestamp = datetime.now()
            except (ValueError, TypeError):
                logging.warning(f"Invalid timestamp format: {timestamp_str}, using current time")
                message_timestamp = datetime.now()
            
            processed_messages.append({
                'dag_run_id': dag_run_id,
                'execution_date': execution_date,
                'data_type': message.get('data_type'),
                'message_timestamp': message_timestamp, # datetime.fromtimestamp(float(message.get('timestamp', 0))),
                'kafka_partition': message.get('partition'),
                'kafka_offset': message.get('offset'),
                'message_data': json.dumps(message)
            })
        return processed_messages

    def execute(self, context):
        src_hook = PostgresHook(self.src_conn_id)
        tar_hook = PostgresHook(self.tar_conn_id)
        
        with closing(src_hook.get_conn()) as src_conn, \
             closing(tar_hook.get_conn()) as tar_conn:
            try:
                # 檢查是否有來自 Kafka 消費者的數據
                task_instance = context['task_instance']
                kafka_messages = task_instance.xcom_pull(task_ids='consume_kafka')
                
                if kafka_messages:
                    # 處理 Kafka 消息
                    processed_messages = self.process_kafka_messages(kafka_messages, context)
                    insert_rows = 0
                    
                    with tar_conn.cursor() as cur:
                        for params in processed_messages:
                            formatted_insert_sql = self.insert_sql.format(
                                schema_name=self.schema_name,
                                table_name=self.table_name
                            )
                            cur.execute(formatted_insert_sql, params)
                            insert_rows += 1
                            
                        msg = f"Inserted {insert_rows} records from Kafka messages"
                        logging.info(msg)

                else:
                    # 取得 source 資料並寫入 target
                    updated_rows, insert_rows = exec_load_in_one_transaction(
                        src_conn=src_conn,
                        tar_conn=tar_conn,
                        select_sql=self.select_sql,
                        insert_sql=self.insert_sql,
                        delete_sql=self.delete_sql,
                        chunksize=100000
                    )

                    msg = f"delete records({updated_rows}), \
                        +insert records({insert_rows})"
                    logging.info(msg)
                tar_conn.commit()
                logging.info("[execute_load] success")

            except ConnectionException as e:
                msg = "[execute_load] failed due to connection error"
                logging.error(msg, exc_info=True)
                raise e

            except Exception as e:
                tar_conn.rollback()
                msg = "[execute_load] failed"
                logging.error(msg, exc_info=True)
                raise e