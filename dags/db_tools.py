import os
import logging
from typing import Type, List
from contextlib import closing
import psycopg2
from psycopg2.extras import execute_values
from airflow.providers.postgres.hooks.postgres import PostgresHook

def exec_load_in_one_transaction(
    src_conn: Type[psycopg2.extensions.connection],
    tar_conn: Type[psycopg2.extensions.connection],
    select_sql: str,
    insert_sql: str,
    delete_sql: str = None,
    chunksize: int = 100000
) -> tuple:
    """
    Load data from source database to target database
    
    1. delete target database data to avoid duplicate
    2. select data from source database
    3. batch insert to target database
    """
    try:
        tar_conn.autocommit = False
        with tar_conn.cursor() as tar_cur:
            tar_cur.itersize = chunksize
            if delete_sql:
                tar_cur.execute(delete_sql)
                updated_rows = tar_cur.rowcount
            else:
                updated_rows = None
            
            with src_conn:
                with src_conn.cursor("icursor") as src_cur:
                    src_cur.itersize = chunksize
                    src_cur.execute(select_sql)
                    insert_rows = 0
                    
                    while True:
                        rows = src_cur.fetchmany(chunksize)
                        if len(rows) > 0:
                            execute_values(
                                tar_cur, 
                                insert_sql,
                                rows, 
                                page_size=chunksize
                            )
                            insert_rows += tar_cur.rowcount
                        else:
                            break
                            
        tar_conn.commit()
        logging.info('load success')
    except Exception as error:
        tar_conn.rollback()
        raise error
    else:
        return updated_rows, insert_rows

def select_one(
    conn: Type[psycopg2.extensions.connection],
    sqlstring: str,
    cursor_factory=None
) -> List:
    """
    Execute select statement and fetch one result
    
    Args:
        - conn: A connection of database
        - sqlstring: A select statement
        
    Returns:
        - a tuple: result of select statement
    """
    with conn.cursor(cursor_factory=cursor_factory) as cur:
        cur.execute(sqlstring)
        result = cur.fetchone()
    return result

def check_age(conn_id, data_ym):
    sqlstring = "SELECT max(age) FROM if_polaris.demo_time_trigger_v1 "\
                f"WHERE data_ym='{data_ym}'"
    
    hook = PostgresHook(conn_id)
    with closing(hook.get_conn()) as conn:
        result = select_one(conn, sqlstring)
        logging.info(f"max age is {result}")
        
    if result[0] > 100:
        return 'email_alert'
    else:
        return 'pass_task'

def get_csv_path(csv_file):
    filepath = os.path.join(
        os.path.abspath(
            os.path.dirname(__file__)
        ),
        "data",
        csv_file
    )
    return filepath