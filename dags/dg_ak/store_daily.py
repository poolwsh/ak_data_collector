from __future__ import annotations

import os
import json
import redis
from typing import Optional, Union
from datetime import datetime, timedelta, date
import akshare as ak
import pandas as pd
from functools import partial

from psycopg2 import sql
from dg_ak.utils import UtilTools as ut
from dg_ak.util_funcs import UtilFuncs as uf

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, ExternalPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis import RedisHook


POSTGRES_CONN_ID = "local_pgsql"
TXY800_PGSQL_CONN_ID = "txy800_pgsql_ak"
REDIS_CONN_ID = "local_redis_3"

redis_hook = RedisHook(redis_conn_id=REDIS_CONN_ID)
pgsql_hook = PostgresHook(postgres_conn_id=TXY800_PGSQL_CONN_ID)
pg_conn = pgsql_hook.get_conn()

def get_tracing_df(ti, ak_func_name: str):
    _date_list = uf.get_date_list(pg_conn, ak_func_name)
    _date_df = pd.DataFrame(_date_list, columns=['Date'])
    _redis_key = f'{ak_func_name}_date_list'
    uf.write_df_to_redis(_redis_key, _date_df, redis_hook.get_conn(), uf.default_redis_ttl)
    ti.xcom_push(key=f'{ak_func_name}_date_list', value=_redis_key)


# download_and_cache_stock_zt_pool_em_by_td = partial(uf.download_and_cache_ak_data_by_td, ak_func_name='stock_zt_pool_em')

def save_ak_zt_pool_em(ti, ak_func_name):
    _redis_key = ti.xcom_pull(key=f'{ak_func_name}_date_list', task_ids='get_ak_zt_pool_em_tracing')
    _temp_csv_path = uf.get_data_and_save2csv(_redis_key, ak_func_name, pg_conn, redis_hook.get_conn())

    _cursor = pg_conn.cursor()
    try:
        with open(_temp_csv_path, 'r') as f:
            _cursor.copy_expert((f"COPY ak_dg_{ak_func_name} FROM STDIN WITH CSV"), f)
        pg_conn.commit()
        print("Data successfully inserted into PostgreSQL from CSV.")
    except Exception as e:
        print("Error: ", e)
        pg_conn.rollback()
        raise AirflowException(e)
    finally:
        _cursor.close()
        os.remove(_temp_csv_path)
        print(f"{_temp_csv_path} has been removde.")
    pass


def copy_and_clean_data(ti, ak_func_name):
    try:
        _conn = pgsql_hook.get_conn()
        _cursor = _conn.cursor()
        
        # Perform the INSERT operation
        insert_sql = f"""
        INSERT INTO ak_dg_{ak_func_name}_store
        SELECT * FROM ak_dg_{ak_func_name}
        ON CONFLICT (td, s_code) DO NOTHING
        RETURNING td;
        """
        _cursor.execute(insert_sql)
        inserted_tds = _cursor.fetchall()  # Fetch all returned tds

        # Fetch the distinct inserted tds
        if inserted_tds:
            fetch_sql = f"SELECT array_agg(distinct td) FROM ak_dg_{ak_func_name}_store WHERE td IN (SELECT td FROM ak_dg_{ak_func_name});"
            _cursor.execute(fetch_sql)
            result = _cursor.fetchone()
            if result:
                ti.xcom_push(key='return_value', value=result[0])
            else:
                ti.xcom_push(key='return_value', value=[])
        else:
            print("No new data was inserted.")
            ti.xcom_push(key='return_value', value=[])  # Push empty list if no new tds

        # Truncate the original table after operation
        truncate_sql = f"TRUNCATE TABLE ak_dg_{ak_func_name};"
        _cursor.execute(truncate_sql)
        
        _conn.commit()  # Commit all changes
        print("Data operation completed successfully.")
    
    except Exception as e:
        if _conn:
            _conn.rollback()
        print(f"Failed to execute SQL operations: {str(e)}")
        raise AirflowException(e)
    
    finally:
        if _cursor:
            _cursor.close()
        if _conn:
            _conn.close()

def insert_tracing_data(ti, ak_func_name):
    pg_hook = PostgresHook(postgres_conn_id='txy800_pgsql_ak')
    td_list = ti.xcom_pull(task_ids='copy_and_clean_data', key='return_value')
    host_name = os.getenv('HOSTNAME')
    current_time = datetime.now()

    if not td_list:
        print("No dates to process.")
        return

    data = [(ak_func_name, td, current_time, current_time, host_name) for td in td_list]

    insert_sql = """
        INSERT INTO dg_ak_tracing_dt (ak_func_name, dt, create_time, update_time, host_name)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (ak_func_name, dt) DO UPDATE 
        SET update_time = EXCLUDED.update_time;
        """

    # 获取数据库连接和游标
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # 执行批量插入
    try:
        cursor.executemany(insert_sql, data)
        conn.commit()
        print(f"Inserted/Updated tracing data for {len(td_list)} dates.")
    except Exception as e:
        conn.rollback()
        print(f"Error executing bulk insert: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


with DAG(
    dag_id="ak_store_daily",
    default_args={
        "owner": "wsh",
        "depends_on_past": False,
        "email": ["mxgzmxgz@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    # [END default_args]
    description="利用akshare下载每日数据.",
    schedule="15 15 * * *",
    start_date=datetime(2024, 4, 1),
    catchup=False,
    tags=["akshare", "daily"],
    max_active_runs=1,
    params={
     },
) as dag:
    # [END instantiate_dag]
    dag.doc_md = """
    依据设定每日运行，下载相关数据
    """  # otherwise, type it like this

    get_ak_zt_pool_em_tracing_task = PythonOperator(
        task_id = "get_ak_zt_pool_em_tracing",
        python_callable=get_tracing_df,
        op_kwargs={'ak_func_name': 'stock_zt_pool_em'},
        provide_context=True
    )

    ak_zt_pool_em_downdload_all = PythonOperator(
        task_id = "ak_zt_pool_em_downdload_all",
        python_callable=save_ak_zt_pool_em,
        op_kwargs={'ak_func_name': 'stock_zt_pool_em'},
        provide_context=True
    )

    copy_and_clean_task = PythonOperator(
        task_id='copy_and_clean_data',
        python_callable=copy_and_clean_data,
        op_kwargs={'ak_func_name': 'stock_zt_pool_em'},
        provide_context=True,
        dag=dag
    )
    
    insert_tracing_task = PythonOperator(
        task_id='insert_tracing_data',
        python_callable=insert_tracing_data,
        op_kwargs={'ak_func_name': 'stock_zt_pool_em'},
        provide_context=True,
        dag=dag
    )

    get_ak_zt_pool_em_tracing_task >> ak_zt_pool_em_downdload_all >> copy_and_clean_task >> insert_tracing_task

