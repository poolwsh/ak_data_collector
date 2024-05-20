from __future__ import annotations

import os
import socket
from datetime import datetime, timedelta
import pandas as pd
from dg_ak.utils.util_funcs import UtilFuncs as uf
from dg_ak.utils.logger import logger
import dg_ak.utils.config as con

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.utils.dates import days_ago

# 配置日志调试开关
LOGGER_DEBUG = con.LOGGER_DEBUG

# 配置数据库连接
redis_hook = RedisHook(redis_conn_id=con.REDIS_CONN_ID)
pgsql_hook = PostgresHook(postgres_conn_id=con.TXY800_PGSQL_CONN_ID)
pg_conn = pgsql_hook.get_conn()

# 统一定义Redis keys
ZDT_DATE_LIST_KEY_PREFIX = "zdt_date_list"
STORED_KEYS_KEY_PREFIX = "stored_keys"

def get_tracing_data(ak_func_name: str):
    logger.info(f"Starting to get tracing dataframe for {ak_func_name}")
    try:
        date_list = uf.get_date_list(pg_conn, ak_func_name)
        date_df = pd.DataFrame(date_list, columns=['Date'])
        redis_key = uf.get_redis_key(ZDT_DATE_LIST_KEY_PREFIX, ak_func_name)
        uf.write_df_to_redis(redis_key, date_df, redis_hook.get_conn(), uf.default_redis_ttl)
        if LOGGER_DEBUG:
            logger.debug(f"Tracing dataframe for {ak_func_name} written to Redis with key: {redis_key}")
    except Exception as e:
        logger.error(f"Failed to get tracing dataframe for {ak_func_name}: {str(e)}")
        raise AirflowException(e)

def get_zdt_data(ak_func_name):
    logger.info(f"Starting to save data for {ak_func_name}")
    try:
        redis_key = uf.get_redis_key(ZDT_DATE_LIST_KEY_PREFIX, ak_func_name)
        temp_csv_path = uf.get_data_and_save2csv(redis_key, ak_func_name, pg_conn, redis_hook.get_conn())
        if LOGGER_DEBUG:
            logger.debug(f"Data for {ak_func_name} saved to CSV at {temp_csv_path}")
        
        cursor = pg_conn.cursor()
        with open(temp_csv_path, 'r') as file:
            copy_sql = f"COPY ak_dg_{ak_func_name} FROM STDIN WITH CSV"
            cursor.copy_expert(sql=copy_sql, file=file)
        pg_conn.commit()
        logger.info("Data successfully inserted into PostgreSQL from CSV.")
    except Exception as e:
        logger.error(f"Failed to save data for {ak_func_name}: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)
    finally:
        cursor.close()
        os.remove(temp_csv_path)
        logger.info(f"{temp_csv_path} has been removed.")

def store_zdt_data(ak_func_name: str):
    logger.info(f"Starting copy and clean data operations for {ak_func_name}")

    insert_sql = f"""
        INSERT INTO ak_dg_{ak_func_name}_store
        SELECT * FROM ak_dg_{ak_func_name}
        ON CONFLICT (td, s_code) DO UPDATE
        SET s_name = EXCLUDED.s_name,
            pct_chg = EXCLUDED.pct_chg,
            c = EXCLUDED.c,
            a = EXCLUDED.a,
            circulation_mv = EXCLUDED.circulation_mv,
            total_mv = EXCLUDED.total_mv,
            turnover_rate = EXCLUDED.turnover_rate,
            lock_fund = EXCLUDED.lock_fund,
            first_lock_time = EXCLUDED.first_lock_time,
            last_lock_time = EXCLUDED.last_lock_time,
            failed_account = EXCLUDED.failed_account,
            zt_stat = EXCLUDED.zt_stat,
            zt_account = EXCLUDED.zt_account,
            industry = EXCLUDED.industry
        RETURNING td, s_code;
    """

    try:
        inserted_rows = uf.store_ak_data(pg_conn, ak_func_name, insert_sql, truncate=False)
        tds_codes = [(row[0], row[1]) for row in inserted_rows]
        redis_key = uf.get_redis_key(STORED_KEYS_KEY_PREFIX, ak_func_name)
        uf.write_list_to_redis(redis_key, tds_codes, redis_hook.get_conn(), uf.default_redis_ttl)
        logger.info(f"Data operation completed successfully for {ak_func_name}. Stored keys: {tds_codes}")
    except Exception as e:
        logger.error(f"Failed during copy and clean operations for {ak_func_name}: {str(e)}")
        raise AirflowException(e)

def update_tracing_data(ak_func_name):
    logger.info(f"Preparing to insert tracing data for {ak_func_name}")
    try:
        redis_key = uf.get_redis_key(STORED_KEYS_KEY_PREFIX, ak_func_name)
        date_list = uf.read_list_from_redis(redis_key, redis_hook.get_conn())
        if LOGGER_DEBUG:
            logger.debug(f"Date list from Redis: {date_list}")

        host_name = os.getenv('HOSTNAME', socket.gethostname())
        category = "zdt"
        current_time = datetime.now()

        if not date_list:
            logger.warning("No dates to process for tracing data.")
            return

        data = [(ak_func_name, date, current_time, current_time, category, True, host_name) for date in date_list]

        insert_sql = """
            INSERT INTO ak_dg_tracing_dt (ak_func_name, dt, create_time, update_time, category, is_active, host_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ak_func_name, dt) DO UPDATE 
            SET update_time = EXCLUDED.update_time;
        """

        cursor = pg_conn.cursor()
        cursor.executemany(insert_sql, data)
        pg_conn.commit()
        logger.info(f"Inserted/Updated tracing data for {len(date_list)} dates in {ak_func_name}")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"Error executing bulk insert for {ak_func_name}: {e}")
        raise
    finally:
        cursor.close()

def generate_dag(ak_func_name):
    logger.info(f"Generating DAG for {ak_func_name}")
    default_args = {
        'owner': 'wsh',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'email': ['wshmxgz@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'max_active_runs': 1,
        'retry_delay': timedelta(minutes=5),
        'tags': ['akshare', 'store_daily', '涨跌停'],
        'schedule': '17 0 * * *', # 北京时间: 17+8=25-24=1
        'catchup': False,
    }

    dag = DAG(
        f'ak_dg_zdt_{ak_func_name}',
        default_args=default_args,
        description=f'利用akshare的函数{ak_func_name}下载涨跌停相关数据'
    )

    tasks = {
        'get_tracing_data': PythonOperator(
            task_id=f'get_tracing_data_{ak_func_name}',
            python_callable=get_tracing_data,
            op_kwargs={'ak_func_name': ak_func_name},
            dag=dag,
        ),
        'get_data': PythonOperator(
            task_id=f'get_data_{ak_func_name}',
            python_callable=get_zdt_data,
            op_kwargs={'ak_func_name': ak_func_name},
            dag=dag,
        ),
        'store_data': PythonOperator(
            task_id=f'store_data_{ak_func_name}',
            python_callable=store_zdt_data,
            op_kwargs={'ak_func_name': ak_func_name},
            dag=dag,
        ),
        'tracing_data': PythonOperator(
            task_id=f'tracing_data_{ak_func_name}',
            python_callable=update_tracing_data,
            op_kwargs={'ak_func_name': ak_func_name},
            dag=dag,
        )
    }
    tasks['get_tracing_data'] >> tasks['get_data'] >> tasks['store_data'] >> tasks['tracing_data']
    return dag

ak_func_name_list = [
    'stock_zt_pool_em', 'stock_zt_pool_previous_em', 'stock_zt_pool_strong_em', 
    'stock_zt_pool_sub_new_em', 'stock_zt_pool_zbgc_em', 'stock_zt_pool_dtgc_em'
]

for func_name in ak_func_name_list:
    dag_name = f'ak_dg_zdt_{func_name}'
    globals()[dag_name] = generate_dag(func_name)
    logger.info(f"DAG for {dag_name} successfully created and registered.")
