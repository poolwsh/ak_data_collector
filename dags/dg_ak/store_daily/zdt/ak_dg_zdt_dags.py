from __future__ import annotations

import os
import sys
from pathlib import Path
current_path = Path(__file__).resolve().parent 
project_root = os.path.abspath(os.path.join(current_path, '..', '..', '..', '..'))
print(project_root)
# 将项目根目录添加到sys.path中
sys.path.append(project_root)


import socket
import pandas as pd
from datetime import datetime, timedelta
from dags.dg_ak.utils.dg_ak_util_funcs import DgAkUtilFuncs as dguf
from utils.logger import logger
import utils.config as con

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

config_path = current_path / 'ak_dg_zdt_config.py'
sys.path.append(config_path.parent.as_posix())
ak_cols_config_dict = dguf.load_ak_cols_config(config_path.as_posix())

# 统一定义Redis keys
ZDT_DATE_LIST_KEY_PREFIX = "zdt_date_list"
STORED_KEYS_KEY_PREFIX = "stored_keys"

TRACING_TABLE_NAME = 'ak_dg_tracing_by_date'

default_trade_dates = 50
rollback_days = 10


def get_tracing_data(ak_func_name: str):
    logger.info(f"Starting to get tracing dataframe for {ak_func_name}")
    try:
        td_list = dguf.get_trade_dates(pg_conn)
        td_list.sort(reverse=True)
        if LOGGER_DEBUG:
            logger.debug(f'length of reversed td list from trade date table {len(td_list)}')
            logger.debug(f'first 5:{td_list[:5]}')
        
        cursor = pg_conn.cursor()
        cursor.execute(f"SELECT last_td FROM {TRACING_TABLE_NAME} WHERE ak_func_name = %s", (ak_func_name,))
        result = cursor.fetchone()
        if result:
            last_td = result[0]
            if LOGGER_DEBUG:
                logger.debug(f"Last trading date for {ak_func_name}: {last_td}")
            start_index = td_list.index(last_td) if last_td in td_list else 0
            selected_dates = td_list[:start_index + rollback_days + 1]
        else:
            selected_dates = td_list[:default_trade_dates]
        
        if LOGGER_DEBUG:
            logger.debug(f'length of selected_dates {len(selected_dates)}')
            logger.debug(f'selected_dates:{selected_dates}')
        cursor.close()
        
        date_df = pd.DataFrame(selected_dates, columns=['td'])
        redis_key = f'{ZDT_DATE_LIST_KEY_PREFIX}_{ak_func_name}'
        dguf.write_df_to_redis(redis_key, date_df, redis_hook.get_conn(), con.DEFAULT_REDIS_TTL)
        if LOGGER_DEBUG:
            logger.debug(f"Tracing dataframe for {ak_func_name} written to Redis with key: {redis_key}")
    except Exception as e:
        logger.error(f"Failed to get tracing dataframe for {ak_func_name}: {str(e)}")
        raise AirflowException(e)

def get_zdt_data(ak_func_name):
    logger.info(f"Starting to save data for {ak_func_name}")
    try:
        redis_key = f'{ZDT_DATE_LIST_KEY_PREFIX}_{ak_func_name}'
        temp_csv_path = dguf.get_data_and_save2csv(redis_key, ak_func_name, ak_cols_config_dict, pg_conn, redis_hook.get_conn())
        if LOGGER_DEBUG:
            logger.debug(f"Data for {ak_func_name} saved to CSV at {temp_csv_path}")

        # 清空表内容
        clear_table_sql = f"TRUNCATE TABLE ak_dg_{ak_func_name};"
        copy_sql = f"COPY ak_dg_{ak_func_name} FROM STDIN WITH CSV"
        with pg_conn.cursor() as cursor:
            cursor.execute(clear_table_sql)
            with open(temp_csv_path, 'r') as file:
                cursor.copy_expert(sql=copy_sql, file=file)
            pg_conn.commit()
        logger.info(f"Table ak_dg_{ak_func_name} has been cleared.")
        logger.info("Data successfully inserted into PostgreSQL from CSV.")

        if os.path.exists(temp_csv_path):
            os.remove(temp_csv_path)
            logger.info(f"{temp_csv_path} has been removed.")
    except ValueError as ve:
        if ("只能获取最近 30 个交易日的数据" in str(ve)) or ("Length mismatch" in str(ve)):
            logger.warning(f"Error fetching data for date {date}: {ve}")
        else:
            raise ve
    except KeyError as e:
        logger.error(f"KeyError while processing data for {ak_func_name}: {str(e)}")
        raise AirflowException(e)
    except Exception as e:
        logger.error(f"Failed to save data for {ak_func_name}: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)
    finally:
        if 'cursor' in locals():
            cursor.close()

def store_zdt_data(ak_func_name: str):
    logger.info(f"Starting copy and clean data operations for {ak_func_name}")

    try:
        source_table = f'ak_dg_{ak_func_name}'
        store_table = f'ak_dg_{ak_func_name}_store'
        # 动态获取表的列名        
        columns = dguf.get_columns_from_table(pg_conn, source_table, redis_hook.get_conn())
        column_names = [col[0] for col in columns]  # Extract column names
        columns_str = ', '.join(column_names)
        update_columns = ', '.join([f"{col} = EXCLUDED.{col}" for col in column_names if col not in ['td', 's_code']])

        insert_sql = f"""
            INSERT INTO {store_table} ({columns_str})
            SELECT {columns_str} FROM {source_table}
            ON CONFLICT (td, s_code) DO UPDATE SET 
            {update_columns}
            RETURNING td;
        """
        
        inserted_rows = dguf.store_ak_data(pg_conn, ak_func_name, insert_sql, truncate=False)
        # 提取所有 td 的最大值
        if inserted_rows:
            max_td = max(row[0] for row in inserted_rows)

            if LOGGER_DEBUG:
                logger.debug(f"Max td to store in Redis for {ak_func_name}: {max_td}")

            redis_key = f"{STORED_KEYS_KEY_PREFIX}@{ak_func_name}"
            dguf.write_list_to_redis(redis_key, [max_td.strftime('%Y-%m-%d')], redis_hook.get_conn())
            logger.info(f"Data operation completed successfully for {ak_func_name}. Max td: {max_td}")
        else:
            logger.warning(f"No rows inserted for {ak_func_name}, skipping Redis update.")

    except Exception as e:
        logger.error(f"Failed during copy and clean operations for {ak_func_name}: {str(e)}")
        raise AirflowException(e)

def update_tracing_data(ak_func_name):
    logger.info(f"Preparing to insert tracing data for {ak_func_name}")
    try:
        redis_key = f"{STORED_KEYS_KEY_PREFIX}@{ak_func_name}"
        date_list = dguf.read_list_from_redis(redis_key, redis_hook.get_conn())
        if LOGGER_DEBUG:
            logger.debug(f"Date list from Redis: {date_list}")

        host_name = os.getenv('HOSTNAME', socket.gethostname())
        current_time = datetime.now()

        if not date_list:
            logger.warning("No dates to process for tracing data.")
            return

        data = [(ak_func_name, date, current_time, current_time, host_name) for date in date_list]

        insert_sql = f"""
            INSERT INTO {TRACING_TABLE_NAME} (ak_func_name, last_td, create_time, update_time, host_name)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ak_func_name) DO UPDATE 
            SET last_td = EXCLUDED.last_td, update_time = EXCLUDED.update_time;
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

ak_func_name_mapping = {
    'stock_zt_pool_em': '涨停股池',
    'stock_zt_pool_previous_em': '昨日涨停股池',
    'stock_zt_pool_strong_em': '强势股池',
    'stock_zt_pool_sub_new_em': '次新股池',
    'stock_zt_pool_zbgc_em': '炸板股池',
    'stock_zt_pool_dtgc_em': '跌停股池'
}

def generate_dag_name(ak_func_name: str) -> str:
    description = ak_func_name_mapping.get(ak_func_name, ak_func_name)
    return f'涨跌停-东方财富-{description}'

def generate_dag(ak_func_name: str):
    logger.info(f"Generating DAG for {ak_func_name}")
    default_args = {
        'owner': 'wsh',
        'depends_on_past': False,
        'email': ['wshmxgz@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=con.DEFAULT_RETRY_DELAY)
    }

    dag_name = generate_dag_name(ak_func_name)

    dag = DAG(
        dag_name,
        default_args=default_args,
        description=f'利用akshare的函数{ak_func_name}下载涨跌停相关数据',
        start_date=days_ago(1),
        
        schedule=dguf.generate_random_minute_schedule(hour=9), # 北京时间: 9+8=17
        catchup=False,
        tags=['akshare', 'store_daily', '涨跌停'],
        max_active_runs=1,
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
