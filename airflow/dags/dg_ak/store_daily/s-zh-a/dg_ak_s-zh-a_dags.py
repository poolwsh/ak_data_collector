from __future__ import annotations

import os
import sys
import socket
import pandas as pd
from typing import List
from pathlib import Path
from datetime import timedelta, datetime
from airflow.models.dag import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import psycopg2.extras

from dags.utils.db import PGEngine, task_cache_conn
from dags.utils.logger import logger
from dags.dg_ak.utils.dg_ak_util_funcs import DgAkUtilFuncs as dgakuf
from dags.dg_ak.utils.dg_ak_config import dgak_config as con

current_path = Path(__file__).resolve().parent 
config_path = current_path / 'dg_ak_s-zh-a_config.py'
sys.path.append(config_path.parent.as_posix())
ak_cols_config_dict = dgakuf.load_ak_cols_config(config_path.as_posix())

ARG_LIST_CACHE_PREFIX = "dg_ak_s_zh_a_arg_list"
FAILED_STOCKS_CACHE_PREFIX = "failed_stocks"

TRACING_TABLE_NAME = 'dg_ak_tracing_s_zh_a'
TRADE_DATE_TABLE_NAME = 'dg_ak_stock_zh_a_trade_date'
STOCK_CODE_NAME_TABLE = 'dg_ak_stock_zh_a_code_name'

DEBUG_MODE = con.DEBUG_MODE
DEFAULT_END_DATE = dgakuf.format_td8(datetime.now())
DEFAULT_START_DATE = con.ZH_A_DEFAULT_START_DATE
BATCH_SIZE = 5000  
ROLLBACK_DAYS = 15 

def insert_code_name_to_db(code_name_list: list[tuple[str, str]]):
    conn = None
    try:
        conn = PGEngine.get_conn()
        with conn.cursor() as cursor:
            sql = f"""
                INSERT INTO {STOCK_CODE_NAME_TABLE} (s_code, s_name, create_time, update_time)
                VALUES (%s, %s, NOW(), NOW())
                ON CONFLICT (s_code) DO UPDATE 
                SET s_name = EXCLUDED.s_name, update_time = EXCLUDED.update_time;
            """
            cursor.executemany(sql, code_name_list)
            conn.commit()
            logger.info(f"s_code and s_name inserted into {STOCK_CODE_NAME_TABLE} successfully.")
    except Exception as e:
        logger.error(f"Failed to insert s_code and s_name into {STOCK_CODE_NAME_TABLE}: {e}")
        raise AirflowException(e)
    finally:
        if conn:
            PGEngine.release_conn(conn)

def update_tracing_table_bulk(ak_func_name: str, period: str, adjust: str, updates: List[tuple]):
    conn = None
    try:
        conn = PGEngine.get_conn()
        sql = f"""
            INSERT INTO {TRACING_TABLE_NAME} (ak_func_name, scode, period, adjust, last_td, create_time, update_time, host_name)
            VALUES %s
            ON CONFLICT (ak_func_name, scode, period, adjust) DO UPDATE 
            SET last_td = EXCLUDED.last_td, update_time = EXCLUDED.update_time, host_name = EXCLUDED.host_name;
        """
        hostname = os.getenv('HOSTNAME', socket.gethostname())
        values = [(ak_func_name, s_code, period, adjust, last_td, datetime.now(), datetime.now(), hostname) for s_code, last_td in updates]

        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, sql, values)
        conn.commit()
        logger.info(f"Tracing data updated for {len(updates)} records in {TRACING_TABLE_NAME}.")
    except Exception as e:
        logger.error(f"Failed to update tracing table in bulk: {e}")
        raise AirflowException(e)
    finally:
        if conn:
            PGEngine.release_conn(conn)

def update_trade_dates(conn, trade_dates):
    insert_date_sql = f"""
        INSERT INTO {TRADE_DATE_TABLE_NAME} (trade_date, create_time, update_time)
        VALUES (%s, NOW(), NOW())
        ON CONFLICT (trade_date) DO NOTHING;
    """
    with conn.cursor() as cursor:
        cursor.executemany(insert_date_sql, [(date,) for date in trade_dates])
    conn.commit()

def process_batch_data(ak_func_name, period, adjust, combined_df, all_trade_dates, conn):
    if DEBUG_MODE:
        logger.debug(f"Combined DataFrame columns for {ak_func_name}: {combined_df.columns}")

    combined_df['s_code'] = combined_df['s_code'].astype(str)
    combined_df = dgakuf.convert_columns(combined_df, f'dg_ak_{ak_func_name}_{period}_{adjust}', conn, task_cache_conn)

    if 'td' in combined_df.columns:
        combined_df['td'] = pd.to_datetime(combined_df['td'], errors='coerce').dt.strftime('%Y-%m-%d')

    temp_csv_path = dgakuf.save_data_to_csv(combined_df, f'{ak_func_name}_{period}_{adjust}')
    if temp_csv_path is None:
        raise AirflowException(f"No CSV file created for {ak_func_name}, skipping database insertion.")

   
    dgakuf.insert_data_from_csv(conn, temp_csv_path, f'dg_ak_{ak_func_name}_{period}_{adjust}', task_cache_conn)

   
    update_trade_dates(conn, all_trade_dates)

    last_td = combined_df['td'].max()
    updates = [(s_code, last_td) for s_code in combined_df['s_code'].unique()]

    update_tracing_table_bulk(ak_func_name, period, adjust, updates)

def prepare_arg_list(ak_func_name: str, period: str, adjust: str):
    conn = None
    try:
        conn = PGEngine.get_conn()
        tracing_df = dgakuf.get_tracing_data_df(conn, TRACING_TABLE_NAME)
        current_tracing_df = tracing_df[
            (tracing_df['ak_func_name'] == ak_func_name) &
            (tracing_df['period'] == period) &
            (tracing_df['adjust'] == adjust)
        ]
        tracing_dict = dict(zip(current_tracing_df['scode'].values, current_tracing_df['last_td'].values))

        s_code_name_list = dgakuf.get_s_code_name_list(task_cache_conn)
        insert_code_name_to_db(s_code_name_list)
        
        arg_list = []
        for s_code, s_name in s_code_name_list:
            start_date = tracing_dict.get(s_code, DEFAULT_START_DATE)

            if start_date != DEFAULT_START_DATE:
                start_date = (datetime.strptime(str(start_date), '%Y-%m-%d') - timedelta(days=ROLLBACK_DAYS)).strftime('%Y-%m-%d')
            arg_list.append((s_code, dgakuf.format_td8(start_date), DEFAULT_END_DATE))

        redis_key = f"{ARG_LIST_CACHE_PREFIX}@{ak_func_name}@{period}@{adjust}"
        dgakuf.write_list_to_redis(redis_key, arg_list, task_cache_conn)
        logger.info(f"Argument list for {ak_func_name} with period={period} and adjust={adjust} has been prepared and cached.")
    finally:
        if conn:
            PGEngine.release_conn(conn)

def process_stock_data(ak_func_name: str, period: str, adjust: str):
    conn = None
    try:
        conn = PGEngine.get_conn()
        logger.info(f"Starting to save data for {ak_func_name} with period={period} and adjust={adjust}")
        redis_key = f"{ARG_LIST_CACHE_PREFIX}@{ak_func_name}@{period}@{adjust}"
        arg_list = dgakuf.read_list_from_redis(redis_key, task_cache_conn)

        if not arg_list:
            raise AirflowException(f"No arguments available for {ak_func_name}, skipping data fetch.")

        if DEBUG_MODE:
            logger.debug(f"Config dictionary for {ak_func_name}: {ak_cols_config_dict}")

        total_codes = len(arg_list)
        all_data = []
        total_rows = 0
        all_trade_dates = set()
        failed_stocks = []

        for index, (s_code, start_date, end_date) in enumerate(arg_list):
            try:
                logger.info(f'({index + 1}/{total_codes}) Fetching data for s_code={s_code} from {start_date} to {end_date}')
                if adjust == 'bfq':
                    stock_data_df = dgakuf.get_s_code_data(
                        ak_func_name, ak_cols_config_dict, s_code, period, start_date, end_date, None
                    )
                else:
                    stock_data_df = dgakuf.get_s_code_data(
                        ak_func_name, ak_cols_config_dict, s_code, period, start_date, end_date, adjust
                    )

                if not stock_data_df.empty:
                    all_data.append(stock_data_df)
                    total_rows += len(stock_data_df)
                    all_trade_dates.update(stock_data_df['td'].unique())
                    if DEBUG_MODE:
                        logger.debug(f's_code={s_code}, len(stock_data_df)={len(stock_data_df)}, len(all_data)={len(all_data)}, total_rows={total_rows}')
                else:
                    failed_stocks.append(arg_list[index])

            
                if total_rows >= BATCH_SIZE or (index + 1) == total_codes:
                    _combined_df = pd.concat(all_data, ignore_index=True)
                    process_batch_data(ak_func_name, period, adjust, _combined_df, all_trade_dates, conn)
                    all_data = []
                    total_rows = 0
                    all_trade_dates.clear()

            except Exception as e:
                logger.error(f"Failed to process data for s_code={s_code}: {e}")
                failed_stocks.append(arg_list[index])

        if failed_stocks:
            dgakuf.write_list_to_redis(FAILED_STOCKS_CACHE_PREFIX, failed_stocks, task_cache_conn)
            logger.info(f"Failed stocks: {failed_stocks}")

    except Exception as e:
        logger.error(f"Failed to process data for {ak_func_name}: {e}")
        raise AirflowException(e)
    finally:
        if conn:
            PGEngine.release_conn(conn)

def retry_failed_stocks(ak_func_name: str, period: str, adjust: str):
    try:
        logger.info(f"Retrying failed stocks for {ak_func_name} with period={period} and adjust={adjust}")
        failed_stocks = dgakuf.read_list_from_redis(FAILED_STOCKS_CACHE_PREFIX, task_cache_conn)
        if not failed_stocks:
            logger.info("No failed stocks to retry.")
            return

        logger.info(f"Failed stocks detected: {failed_stocks}")

        if failed_stocks:
            formatted_failed_stocks = "\n".join([str(index) for index in failed_stocks])
            logger.warning(f"Warning: There are failed indexes that need to be retried:\n{formatted_failed_stocks}")

    except Exception as e:
        logger.error(f"Failed to retry stocks for {ak_func_name}: {e}")
        raise AirflowException(e)

def generate_dag_name(stock_func, period, adjust) -> str:
    adjust_mapping = {
        'bfq': '除权',
        'hfq': '后复权'
    }
    period_mapping = {
        'daily': '日线',
        'weekly': '周线'
    }
    source_mapping = {
        'stock_zh_a_hist': '东方财富'
    }
    return f"个股行情-{source_mapping.get(stock_func, stock_func)}-{period_mapping.get(period, period)}-{adjust_mapping.get(adjust, adjust)}"

def generate_dag(stock_func, period, adjust):
    logger.info(f"Generating DAG for {stock_func} with period={period} and adjust={adjust}")
    default_args = {
        'owner': con.DEFAULT_OWNER,
        'depends_on_past': False,
        'email': [con.DEFAULT_EMAIL],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': con.DEFAULT_RETRIES,
        'retry_delay': timedelta(minutes=con.DEFAULT_RETRY_DELAY)
    }

    dag_name = generate_dag_name(stock_func, period, adjust)

    dag = DAG(
        dag_name,
        default_args=default_args,
        description=f'利用akshare的函数{stock_func}(period={period}, adjust={adjust})下载个股行情相关数据',
        start_date=days_ago(1),
        schedule=dgakuf.generate_random_minute_schedule(hour=8), 
        catchup=False,
        tags=['akshare', 'store_daily', '个股行情'],
        max_active_runs=1,
        params={},
    )

    tasks = {
        'prepare_arg_list': PythonOperator(
            task_id=f'prepare_arg_list_{stock_func}_{period}_{adjust}',
            python_callable=prepare_arg_list,
            op_kwargs={'ak_func_name': stock_func, 'period': period, 'adjust': adjust},
            dag=dag,
        ),
        'process_stock_data': PythonOperator(
            task_id=f'process_stock_data_{stock_func}_{period}_{adjust}',
            python_callable=process_stock_data,
            op_kwargs={'ak_func_name': stock_func, 'period': period, 'adjust': adjust},
            dag=dag,
        ),
        'retry_failed_stocks': PythonOperator(
            task_id=f'retry_failed_stocks_{stock_func}_{period}_{adjust}',
            python_callable=retry_failed_stocks,
            op_kwargs={'ak_func_name': stock_func, 'period': period, 'adjust': adjust},
            dag=dag,
        ),
    }
    tasks['prepare_arg_list'] >> tasks['process_stock_data'] >> tasks['retry_failed_stocks']
    return dag

def create_dags(ak_func_name, period, adjust):
    globals()[f'dg_ak_s_zh_a_{ak_func_name}_{period}_{adjust}'] = generate_dag(ak_func_name, period, adjust)
    logger.info(f"DAG for {ak_func_name} successfully created and registered.")

create_dags('stock_zh_a_hist', 'daily', 'hfq')
create_dags('stock_zh_a_hist', 'daily', 'bfq')
