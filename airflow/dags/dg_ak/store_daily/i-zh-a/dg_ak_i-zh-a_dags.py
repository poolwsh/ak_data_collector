from __future__ import annotations

import os
import sys
import socket
import redis
import pandas as pd
import akshare as ak
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
config_path = current_path / 'dg_ak_i-zh-a_config.py'
sys.path.append(config_path.parent.as_posix())
ak_cols_config_dict = dgakuf.load_ak_cols_config(config_path.as_posix())

ARG_LIST_CACHE_PREFIX = "dg_ak_i_zh_a_arg_list"
FAILED_INDEXES_CACHE_PREFIX = "failed_indexes"

TRACING_TABLE_NAME = 'dg_ak_tracing_i_zh_a'
INDEX_CODE_NAME_TABLE = 'dg_ak_index_zh_a_code_name'

DEBUG_MODE = con.DEBUG_MODE
DEFAULT_END_DATE = dgakuf.format_td8(datetime.now())
DEFAULT_START_DATE = con.ZH_A_DEFAULT_START_DATE
BATCH_SIZE = 5000  
ROLLBACK_DAYS = 15 

def get_i_code_name_list(redis_conn: redis.Redis, pg_conn, ttl: int = 60 * 60):
    if DEBUG_MODE:
        logger.debug("Attempting to get index codes and names list from Redis.")

    try:
        _df = dgakuf.read_df_from_redis(con.INDEX_A_REALTIME_KEY, redis_conn)
        if _df is not None:
            logger.info('Read index real-time data from Redis successfully.')
            if DEBUG_MODE:
                logger.debug(f"Index codes and names list length: {len(_df)}")
                logger.debug("First 5:")
                for item in _df[['i_code', 'i_name']].values.tolist()[:5]:
                    logger.debug(item)
            return _df[['i_code', 'i_name']].values.tolist()
        else:
            logger.warning(f"No data found in Redis for key: {con.INDEX_A_REALTIME_KEY}")
    except Exception as _e:
        logger.warning(f"Failed to read index real-time data from Redis: {_e}")

    symbols = ["上证系列指数", "深证系列指数", "指数成份", "中证系列指数"]
    combined_df = pd.DataFrame()
    
    for symbol in symbols:
        try:
            _df = ak.stock_zh_index_spot_em(symbol=symbol)
            if _df is not None and '代码' in _df.columns and '名称' in _df.columns:
                _df.rename(columns={'代码': 'i_code', '名称': 'i_name'}, inplace=True)
                _df['symbol'] = symbol
                _df['i_code'] = _df['i_code'].astype(str)
                combined_df = pd.concat([combined_df, _df], ignore_index=True)
                dgakuf.write_df_to_redis(con.INDEX_A_REALTIME_KEY, combined_df, redis_conn, ttl)
                if DEBUG_MODE:
                    logger.debug(f"Fetched and cached index codes and names list length: {len(combined_df)}")
                    logger.debug("First 5:")
                    for item in combined_df[['i_code', 'i_name']].values.tolist()[:5]:
                        logger.debug(item)
        except Exception as _inner_e:
            logger.error(f"Error while fetching or writing data for symbol {symbol}: {_inner_e}")
    
    if not combined_df.empty:
        update_i_code_name_in_db(combined_df, pg_conn)
        return combined_df[['i_code', 'i_name']].values.tolist()
    
    # Read from database if Redis and AkShare data are not available
    try:
        query = "SELECT i_code, i_name FROM dg_ak_index_zh_a_code_name"
        _df = pd.read_sql(query, pg_conn)
        if not _df.empty:
            if DEBUG_MODE:
                logger.debug(f"Read index codes and names list from database, length: {len(_df)}")
                logger.debug("First 5:")
                for item in _df[['i_code', 'i_name']].values.tolist()[:5]:
                    logger.debug(item)
            return _df[['i_code', 'i_name']].values.tolist()
        else:
            logger.warning("No data found in database table dg_ak_index_zh_a_code_name.")
    except Exception as _db_e:
        logger.error(f"Error reading from database: {_db_e}")
    
    return []

def update_i_code_name_in_db(df, pg_conn):
    try:
        with pg_conn.cursor() as cursor:
            for index, row in df.iterrows():
                sql = f"""
                    INSERT INTO dg_ak_index_zh_a_code_name (i_code, i_name, symbol, create_time, update_time)
                    VALUES (%s, %s, %s, NOW(), NOW())
                    ON CONFLICT (i_code) DO UPDATE 
                    SET i_name = EXCLUDED.i_name, symbol = EXCLUDED.symbol, update_time = EXCLUDED.update_time;
                """
                cursor.execute(sql, (row['i_code'], row['i_name'], row['symbol']))
            pg_conn.commit()
            logger.info("Index codes and names inserted/updated in database successfully.")
    except Exception as e:
        pg_conn.rollback()
        logger.error(f"Failed to insert/update index codes and names in database: {e}")
        raise

def insert_code_name_to_db(code_name_list: list[tuple[str, str]]):
    conn = None
    try:
        conn = PGEngine.get_conn()
        with conn.cursor() as cursor:
            sql = f"""
                INSERT INTO {INDEX_CODE_NAME_TABLE} (i_code, i_name, create_time, update_time)
                VALUES (%s, %s, NOW(), NOW())
                ON CONFLICT (i_code) DO UPDATE 
                SET i_name = EXCLUDED.i_name, update_time = EXCLUDED.update_time;
            """
            cursor.executemany(sql, code_name_list)
            conn.commit()
            logger.info(f"i_code and i_name inserted into {INDEX_CODE_NAME_TABLE} successfully.")
    except Exception as e:
        logger.error(f"Failed to insert i_code and i_name into {INDEX_CODE_NAME_TABLE}: {e}")
        raise AirflowException(e)
    finally:
        if conn:
            PGEngine.release_conn(conn)

def update_tracing_table_bulk(ak_func_name: str, period: str, updates):
    conn = None
    try:
        conn = PGEngine.get_conn()
        sql = f"""
            INSERT INTO {TRACING_TABLE_NAME} (ak_func_name, icode, period, last_td, create_time, update_time, host_name)
            VALUES %s
            ON CONFLICT (ak_func_name, icode, period) DO UPDATE 
            SET last_td = EXCLUDED.last_td, update_time = EXCLUDED.update_time, host_name = EXCLUDED.host_name;
        """
        hostname = os.getenv('HOSTNAME', socket.gethostname())
        values = [(ak_func_name, i_code, period, last_td, datetime.now(), datetime.now(), hostname) for i_code, last_td in updates]

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

def process_batch_data(ak_func_name, period, combined_df, conn):
    if DEBUG_MODE:
        logger.debug(f"Combined DataFrame columns for {ak_func_name}: {combined_df.columns}")

    combined_df['i_code'] = combined_df['i_code'].astype(str)
    combined_df = dgakuf.convert_columns(combined_df, f'dg_ak_{ak_func_name}_{period}', conn, task_cache_conn)

    if 'td' in combined_df.columns:
        combined_df['td'] = pd.to_datetime(combined_df['td'], errors='coerce').dt.strftime('%Y-%m-%d')

    temp_csv_path = dgakuf.save_data_to_csv(combined_df, f'{ak_func_name}_{period}')
    if temp_csv_path is None:
        raise AirflowException(f"No CSV file created for {ak_func_name}, skipping database insertion.")

    dgakuf.insert_data_from_csv(conn, temp_csv_path, f'dg_ak_{ak_func_name}_{period}', task_cache_conn)


    last_td = combined_df['td'].max()
    updates = [(i_code, last_td) for i_code in combined_df['i_code'].unique()]

    update_tracing_table_bulk(ak_func_name, period, updates)

def prepare_arg_list(ak_func_name: str, period: str):
    conn = None
    try:
        conn = PGEngine.get_conn()
        tracing_df = dgakuf.get_tracing_data_df(conn, TRACING_TABLE_NAME)
        current_tracing_df = tracing_df[
            (tracing_df['ak_func_name'] == ak_func_name) &
            (tracing_df['period'] == period)
        ]
        tracing_dict = dict(zip(current_tracing_df['icode'].values, current_tracing_df['last_td'].values))

        i_code_name_list = get_i_code_name_list(task_cache_conn, conn)
        insert_code_name_to_db(i_code_name_list)
        
        arg_list = []
        for i_code, i_name in i_code_name_list:
            start_date = tracing_dict.get(i_code, DEFAULT_START_DATE)

            if start_date != DEFAULT_START_DATE:
                start_date = (datetime.strptime(str(start_date), '%Y-%m-%d') - timedelta(days=ROLLBACK_DAYS)).strftime('%Y-%m-%d')
            arg_list.append((i_code, dgakuf.format_td8(start_date), DEFAULT_END_DATE))

        redis_key = f"{ARG_LIST_CACHE_PREFIX}@{ak_func_name}@{period}"
        dgakuf.write_list_to_redis(redis_key, arg_list, task_cache_conn)
        logger.info(f"Argument list for {ak_func_name} with period={period} has been prepared and cached.")
    finally:
        if conn:
            PGEngine.release_conn(conn)

def process_index_data(ak_func_name: str, period: str):
    conn = None
    try:
        conn = PGEngine.get_conn()
        logger.info(f"Starting to save data for {ak_func_name} with period={period}")
        redis_key = f"{ARG_LIST_CACHE_PREFIX}@{ak_func_name}@{period}"
        arg_list = dgakuf.read_list_from_redis(redis_key, task_cache_conn)

        if not arg_list:
            raise AirflowException(f"No arguments available for {ak_func_name}, skipping data fetch.")

        if DEBUG_MODE:
            logger.debug(f"Config dictionary for {ak_func_name}: {ak_cols_config_dict}")

        total_codes = len(arg_list)
        all_data = []
        total_rows = 0
        failed_indexes = []

        for index, (i_code, start_date, end_date) in enumerate(arg_list):
            try:
                logger.info(f'({index + 1}/{total_codes}) Fetching data for i_code={i_code} from {start_date} to {end_date}')
                
                index_data_df = dgakuf.get_i_code_data(
                    ak_func_name, ak_cols_config_dict, i_code, period, start_date, end_date
                )

                if not index_data_df.empty:
                    all_data.append(index_data_df)
                    total_rows += len(index_data_df)
                    if DEBUG_MODE:
                        logger.debug(f'i_code={i_code}, len(index_data_df)={len(index_data_df)}, len(all_data)={len(all_data)}, total_rows={total_rows}')
                else:
                    failed_indexes.append(arg_list[index])

                if total_rows >= BATCH_SIZE or (index + 1) == total_codes:
                    _combined_df = pd.concat(all_data, ignore_index=True)
                    process_batch_data(ak_func_name, period, _combined_df, conn)
                    all_data = []
                    total_rows = 0

            except Exception as e:
                logger.error(f"Failed to process data for i_code={i_code}: {e}")
                failed_indexes.append(arg_list[index])

        if failed_indexes:
            dgakuf.write_list_to_redis(FAILED_INDEXES_CACHE_PREFIX, failed_indexes, task_cache_conn)
            logger.info(f"Failed indexes: {failed_indexes}")

    except Exception as e:
        logger.error(f"Failed to process data for {ak_func_name}: {e}")
        raise AirflowException(e)
    finally:
        if conn:
            PGEngine.release_conn(conn)

def retry_failed_indexes(ak_func_name: str, period: str):
    try:
        logger.info(f"Retrying failed indexes for {ak_func_name} with period={period}")
        failed_indexes = dgakuf.read_list_from_redis(FAILED_INDEXES_CACHE_PREFIX, task_cache_conn)
        if not failed_indexes:
            logger.info("No failed indexes to retry.")
            return

        logger.info(f"Failed indexes detected: {failed_indexes}")

        if failed_indexes:
            formatted_failed_indexes = "\n".join([str(index) for index in failed_indexes])
            logger.warning(f"Warning: There are failed indexes that need to be retried:\n{formatted_failed_indexes}")

    except Exception as e:
        logger.error(f"Failed to retry indexes for {ak_func_name}: {e}")
        raise AirflowException(e)

def generate_dag_name(index_func, period) -> str:
    period_mapping = {
        'daily': '日线',
        'weekly': '周线'
    }
    source_mapping = {
        'index_zh_a_hist': '东方财富'
    }
    return f"指数行情-{source_mapping.get(index_func, index_func)}-{period_mapping.get(period, period)}"

def generate_dag(index_func, period):
    logger.info(f"Generating DAG for {index_func} with period={period}")
    default_args = {
        'owner': con.DEFAULT_OWNER,
        'depends_on_past': False,
        'email': [con.DEFAULT_EMAIL],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': con.DEFAULT_RETRIES,
        'retry_delay': timedelta(minutes=con.DEFAULT_RETRY_DELAY)
    }

    dag_name = generate_dag_name(index_func, period)

    dag = DAG(
        dag_name,
        default_args=default_args,
        description=f'利用akshare的函数{index_func}(period={period})下载指数行情相关数据',
        start_date=days_ago(1),
        schedule=dgakuf.generate_random_minute_schedule(hour=8), # 北京时间: 8+8=16
        catchup=False,
        tags=['akshare', 'store_daily', '指数行情'],
        max_active_runs=1,
        params={},
    )

    tasks = {
        'prepare_arg_list': PythonOperator(
            task_id=f'prepare_arg_list_{index_func}_{period}',
            python_callable=prepare_arg_list,
            op_kwargs={'ak_func_name': index_func, 'period': period},
            dag=dag,
        ),
        'process_index_data': PythonOperator(
            task_id=f'process_index_data_{index_func}_{period}',
            python_callable=process_index_data,
            op_kwargs={'ak_func_name': index_func, 'period': period},
            dag=dag,
        ),
        'retry_failed_indexes': PythonOperator(
            task_id=f'retry_failed_indexes_{index_func}_{period}',
            python_callable=retry_failed_indexes,
            op_kwargs={'ak_func_name': index_func, 'period': period},
            dag=dag,
        ),
    }
    tasks['prepare_arg_list'] >> tasks['process_index_data'] >> tasks['retry_failed_indexes']
    return dag

def create_dags(ak_func_name, period):
    globals()[f'dg_ak_i_zh_a_{ak_func_name}_{period}'] = generate_dag(ak_func_name, period)
    logger.info(f"DAG for {ak_func_name} successfully created and registered.")

create_dags('index_zh_a_hist', 'daily')
