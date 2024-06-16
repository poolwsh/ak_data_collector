from __future__ import annotations
import os
import sys

import random
import pandas as pd
from datetime import timedelta, datetime
from dags.dg_ak.utils.dg_ak_util_funcs import DgAkUtilFuncs as dgakuf
from dags.utils.db import PGEngine, task_cache_conn
from dags.utils.logger import logger
from dags.dg_ak.utils.dg_ak_config import dgak_config as con

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

DEBUG_MODE = con.DEBUG_MODE

from pathlib import Path
current_path = Path(__file__).resolve().parent 
config_path = current_path / 'dg_ak_board_config.py'
sys.path.append(config_path.parent.as_posix())
ak_cols_config_dict = dgakuf.load_ak_cols_config(config_path.as_posix())

BOARD_LIST_KEY_PREFIX = "board_list"

def get_redis_key(base_key: str, identifier: str) -> str:
    return f"{base_key}@{identifier}"

def download_and_store_board_list(board_list_func_name: str):
    logger.info(f"Starting combined operations for downloading and storing board list for {board_list_func_name}")
    try:
        board_list_data_df = dgakuf.get_data_today(board_list_func_name, ak_cols_config_dict)
        if 'td' in board_list_data_df.columns:
            board_list_data_df['td'] = pd.to_datetime(board_list_data_df['td'], errors='coerce').dt.strftime('%Y-%m-%d')
        if board_list_data_df.empty:
            raise AirflowException(f"No data retrieved for {board_list_func_name}")
        if DEBUG_MODE:
            logger.debug(f'length of board_list_data_df: {len(board_list_data_df)}')
            logger.debug(f'head 5 of board_list_data_df:')
            logger.debug(board_list_data_df.head(5))

        with PGEngine.managed_conn() as conn:
            board_list_data_df = dgakuf.convert_columns(board_list_data_df, f'dg_ak_{board_list_func_name}', conn, task_cache_conn)
            redis_key = get_redis_key(BOARD_LIST_KEY_PREFIX, board_list_func_name)
            dgakuf.write_df_to_redis(redis_key, board_list_data_df, task_cache_conn, con.DEFAULT_REDIS_TTL)
            temp_csv_path = dgakuf.save_data_to_csv(board_list_data_df, f'{board_list_func_name}')
            if temp_csv_path is None:
                raise AirflowException(f"No CSV file created for {board_list_func_name}, skipping database insertion.")
            dgakuf.insert_data_from_csv(conn, temp_csv_path, f'dg_ak_{board_list_func_name}', task_cache_conn)
            dgakuf.insert_tracing_date_data(conn, board_list_func_name, board_list_data_df['td'].max())

    except Exception as e:
        logger.error(f"Failed during combined operations for {board_list_func_name}: {str(e)}")
        conn.rollback()
        raise AirflowException(e)

def download_and_store_board_cons(board_list_func_name: str, board_cons_func_name: str, param_name: str):
    logger.info(f"Starting combined operations for downloading and storing board constituents for {board_cons_func_name}")

    try:
        redis_key = get_redis_key(BOARD_LIST_KEY_PREFIX, board_list_func_name)
        board_list_df = dgakuf.read_df_from_redis(redis_key, task_cache_conn)
        if board_list_df.empty:
            raise AirflowException(f"No dates available for {board_list_func_name}, skipping data fetch.")
        board_cons_df = dgakuf.get_data_by_board_names(board_cons_func_name, ak_cols_config_dict, board_list_df['b_name'])
        board_cons_df['s_code'] = board_cons_df['s_code'].astype(str) 
        today_date = datetime.now().strftime('%Y-%m-%d')
        board_cons_df['td'] = today_date
        if 'td' in board_cons_df.columns:
            board_cons_df['td'] = pd.to_datetime(board_cons_df['td'], errors='coerce').dt.strftime('%Y-%m-%d')

        with PGEngine.managed_conn() as conn:
            board_cons_df = dgakuf.convert_columns(board_cons_df, f'dg_ak_{board_cons_func_name}', conn, task_cache_conn)

            temp_csv_path = dgakuf.save_data_to_csv(board_cons_df, f'{board_cons_func_name}')
            if temp_csv_path is None:
                raise AirflowException(f"No CSV file created for {board_cons_func_name}, skipping database insertion.")
            dgakuf.insert_data_from_csv(conn, temp_csv_path, f'dg_ak_{board_cons_func_name}', task_cache_conn)
            dgakuf.insert_tracing_date_data(conn, board_cons_func_name, board_cons_df['td'].max())
    except Exception as e:
        logger.error(f"Failed during combined operations for {board_cons_func_name}: {str(e)}")
        conn.rollback()
        raise AirflowException(e)

def generate_dag_name(board_list_func_name: str, board_cons_func_name: str) -> str:
    type_mapping = {
        'concept': '概念板块',
        'industry': '行业板块'
    }
    source_mapping = {
        'ths': '同花顺',
        'em': '东方财富'
    }
    board_type = board_list_func_name.split('_')[2]  # 获取第三部分
    source = board_list_func_name.split('_')[-1]  # 获取最后一部分
    return f"板块-{source_mapping.get(source, source)}-{type_mapping.get(board_type, board_type)}"

def is_trading_day(**kwargs) -> str:
    try:
        with PGEngine.managed_conn() as conn:
            trade_dates = dgakuf.get_trade_dates(conn)
            trade_dates.sort(reverse=True)
            if DEBUG_MODE:
                today_str = max(trade_dates).strftime('%Y-%m-%d')
            else:
                today_str = datetime.now().strftime('%Y-%m-%d')
            today = datetime.strptime(today_str, '%Y-%m-%d').date()

            if DEBUG_MODE:
                logger.debug(f'today:{today}')
                logger.debug(f'first 5 trade_dates: {trade_dates[:5]}')
            if today in trade_dates:
                return 'continue_task'
            else:
                return 'skip_task'
    except Exception as e:
        logger.error(f"Failed during data operations: {str(e)}")
        raise AirflowException(e)

def generate_dag(board_list_func_name: str, board_cons_func_name: str):
    logger.info(f"Generating DAG for {board_list_func_name} and {board_cons_func_name}")
    default_args = {
        'owner': con.DEFAULT_OWNER,
        'depends_on_past': False,
        'email': [con.DEFAULT_EMAIL],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': con.DEFAULT_RETRIES,
        'retry_delay': timedelta(minutes=con.DEFAULT_RETRY_DELAY)
    }

    dag_name = generate_dag_name(board_list_func_name, board_cons_func_name)

    dag = DAG(
        dag_name,
        default_args=default_args,
        start_date=datetime(2024, 5, 22, 14, random.randint(0, 59)),  # 设置首次运行时间为2024年5月22日14点随机分钟
        description=f'利用akshare的函数{board_list_func_name}和{board_cons_func_name}下载板块相关数据',
        schedule_interval='0 */3 * * *',  # 每3小时运行一次
        catchup=False,
        tags=['akshare', 'store_daily', '板块'],
        max_active_runs=1,
        params={},
    )

    with dag:
        check_trading_day = BranchPythonOperator(
            task_id='check_trading_day',
            python_callable=is_trading_day,
            provide_context=True,
        )

        continue_task = DummyOperator(task_id='continue_task')
        skip_task = DummyOperator(task_id='skip_task')

        download_and_store_board_list_task = PythonOperator(
            task_id=f'download_and_store_board_list-{board_list_func_name}',
            python_callable=download_and_store_board_list,
            op_kwargs={'board_list_func_name': board_list_func_name},
        )

        download_and_store_board_cons_task = PythonOperator(
            task_id=f'download_and_store_board_cons-{board_cons_func_name}',
            python_callable=download_and_store_board_cons,
            op_kwargs={'board_list_func_name': board_list_func_name, 'board_cons_func_name': board_cons_func_name, 'param_name': 'symbol'},
        )

        check_trading_day >> continue_task >> download_and_store_board_list_task >> download_and_store_board_cons_task
        check_trading_day >> skip_task

    return dag

ak_func_name_list = [
    # ['stock_board_concept_name_ths', 'stock_board_concept_cons_ths'],
    ['stock_board_concept_name_em', 'stock_board_concept_cons_em'],
    # ['stock_board_industry_summary_ths', 'stock_board_industry_cons_ths'],
    ['stock_board_industry_name_em', 'stock_board_industry_cons_em']
]

for func_names in ak_func_name_list:
    words = func_names[0].split('_')
    source = words[-1]
    board_type = words[-3]
    dag_name = f'dg_ak_board_{board_type}_{source}'
    globals()[dag_name] = generate_dag(func_names[0], func_names[1])
    logger.info(f"DAG for {dag_name} successfully created and registered.")
