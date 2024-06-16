from __future__ import annotations

import os
import sys
from datetime import timedelta
from dags.dg_ak.utils.dg_ak_util_funcs import DgAkUtilFuncs as dgakuf
from dags.utils.db import PGEngine, task_cache_conn
from dags.utils.logger import logger
from dags.dg_ak.utils.dg_ak_config import dgak_config as con
from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DEBUG_MODE = con.DEBUG_MODE

from pathlib import Path
current_path = Path(__file__).resolve().parent 
config_path = current_path / 'dg_ak_zdt_config.py'
sys.path.append(config_path.parent.as_posix())
ak_cols_config_dict = dgakuf.load_ak_cols_config(config_path.as_posix())

TRACING_TABLE_NAME = 'dg_ak_tracing_by_date'

default_trade_dates = 50
rollback_days = 10

def get_date_list(conn, ak_func_name):
    tracing_df = dgakuf.get_tracing_data_df(conn, TRACING_TABLE_NAME)
    result = tracing_df[tracing_df['ak_func_name'] == ak_func_name]
    if not result.empty:
        last_td = result['last_td'].iloc[0]
        if DEBUG_MODE:
            logger.debug(f"Last trading date for {ak_func_name}: {last_td}")
        selected_dates = dgakuf.get_selected_trade_dates(conn, rollback_days, last_td)
    else:
        selected_dates = dgakuf.get_selected_trade_dates(conn, default_trade_dates, None)
    if DEBUG_MODE:
        logger.debug(f'Length of selected_dates: {len(selected_dates)}')
        logger.debug(f'selected_dates: {selected_dates}')

    selected_dates = [dgakuf.format_td8(date) for date in selected_dates]
    return selected_dates

def get_zdt_data(pg_conn, ak_func_name, date_list):
    _ak_data_df = dgakuf.get_data_by_td_list(ak_func_name, ak_cols_config_dict, date_list)
    _ak_data_df = dgakuf.convert_columns(_ak_data_df, f'dg_ak_{ak_func_name}', pg_conn, task_cache_conn)
    _temp_csv_path = dgakuf.save_data_to_csv(_ak_data_df, f'{ak_func_name}')
    if _temp_csv_path is None:
        raise AirflowException(f"No CSV file created for {ak_func_name}, skipping database insertion.")
    return _temp_csv_path

def update_tracing_table(conn, ak_func_name, date_list):
    max_td = max(date_list)
    max_td = dgakuf.format_td10(max_td)
    if DEBUG_MODE:
        logger.debug(f'max_td={max_td}')
    dgakuf.insert_tracing_date_data(conn, ak_func_name, max_td)

def get_store_and_update_data(ak_func_name: str):
    logger.info(f"Starting to get, store, and update data for {ak_func_name}")
    try:
        with PGEngine.managed_conn() as conn:
            selected_date_list = get_date_list(conn, ak_func_name)
            temp_csv_path = get_zdt_data(conn, ak_func_name, selected_date_list)
            dgakuf.insert_data_from_csv(conn, temp_csv_path, f'dg_ak_{ak_func_name}', task_cache_conn)
            update_tracing_table(conn, ak_func_name, selected_date_list)
    except Exception as e:
        logger.error(f"Failed to get, store, and update data for {ak_func_name}: {str(e)}")

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
        'owner': con.DEFAULT_OWNER,
        'depends_on_past': False,
        'email': [con.DEFAULT_EMAIL],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': con.RETRIES,
        'retry_delay': timedelta(minutes=con.DEFAULT_RETRY_DELAY)
    }

    dag_name = generate_dag_name(ak_func_name)

    dag = DAG(
        dag_name,
        default_args=default_args,
        description=f'利用akshare的函数{ak_func_name}下载涨跌停相关数据',
        start_date=days_ago(1),
        schedule=dgakuf.generate_random_minute_schedule(hour=9), # 北京时间: 9+8=17
        catchup=False,
        tags=['akshare', 'store_daily', '涨跌停'],
        max_active_runs=1,
    )

    task = PythonOperator(
        task_id='get_zdt_data',
        python_callable=get_store_and_update_data,
        op_kwargs={'ak_func_name': ak_func_name},
        dag=dag,
    )

    return dag

ak_func_name_list = [
    'stock_zt_pool_em', 'stock_zt_pool_previous_em', 'stock_zt_pool_strong_em', 
    'stock_zt_pool_sub_new_em', 'stock_zt_pool_zbgc_em', 'stock_zt_pool_dtgc_em'
]

for func_name in ak_func_name_list:
    dag_name = f'dg_ak_zdt_{func_name}'
    globals()[dag_name] = generate_dag(func_name)
    logger.info(f"DAG for {dag_name} successfully created and registered.")
