from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
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
config_path = current_path / 'dg_ak_holders_config.py'
sys.path.append(config_path.parent.as_posix())
ak_cols_config_dict = dgakuf.load_ak_cols_config(config_path.as_posix())

TRACING_TABLE_NAME = 'dg_ak_tracing_by_date'

rollback_quarter = 4


def get_past_quarter_end_dates(year: int = 2017, end_day: datetime = datetime.today()) -> list:
    quarters_end_dates = [(3, 31), (6, 30), (9, 30), (12, 31)]
    return [
        datetime(y, month, day).strftime('%Y%m%d')
        for y in range(year, end_day.year + 1)
        for month, day in quarters_end_dates
        if datetime(y, month, day) <= end_day
    ]


def get_dates_based_on_tracing(conn, ak_func_name):
    qe_list = get_past_quarter_end_dates()
    qe_list_sorted = sorted(qe_list, reverse=True) 
    tracing_df = dgakuf.get_tracing_data_df(conn, TRACING_TABLE_NAME)

    if tracing_df.empty or ak_func_name not in tracing_df['ak_func_name'].values:
        return qe_list_sorted
    
    result = tracing_df[tracing_df['ak_func_name'] == ak_func_name]
    if result.empty:
        return qe_list_sorted
    
    last_td = result['last_td'].iloc[0]
    if isinstance(last_td, datetime):
        last_td = last_td.date()

    index = next(
        (i for i, date_str in enumerate(qe_list_sorted) if datetime.strptime(date_str, '%Y%m%d').date() < last_td),
        len(qe_list_sorted)
    )

    result_list = qe_list_sorted[:rollback_quarter + index + 1]
    if DEBUG_MODE:
        logger.debug(f'index={index}')
        logger.debug(f'last_td={last_td}')
        logger.debug(f'qe_list_sorted={result_list}')
    return result_list



def get_holders_data(pg_conn, ak_func_name, date_list, temp_dir=con.CACHE_ROOT):
    _ak_data_df = dgakuf.get_data_by_td_list(ak_func_name, ak_cols_config_dict, date_list)
    _ak_data_df = dgakuf.convert_columns(_ak_data_df, f'dg_ak_{ak_func_name}', pg_conn, task_cache_conn)
    _int_columns = ['cur_sh', 'pre_sh']  # Add other integer columns as needed
    for _col in _int_columns:
        if _col in _ak_data_df.columns:
            _ak_data_df[_col] = _ak_data_df[_col].astype('Int64')
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
            selected_date_list = get_dates_based_on_tracing(conn, ak_func_name)
            if DEBUG_MODE:
                logger.debug(f'td_list:{selected_date_list[:5]}')
            temp_csv_path = get_holders_data(conn, ak_func_name, selected_date_list)
            dgakuf.insert_data_from_csv(conn, temp_csv_path, f'dg_ak_{ak_func_name}', task_cache_conn)
            update_tracing_table(conn, ak_func_name, selected_date_list)

    except Exception as e:
        logger.error(f"Failed to get, store, and update data for {ak_func_name}: {str(e)}")
        raise AirflowException(e)

ak_func_name_mapping = {
    'stock_hold_num_cninfo': '股东人数'
}

def generate_dag_name(ak_func_name: str) -> str:
    description = ak_func_name_mapping.get(ak_func_name, ak_func_name)
    return f'股东研究-巨潮资讯-{description}'

def generate_dag(ak_func_name: str):
    logger.info(f"Generating DAG for {ak_func_name}")
    default_args = {
        'owner': con.DEFAULT_OWNER,
        'depends_on_past': False,
        'email': [con.DEFAULT_EMAIL],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': con.DEFAULT_RETRIES,
        'retry_delay': timedelta(minutes=con.DEFAULT_RETRY_DELAY)
    }

    dag_name = generate_dag_name(ak_func_name)

    dag = DAG(
        dag_name,
        default_args=default_args,
        description=f'利用akshare的函数{ak_func_name}下载股东相关数据',
        start_date=days_ago(0),
        schedule=dgakuf.generate_random_minute_schedule(hour=9),
        catchup=False,
        tags=['akshare', 'store_daily', '股东人数'],
        max_active_runs=1,
    )

    task = PythonOperator(
        task_id='get_holders',
        python_callable=get_store_and_update_data,
        op_kwargs={'ak_func_name': ak_func_name},
        dag=dag,
    )

    return dag

ak_func_name_list = [
    'stock_hold_num_cninfo'
]

for func_name in ak_func_name_list:
    dag_name = f'dg_ak_holders_{func_name}'
    globals()[dag_name] = generate_dag(func_name)
    logger.info(f"DAG for {dag_name} successfully created and registered.")
