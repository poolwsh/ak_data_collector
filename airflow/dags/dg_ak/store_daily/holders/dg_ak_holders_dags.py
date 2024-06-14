from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from dags.dg_ak.utils.dg_ak_util_funcs import DgAkUtilFuncs as dguf
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
ak_cols_config_dict = dguf.load_ak_cols_config(config_path.as_posix())

TRACING_TABLE_NAME = 'dg_ak_tracing_by_date'

default_trade_dates = 50
rollback_quarter = 2


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
    tracing_df = dguf.get_tracing_data_df(conn, TRACING_TABLE_NAME)
    if tracing_df.empty or ak_func_name not in tracing_df['ak_func_name'].values:
        return qe_list
    
    result = tracing_df[tracing_df['ak_func_name'] == ak_func_name]
    if result.empty:
        return qe_list
    
    last_td = result['last_td'].iloc[0]
    index = next(
        (i for i, date_str in enumerate(qe_list) if datetime.strptime(date_str, '%Y%m%d') > last_td),
        len(qe_list)
    ) - 1
    return qe_list[:rollback_quarter + index + 1]

def get_holders_data(pg_conn, ak_func_name, date_list, temp_dir=con.CACHE_ROOT):
    _ak_data_df = dguf.get_data_by_td_list(ak_func_name, ak_cols_config_dict, date_list)
    
    _desired_columns = [col[0] for col in dguf.get_columns_from_table(pg_conn, f'dg_ak_{ak_func_name}', task_cache_conn)]
    try:
        _ak_data_df = _ak_data_df[_desired_columns]
    except KeyError as e:
        logger.error(f"KeyError while selecting columns for {ak_func_name}: {str(e)}")
        raise
    os.makedirs(temp_dir, exist_ok=True)
    _temp_csv_path = os.path.join(temp_dir, f'{ak_func_name}.csv')
    _ak_data_df.to_csv(_temp_csv_path, index=False, header=False)

    if DEBUG_MODE:
        logger.debug(f"Data saved to CSV at {_temp_csv_path}, length: {len(_ak_data_df)}, first 5 rows: {_ak_data_df.head().to_dict(orient='records')}")
    return _temp_csv_path

def update_tracing_table(conn, ak_func_name, date_list):
    max_td = max(date_list)
    max_td = dguf.format_td10(max_td)
    if DEBUG_MODE:
        logger.debug(f'max_td={max_td}')
    dguf.insert_tracing_date_data(conn, ak_func_name, max_td)

def get_store_and_update_data(ak_func_name: str):
    logger.info(f"Starting to get, store, and update data for {ak_func_name}")
    conn = None
    try:
        conn = PGEngine.get_conn()
        if not conn:
            raise AirflowException("Failed to get database connection")

        selected_date_list = get_dates_based_on_tracing(conn, ak_func_name)
        temp_csv_path = get_holders_data(conn, ak_func_name, selected_date_list)
        dguf.insert_data_from_csv(conn, temp_csv_path, f'dg_ak_{ak_func_name}', task_cache_conn)
        update_tracing_table(conn, ak_func_name, selected_date_list)

    except Exception as e:
        logger.error(f"Failed to get, store, and update data for {ak_func_name}: {str(e)}")
        raise AirflowException(e)
    finally:
        if conn:
            PGEngine.release_conn(conn)

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
        'retries': con.RETRIES,
        'retry_delay': timedelta(minutes=con.DEFAULT_RETRY_DELAY)
    }

    dag_name = generate_dag_name(ak_func_name)

    dag = DAG(
        dag_name,
        default_args=default_args,
        description=f'利用akshare的函数{ak_func_name}下载股东相关数据',
        start_date=days_ago(1),
        schedule=dguf.generate_random_minute_schedule(hour=9), # 北京时间: 9+8=17
        catchup=False,
        tags=['akshare', 'store_daily', '股东人数'],
        max_active_runs=1,
    )

    task = PythonOperator(
        task_id='get_data',
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
