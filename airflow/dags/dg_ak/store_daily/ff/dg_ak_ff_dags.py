from __future__ import annotations

import os
import sys
from typing import List
import socket
import pandas as pd
from pathlib import Path
from datetime import timedelta, datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
import akshare as ak
import psycopg2.extras

from dags.dg_ak.utils.dg_ak_util_funcs import DgAkUtilFuncs as dgakuf
from dags.utils.db import PGEngine, task_cache_conn
from dags.utils.logger import logger
from dags.dg_ak.utils.dg_ak_config import dgak_config as con

DEBUG_MODE = con.DEBUG_MODE

config_path = Path(__file__).resolve().parent / 'dg_ak_ff_config.py'
sys.path.append(config_path.parent.as_posix())
ak_cols_config_dict = dgakuf.load_ak_cols_config(config_path.as_posix())

TRACING_TABLE_NAME_BY_SCODE_DATE = 'dg_ak_tracing_by_scode_date'

with PGEngine.managed_conn() as conn:
    td_list = dgakuf.get_trade_dates(conn)

if DEBUG_MODE and td_list and len(td_list)>0:
        today = max(td_list).strftime('%Y-%m-%d')
else:
    today = datetime.now().strftime('%Y-%m-%d')

def is_trading_day() -> str:
    with PGEngine.managed_conn() as conn:
        if dgakuf.is_trading_day(conn, date=datetime.strptime(today, '%Y-%m-%d').date()):
            return 'continue_task'
        else:
            return 'skip_task'
        
def update_tracing_by_scode_date_bulk(ak_func_name: str, updates: List[tuple]):
    conn = None
    try:
        with PGEngine.managed_conn() as conn:
            sql = f"""
                INSERT INTO {TRACING_TABLE_NAME_BY_SCODE_DATE} (ak_func_name, scode, last_td, create_time, update_time, host_name)
                VALUES %s
                ON CONFLICT (ak_func_name, scode) DO UPDATE 
                SET last_td = EXCLUDED.last_td, update_time = EXCLUDED.update_time, host_name = EXCLUDED.host_name;
            """
            hostname = os.getenv('HOSTNAME', socket.gethostname())
            values = [(ak_func_name, s_code, last_td, datetime.now(), datetime.now(), hostname) for s_code, last_td in updates]

            with conn.cursor() as cursor:
                psycopg2.extras.execute_values(cursor, sql, values)
            conn.commit()
            logger.info(f"Tracing data updated for {len(updates)} records in {TRACING_TABLE_NAME_BY_SCODE_DATE}.")
    except Exception as e:
        logger.error(f"Failed to update tracing table in bulk: {e}")
        raise AirflowException(e)

def process_batch_data(ak_func_name: str, df: pd.DataFrame):
    temp_csv_path = dgakuf.save_data_to_csv(df, f'{ak_func_name}')
    if temp_csv_path is None:
        raise AirflowException(f"No CSV file created for {ak_func_name}, skipping database insertion.")

    with PGEngine.managed_conn() as conn:
        dgakuf.insert_data_from_csv(conn, temp_csv_path, f'dg_ak_{ak_func_name}', task_cache_conn)

    last_td = df['td'].max()
    updates = [(_s_code, last_td) for _s_code in df['s_code'].unique()]
    update_tracing_by_scode_date_bulk(ak_func_name, updates)

def get_stock_individual_fund_flow():
    ak_func_name = "stock_individual_fund_flow"
    BATCH_SIZE = 5000
    logger.info("Fetching stock individual fund flow data")
    s_code_list = dgakuf.get_s_code_list(task_cache_conn)
    total_codes = len(s_code_list)
    all_data = []
    total_rows = 0

    for _index, _s_code in enumerate(s_code_list):
        try:
            logger.info(f'({_index + 1}/{total_codes}) downloading data with s_code={_s_code}')
            ak_func = getattr(ak, ak_func_name)
            if not ak_func:
                raise AirflowException(f"Function {ak_func_name} not found in akshare.")

            market = 'sh' if _s_code.startswith('6') else 'sz' if _s_code.startswith(('0', '3')) else 'bj' if _s_code.startswith(('4', '8')) else None
            data = dgakuf.try_to_call(ak_func, {'stock': _s_code, 'market': market})
            if data is None or data.empty:
                logger.warning(f"No data found for s_code {_s_code} using function {ak_func_name}. It may indicate that the stock is newly listed.")
                continue

            data = dgakuf.remove_cols(data, ak_cols_config_dict[ak_func_name])
            data.rename(columns=dgakuf.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
            data.dropna(inplace=True)
            data['s_code'] = _s_code
            data['td'] = data['td'].apply(dgakuf.format_td10)
            with PGEngine.managed_conn() as conn:
                data = dgakuf.convert_columns(data, f'dg_ak_{ak_func_name}', conn, task_cache_conn)
            all_data.append(data)
            total_rows += len(data)

            if total_rows >= BATCH_SIZE or (_index + 1) == total_codes:
                combined_df = pd.concat(all_data, ignore_index=True)
                process_batch_data(ak_func_name, combined_df)
                all_data = []
                total_rows = 0
        except Exception as e:
            logger.error(f"Failed to fetch data for s_code {_s_code} using function {ak_func_name}: {str(e)}")
            continue

def store_tracing_ak_data_today(ak_data_df, ak_func_name):
    with PGEngine.managed_conn() as conn:
        ak_data_df = dgakuf.convert_columns(ak_data_df, f'dg_ak_{ak_func_name}', conn, task_cache_conn)
        temp_csv_path = dgakuf.save_data_to_csv(ak_data_df, f'{ak_func_name}')
        if temp_csv_path is None:
            raise AirflowException(f"No CSV file created for {ak_func_name}, skipping database insertion.")
        dgakuf.insert_data_from_csv(conn, temp_csv_path, f'dg_ak_{ak_func_name}', task_cache_conn)
        dgakuf.insert_tracing_date_data(conn, ak_func_name, today)

def get_stock_individual_fund_flow_rank():
    ak_func_name = "stock_individual_fund_flow_rank"
    ak_data_df = dgakuf.try_to_call(lambda: ak.stock_individual_fund_flow_rank(indicator="今日"))
    ak_data_df = dgakuf.remove_cols(ak_data_df, ak_cols_config_dict[ak_func_name])
    ak_data_df.rename(columns=dgakuf.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
    ak_data_df['td'] = today
    store_tracing_ak_data_today(ak_data_df, ak_func_name)

def get_stock_market_fund_flow():
    ak_func_name = "stock_market_fund_flow"
    ak_data_df = dgakuf.get_data_by_none(ak_func_name, ak_cols_config_dict)
    store_tracing_ak_data_today(ak_data_df, ak_func_name)

def get_stock_sector_fund_flow_rank():
    ak_func_name = 'stock_sector_fund_flow_rank'
    sectors = ["行业资金流", "概念资金流", "地域资金流"]
    all_data = []
    for sector in sectors:
        data = dgakuf.try_to_call(lambda: ak.stock_sector_fund_flow_rank(indicator='今日', sector_type=sector))
        if data is not None:
            data = dgakuf.remove_cols(data, ak_cols_config_dict[ak_func_name])
            data.rename(columns=dgakuf.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
            data['sector_type'] = sector
            all_data.append(data)
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df['td'] = today
        store_tracing_ak_data_today(combined_df, ak_func_name)

def get_stock_main_fund_flow():
    ak_func_name = 'stock_main_fund_flow'
    ak_data_df = dgakuf.try_to_call(lambda: ak.stock_main_fund_flow(symbol="全部股票"))
    ak_data_df = dgakuf.remove_cols(ak_data_df, ak_cols_config_dict[ak_func_name])
    ak_data_df.rename(columns=dgakuf.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
    ak_data_df['td'] = today
    int_columns = ['today_rank', 'rank_5day', 'rank_10day']  
    for col in int_columns:
        if col in ak_data_df.columns:
            logger.debug(f'convert {col} type to int')
            ak_data_df[col] = ak_data_df[col].astype('Int64')
    store_tracing_ak_data_today(ak_data_df, ak_func_name)

def get_stock_sector_fund_flow_summary():
    ak_func_name = 'stock_sector_fund_flow_summary'
    with PGEngine.managed_conn() as conn:
        b_names, actual_date  = dgakuf.get_b_names_by_date(conn, "dg_ak_stock_board_industry_name_em", today)
    if actual_date is not today:
        logger.warning(f'Get b_names by {actual_date}(today is {today}).')
    all_data = []
    len_b_names = len(b_names)

    for index, b_name in enumerate(b_names):
        logger.info(f'({index + 1}/{len_b_names}) downloading data with b_name={b_name}')
        data = dgakuf.try_to_call(lambda: ak.stock_sector_fund_flow_summary(symbol=b_name, indicator='今日'))
        if data is not None:
            data = dgakuf.remove_cols(data, ak_cols_config_dict[ak_func_name])
            data.rename(columns=dgakuf.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
            data['b_name'] = b_name
            all_data.append(data)
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df['td'] = today
        store_tracing_ak_data_today(combined_df, ak_func_name)

def get_sector_f_hist(ak_func_name, b_name_table_name):
    with PGEngine.managed_conn() as conn:
        b_names, actual_date = dgakuf.get_b_names_by_date(conn, b_name_table_name, today)
    if actual_date is not today:
        logger.warning(f'Get b_names by {actual_date}(today is {today}).')
    b_con_df = dgakuf.get_data_by_board_names(ak_func_name, ak_cols_config_dict, b_names)
    store_tracing_ak_data_today(b_con_df, ak_func_name)

def get_stock_sector_fund_flow_hist():
    get_sector_f_hist("stock_sector_fund_flow_hist", "dg_ak_stock_board_industry_name_em")

def get_stock_concept_fund_flow_hist():
    get_sector_f_hist("stock_concept_fund_flow_hist", "dg_ak_stock_board_concept_name_em")

ak_func_name_mapping = {
    'stock_individual_fund_flow': ('个股资金流向', '东方财富'),
    'stock_individual_fund_flow_rank': ('个股资金流排名', '东方财富'),
    'stock_market_fund_flow': ('大盘资金流向', '东方财富'),
    'stock_sector_fund_flow_rank': ('行业资金流排名', '东方财富'),
    'stock_main_fund_flow': ('主力资金流', '东方财富'),
    'stock_sector_fund_flow_summary': ('行业资金流概况', '东方财富'),
    'stock_sector_fund_flow_hist': ('行业历史资金流', '东方财富'),
    'stock_concept_fund_flow_hist': ('概念历史资金流', '东方财富')
}

def generate_dag_name(ak_func_name: str) -> str:
    description, source = ak_func_name_mapping.get(ak_func_name, (ak_func_name, ak_func_name))
    return f'资金流向-{description}-{source}'

def generate_dag(ak_func_name: str, task_func):
    logger.info(f"Generating DAG for {ak_func_name}")
    default_args = {
        'owner': con.DEFAULT_OWNER,
        'depends_on_past': False,
        'start_date': days_ago(1),
        'email': [con.DEFAULT_EMAIL],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': con.DEFAULT_RETRIES,
        'retry_delay': timedelta(minutes=con.DEFAULT_RETRY_DELAY),
    }

    dag_name = generate_dag_name(ak_func_name)

    dag = DAG(
        dag_name,
        default_args=default_args,
        description=f'利用akshare的函数{ak_func_name}下载资金流向相关数据',
        schedule_interval='0 */12 * * *', 
        catchup=False,
        tags=['akshare', 'store_daily', '资金流向'],
        max_active_runs=1,
    )

    with dag:
        check_trading_day = BranchPythonOperator(
            task_id='check_trading_day',
            python_callable=is_trading_day,
            provide_context=True,
        )

        continue_task = DummyOperator(task_id='continue_task')
        skip_task = DummyOperator(task_id='skip_task')

        get_data_task = PythonOperator(
            task_id=f'get_data_{ak_func_name}',
            python_callable=task_func,
            dag=dag,
        )

        check_trading_day >> continue_task >> get_data_task
        check_trading_day >> skip_task

    return dag

ak_func_name_list = {
    'stock_individual_fund_flow': get_stock_individual_fund_flow,
    'stock_individual_fund_flow_rank': get_stock_individual_fund_flow_rank,
    'stock_market_fund_flow': get_stock_market_fund_flow,
    'stock_sector_fund_flow_rank': get_stock_sector_fund_flow_rank,
    'stock_main_fund_flow': get_stock_main_fund_flow,
    'stock_sector_fund_flow_summary': get_stock_sector_fund_flow_summary,
    'stock_sector_fund_flow_hist': get_stock_sector_fund_flow_hist,
    'stock_concept_fund_flow_hist': get_stock_concept_fund_flow_hist
}

for func_name, task_func in ak_func_name_list.items():
    try:
        dag_name = f'dg_ak_ff_{func_name}'
        globals()[dag_name] = generate_dag(func_name, task_func)
        logger.info(f"DAG for {dag_name} successfully created and registered.")
    except Exception as e:
        logger.error(f"Failed to create DAG for {func_name}: {str(e)}")
