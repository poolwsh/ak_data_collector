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

from dags.dg_ak.utils.dg_ak_util_funcs import DgAkUtilFuncs as dguf
from dags.utils.db import PGEngine, task_cache_conn
from dags.utils.logger import logger
from dags.dg_ak.utils.dg_ak_config import dgak_config as con

DEBUG_MODE = con.DEBUG_MODE

# pg_conn = PGEngine.get_conn()

config_path = Path(__file__).resolve().parent / 'dg_ak_ff_config.py'
sys.path.append(config_path.parent.as_posix())
ak_cols_config_dict = dguf.load_ak_cols_config(config_path.as_posix())

FF_DATE_LIST_KEY_PREFIX = "ff_date_list"
STORED_KEYS_KEY_PREFIX = "stored_keys"

TRACING_TABLE_NAME_BY_DATE = 'dg_ak_tracing_by_date'
TRACING_TABLE_NAME_BY_DATE_PARAM = 'dg_ak_tracing_by_date_1_param'
TRACING_TABLE_NAME_BY_SCODE_DATE = 'dg_ak_tracing_by_scode_date'


with PGEngine.managed_conn() as conn:
    td_list = dguf.get_trade_dates(conn)

if DEBUG_MODE and td_list and len(td_list)>0:
        today = max(td_list).strftime('%Y-%m-%d')
else:
    today = datetime.now().strftime('%Y-%m-%d')


def is_trading_day() -> str:
    with PGEngine.managed_conn() as conn:
        if dguf.is_trading_day(conn, date=datetime.strptime(today, '%Y-%m-%d').date()):
            return 'continue_task'
        else:
            return 'skip_task'

def fetch_and_process_data(func_name, ak_func, table_name, params=None, param_key=None, b_name=None):
    logger.info(f"Processing data for {func_name}")
    try:
        data = dguf.try_to_call(ak_func, params)
        if data is None or data.empty:
            logger.warning(f"No data found for {func_name}")
            return

        data = dguf.remove_cols(data, ak_cols_config_dict[func_name])
        data.rename(columns=dguf.get_col_dict(ak_cols_config_dict[func_name]), inplace=True)
        if b_name:
            data['b_name'] = b_name
        if 'td' not in data.columns:
            data['td'] = today
        else:
            data['td'] = data['td'].apply(dguf.format_td10)

        data.dropna(inplace=True)
        with PGEngine.managed_conn() as conn:
            data = dguf.convert_columns(data, table_name, conn, task_cache_conn)

        conflict_columns = ['td', 'b_name'] if table_name in [
            'dg_ak_stock_sector_fund_flow_rank', 'stock_sector_fund_flow_summary',
            'dg_ak_stock_sector_fund_flow_hist', 'dg_ak_stock_concept_fund_flow_hist'] else ['td', 's_code']
        if table_name == 'dg_ak_stock_market_fund_flow':
            conflict_columns = ['td']

        update_columns = [col for col in data.columns if col not in conflict_columns]
        dguf.insert_data_with_conflict_handling(data, table_name, conflict_columns, update_columns)

        if param_key:
            tracing_data = [(func_name, param_key, param_value, data['td'].max(), datetime.now(), datetime.now(), os.getenv('HOSTNAME', socket.gethostname())) for param_value in data[param_key].unique()]
            dguf.store_tracing_data(TRACING_TABLE_NAME_BY_DATE_PARAM, tracing_data)
        else:
            dguf.store_tracing_data(TRACING_TABLE_NAME_BY_DATE, [(func_name, data['td'].max(), datetime.now(), datetime.now(), os.getenv('HOSTNAME', socket.gethostname()))])
    except Exception as e:
        logger.error(f"Failed to process data for {func_name}: {str(e)}")
        raise AirflowException(e)



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
            values = [(ak_func_name, s_code, last_td, datetime.now(), hostname) for s_code, last_td in updates]

            with conn.cursor() as cursor:
                psycopg2.extras.execute_values(cursor, sql, values)
            conn.commit()
            logger.info(f"Tracing data updated for {len(updates)} records in {TRACING_TABLE_NAME_BY_SCODE_DATE}.")
    except Exception as e:
        logger.error(f"Failed to update tracing table in bulk: {e}")
        raise AirflowException(e)

def process_batch_data(ak_func_name: str, df: pd.DataFrame):
    temp_csv_path = dguf.save_data_to_csv(df, f'{ak_func_name}')
    if temp_csv_path is None:
        raise AirflowException(f"No CSV file created for {ak_func_name}, skipping database insertion.")

    with PGEngine.managed_conn() as conn:
        dguf.insert_data_from_csv(conn, temp_csv_path, f'dg_ak_{ak_func_name}', task_cache_conn)

    last_td = df['td'].max()
    updates = [(_s_code, last_td) for _s_code in df['s_code'].unique()]
    update_tracing_by_scode_date_bulk(ak_func_name, updates)

def get_stock_individual_fund_flow():
    ak_func_name = "stock_individual_fund_flow"
    BATCH_SIZE = 5000
    logger.info("Fetching stock individual fund flow data")
    s_code_list = dguf.get_s_code_list(task_cache_conn)
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
            data = dguf.try_to_call(ak_func, {'stock': _s_code, 'market': market})
            if data is None or data.empty:
                logger.warning(f"No data found for s_code {_s_code} using function {ak_func_name}. It may indicate that the stock is newly listed.")
                continue

            data = dguf.remove_cols(data, ak_cols_config_dict[ak_func_name])
            data.rename(columns=dguf.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
            data.dropna(inplace=True)
            data['s_code'] = _s_code
            data['td'] = data['td'].apply(dguf.format_td10)
            data = dguf.convert_columns(data, f'dg_ak_{ak_func_name}', pg_conn, task_cache_conn)
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


def get_stock_individual_fund_flow_rank():
    ak_func_name = "stock_individual_fund_flow_rank"
    df = dguf.try_to_call(lambda: ak.stock_individual_fund_flow_rank(indicator="今日"))
    df = dguf.remove_cols(df, ak_cols_config_dict[ak_func_name])
    df.rename(columns=dguf.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
    df['td'] = today
    desired_columns = [col[0] for col in dguf.get_columns_from_table(pg_conn, f'dg_ak_{ak_func_name}', task_cache_conn)]
    df = df[desired_columns]
    temp_csv_path = dguf.save_data_to_csv(df, f'{ak_func_name}')
    if temp_csv_path is None:
        raise AirflowException(f"No CSV file created for {ak_func_name}, skipping database insertion.")

    with PGEngine.managed_conn() as conn:
        dguf.insert_data_from_csv(conn, temp_csv_path, f'dg_ak_{ak_func_name}', task_cache_conn)
        dguf.insert_tracing_date_data(conn, ak_func_name, today)

def get_stock_market_fund_flow():
    # ak_func_name = "stock_market_fund_flow"
    # fetch_and_process_data("stock_market_fund_flow", lambda: dguf.get_data_today("stock_market_fund_flow", ak_cols_config_dict), 'dg_ak_stock_market_fund_flow')
    ak_func_name = "stock_market_fund_flow"
    df = dguf.get_data_by_none(ak_func_name, ak_cols_config_dict)
    desired_columns = [col[0] for col in dguf.get_columns_from_table(pg_conn, f'dg_ak_{ak_func_name}', task_cache_conn)]
    df = df[desired_columns]
    temp_csv_path = dguf.save_data_to_csv(df, f'{ak_func_name}')
    if temp_csv_path is None:
        raise AirflowException(f"No CSV file created for {ak_func_name}, skipping database insertion.")

    with PGEngine.managed_conn() as conn:
        dguf.insert_data_from_csv(conn, temp_csv_path, f'dg_ak_{ak_func_name}', task_cache_conn)
        dguf.insert_tracing_date_data(conn, ak_func_name, df['td'].max())


def get_stock_sector_fund_flow_rank():
    ak_func_name = 'stock_sector_fund_flow_rank'
    sectors = ["行业资金流", "概念资金流", "地域资金流"]
    all_data = []
    for sector in sectors:
        data = dguf.try_to_call(lambda: ak.stock_sector_fund_flow_rank(indicator='今日', sector_type=sector))
        if data is not None:
            data = dguf.remove_cols(data, ak_cols_config_dict[ak_func_name])
            data.rename(columns=dguf.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
            data['sector_type'] = sector
            all_data.append(data)
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df['td'] = today
        temp_csv_path = dguf.save_data_to_csv(combined_df, f'{ak_func_name}')
        if temp_csv_path is None:
            raise AirflowException(f"No CSV file created for {ak_func_name}, skipping database insertion.")

    with PGEngine.managed_conn() as conn:
        dguf.insert_data_from_csv(conn, temp_csv_path, f'dg_ak_{ak_func_name}', task_cache_conn)
        dguf.insert_tracing_date_data(conn, ak_func_name, today)

def get_stock_main_fund_flow():
    ak_func_name = 'stock_main_fund_flow'
    data = dguf.try_to_call(lambda: ak.stock_main_fund_flow(symbol="全部股票"))

    data = dguf.remove_cols(data, ak_cols_config_dict[ak_func_name])
    data.rename(columns=dguf.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
    data['td'] = today
    desired_columns = [col[0] for col in dguf.get_columns_from_table(pg_conn, f'dg_ak_{ak_func_name}', task_cache_conn)]
    data = data[desired_columns]
    int_columns = ['today_rank', 'rank_5day', 'rank_10day']  
    for col in int_columns:
        if col in data.columns:
            logger.debug(f'convert {col} type to int')
            data[col] = data[col].astype('Int64')
    temp_csv_path = dguf.save_data_to_csv(data, f'{ak_func_name}')
    if temp_csv_path is None:
        raise AirflowException(f"No CSV file created for {ak_func_name}, skipping database insertion.")

    with PGEngine.managed_conn() as conn:
        dguf.insert_data_from_csv(conn, temp_csv_path, f'dg_ak_{ak_func_name}', task_cache_conn)
        dguf.insert_tracing_date_data(conn, ak_func_name, today)

def get_stock_sector_fund_flow_summary():
    ak_func_name = 'stock_sector_fund_flow_summary'
    b_names = dguf.get_b_names_from_date(pg_conn, "dg_ak_stock_board_industry_name_em", today)
    all_data = []
    len_b_names = len(b_names)

    for index, b_name in enumerate(b_names):
        logger.info(f'({index + 1}/{len_b_names}) downloading data with b_name={b_name}')
        data = dguf.try_to_call(lambda: ak.stock_sector_fund_flow_summary(symbol=b_name, indicator='今日'))
        if data is not None:
            data = dguf.remove_cols(data, ak_cols_config_dict[ak_func_name])
            data.rename(columns=dguf.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
            data['b_name'] = b_name
            all_data.append(data)
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df['td'] = today
        temp_csv_path = dguf.save_data_to_csv(combined_df, f'{ak_func_name}')
        if temp_csv_path is None:
            raise AirflowException(f"No CSV file created for {ak_func_name}, skipping database insertion.")
    
    with PGEngine.managed_conn() as conn:
        dguf.insert_data_from_csv(conn, temp_csv_path, f'dg_ak_{ak_func_name}', task_cache_conn)
        dguf.insert_tracing_date_data(conn, ak_func_name, today)


def get_sector_f_hist(ak_func_name, b_name_table_name):
    with PGEngine.managed_conn() as conn:
        b_names = dguf.get_b_names_from_date(conn, b_name_table_name, today)
        all_data = []
        for b_name in b_names:
            data = dguf.try_to_call(lambda: ak.stock_sector_fund_flow_hist(symbol=b_name))
            if data is not None:
                data = dguf.remove_cols(data, ak_cols_config_dict[ak_func_name])
                data.rename(columns=dguf.get_col_dict(ak_cols_config_dict[ak_func_name]), inplace=True)
                data['b_name'] = b_name
                all_data.append(data)
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            temp_csv_path = dguf.save_data_to_csv(combined_data, f'{ak_func_name}')
            dguf.insert_data_from_csv(conn, temp_csv_path, f'dg_ak_{ak_func_name}', task_cache_conn)
            dguf.insert_tracing_date_data(conn, ak_func_name, combined_data['td'].max())


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
        'owner': 'wsh',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'email': ['wshmxgz@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    dag_name = generate_dag_name(ak_func_name)

    dag = DAG(
        dag_name,
        default_args=default_args,
        description=f'利用akshare的函数{ak_func_name}下载资金流向相关数据',
        schedule_interval='0 */8 * * *',  # 每8小时运行一次
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
