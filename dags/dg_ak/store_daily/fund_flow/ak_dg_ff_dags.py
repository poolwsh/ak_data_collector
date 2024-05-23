from __future__ import annotations

import os
import sys
import pandas as pd
from pathlib import Path
from datetime import timedelta, datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from airflow.operators.dummy import DummyOperator

from dg_ak.utils.util_funcs import UtilFuncs as uf
from dg_ak.utils.logger import logger
import dg_ak.utils.config as con

# 配置日志调试开关
LOGGER_DEBUG = con.LOGGER_DEBUG

# 配置数据库连接
redis_hook = RedisHook(redis_conn_id=con.REDIS_CONN_ID)
pgsql_hook = PostgresHook(postgres_conn_id=con.TXY800_PGSQL_CONN_ID)
pg_conn = pgsql_hook.get_conn()

# 配置路径
config_path = Path(__file__).resolve().parent / 'ak_dg_ff_config.py'
sys.path.append(config_path.parent.as_posix())
ak_cols_config_dict = uf.load_ak_cols_config(config_path.as_posix())

# 统一定义Redis keys
FF_DATE_LIST_KEY_PREFIX = "ff_date_list"
STORED_KEYS_KEY_PREFIX = "stored_keys"

TRACING_TABLE_NAME = 'ak_dg_tracing_fund_flow'
TRADE_DATE_TABLE_NAME = 'ak_dg_stock_zh_a_trade_date'

def is_trading_day(**kwargs) -> str:
    today_str = datetime.now().strftime('%Y-%m-%d')
    today = datetime.strptime(today_str, '%Y-%m-%d').date()
    trade_dates = uf.get_trade_dates(pg_conn)
    trade_dates.sort(reverse=True)
    if LOGGER_DEBUG:
        logger.debug(f'today:{today}')
        logger.debug(f'first 5 trade_dates: {trade_dates[:5]}')
    if today in trade_dates:
        return 'continue_task'
    else:
        return 'skip_task'

def get_data_by_s_code(func_name: str, s_code: str):
    return uf.get_data_today(func_name, ak_cols_config_dict, s_code=s_code)

def get_stock_individual_fund_flow():
    logger.info("Fetching stock individual fund flow data")
    func_name = "stock_individual_fund_flow"
    try:
        s_code_list = uf.get_s_code_list(pg_conn)
        if LOGGER_DEBUG:
            logger.debug(f's_code_list: {s_code_list}')
        all_data = []
        for s_code in s_code_list:
            data = get_data_by_s_code(func_name, s_code)
            if not data.empty:
                all_data.append(data)

        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            combined_data = uf.convert_columns(combined_data, f'ak_dg_{func_name}', pg_conn, redis_hook.get_conn())

            if 'td' in combined_data.columns:
                combined_data['td'] = pd.to_datetime(combined_data['td'], errors='coerce').dt.strftime('%Y-%m-%d')

            temp_csv_path = uf.save_data_to_csv(combined_data, func_name)
            if temp_csv_path is None:
                raise AirflowException(f"No CSV file created for {func_name}, skipping database insertion.")
            
            uf.insert_data_from_csv(pg_conn, temp_csv_path, f'ak_dg_{func_name}')

            # 记录tracing信息
            tracing_data = [(func_name, s_code, datetime.now().date(), datetime.now(), datetime.now(), os.getenv('HOSTNAME', socket.gethostname())) for s_code in s_code_list]
            store_tracing_data('ak_dg_tracing_by_scode_date', tracing_data)
    except Exception as e:
        logger.error(f"Failed to fetch data for {func_name}: {str(e)}")
        raise AirflowException(e)

def get_fund_flow_data_by_func(func_name: str, func_call: str, sector_type: str = None):
    logger.info(f"Fetching fund flow data for {func_name}")
    try:
        data = uf.try_to_call(func_call)
        if sector_type:
            data['sector_type'] = sector_type
        data = uf.convert_columns(data, f'ak_dg_{func_name}', pg_conn, redis_hook.get_conn())

        if 'td' in data.columns:
            data['td'] = pd.to_datetime(data['td'], errors='coerce').dt.strftime('%Y-%m-%d')

        temp_csv_path = uf.save_data_to_csv(data, func_name)
        if temp_csv_path is None:
            raise AirflowException(f"No CSV file created for {func_name}, skipping database insertion.")
        
        uf.insert_data_from_csv(pg_conn, temp_csv_path, f'ak_dg_{func_name}')
    except Exception as e:
        logger.error(f"Failed to fetch data for {func_name}: {str(e)}")
        raise AirflowException(e)

def store_tracing_data(table_name: str, data: list):
    insert_sql = f"""
        INSERT INTO {table_name} (ak_func_name, last_td, create_time, update_time, host_name)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (ak_func_name) DO UPDATE 
        SET last_td = EXCLUDED.last_td, update_time = EXCLUDED.update_time;
    """
    try:
        cursor = pg_conn.cursor()
        cursor.executemany(insert_sql, data)
        pg_conn.commit()
        cursor.close()
        if LOGGER_DEBUG:
            logger.debug(f"Inserted tracing data: {data}")
    except Exception as e:
        logger.error(f"Failed to insert tracing data into {table_name}: {str(e)}")
        pg_conn.rollback()
        raise AirflowException(e)

def get_stock_individual_fund_flow_rank():
    logger.info("Fetching stock individual fund flow rank data")
    func_name = "stock_individual_fund_flow_rank"
    try:
        data = uf.try_to_call("ak.stock_individual_fund_flow_rank(indicator='今日')")
        if LOGGER_DEBUG:
            logger.debug(f"Fetched data: {data.head()}")
        if data.empty:
            return
        data = uf.convert_columns(data, f'ak_dg_{func_name}', pg_conn, redis_hook.get_conn())

        if 'td' in data.columns:
            data['td'] = pd.to_datetime(data['td'], errors='coerce').dt.strftime('%Y-%m-%d')

        temp_csv_path = uf.save_data_to_csv(data, func_name)
        if temp_csv_path is None:
            raise AirflowException(f"No CSV file created for {func_name}, skipping database insertion.")
        
        uf.insert_data_from_csv(pg_conn, temp_csv_path, f'ak_dg_{func_name}')

        store_tracing_data('ak_dg_tracing_by_date', [(func_name, datetime.now(), datetime.now(), datetime.now(), os.getenv('HOSTNAME', socket.gethostname()))])
    except Exception as e:
        logger.error(f"Failed to fetch data for {func_name}: {str(e)}")
        raise AirflowException(e)

def get_stock_market_fund_flow():
    logger.info("Fetching stock market fund flow data")
    func_name = "stock_market_fund_flow"
    try:
        data = uf.get_data_today(func_name, ak_cols_config_dict)
        if LOGGER_DEBUG:
            logger.debug(f"Fetched data: {data.head()}")
        if data.empty:
            return
        data = uf.convert_columns(data, f'ak_dg_{func_name}', pg_conn, redis_hook.get_conn())

        if 'td' in data.columns:
            data['td'] = pd.to_datetime(data['td'], errors='coerce').dt.strftime('%Y-%m-%d')

        temp_csv_path = uf.save_data_to_csv(data, func_name)
        if temp_csv_path is None:
            raise AirflowException(f"No CSV file created for {func_name}, skipping数据库insertion.")
        
        uf.insert_data_from_csv(pg_conn, temp_csv_path, f'ak_dg_{func_name}')

        store_tracing_data('ak_dg_tracing_by_date', [(func_name, datetime.now(), datetime.now(), datetime.now(), os.getenv('HOSTNAME', socket.gethostname()))])
    except Exception as e:
        logger.error(f"Failed to fetch data for {func_name}: {str(e)}")
        raise AirflowException(e)

def get_stock_sector_fund_flow_rank():
    logger.info("Fetching stock sector fund flow rank data")
    func_name = "stock_sector_fund_flow_rank"
    sector_types = ["行业资金流", "概念资金流", "地域资金流"]
    try:
        for sector_type in sector_types:
            func_call = f"ak.stock_sector_fund_flow_rank(indicator='今日', sector_type='{sector_type}')"
            get_fund_flow_data_by_func(func_name, func_call, sector_type=sector_type)

        store_tracing_data('ak_dg_tracing_by_date', [(func_name, datetime.now(), datetime.now(), datetime.now(), os.getenv('HOSTNAME', socket.gethostname()))])
    except Exception as e:
        logger.error(f"Failed to fetch data for {func_name}: {str(e)}")
        raise AirflowException(e)

def get_stock_main_fund_flow():
    logger.info("Fetching stock main fund flow data")
    func_name = "stock_main_fund_flow"
    try:
        func_call = "ak.stock_main_fund_flow(symbol='全部股票')"
        get_fund_flow_data_by_func(func_name, func_call)

        store_tracing_data('ak_dg_tracing_by_date', [(func_name, datetime.now(), datetime.now(), datetime.now(), os.getenv('HOSTNAME', socket.gethostname()))])
    except Exception as e:
        logger.error(f"Failed to fetch data for {func_name}: {str(e)}")
        raise AirflowException(e)

def get_stock_sector_fund_flow_summary():
    logger.info("Fetching stock sector fund flow summary data")
    func_name = "stock_sector_fund_flow_summary"
    try:
        symbols = uf.get_b_names_from_table(pg_conn, "ak_dg_stock_board_industry_name_em_store")
        if LOGGER_DEBUG:
            logger.debug(f"symbols: {symbols}")
        all_data = []
        for symbol in symbols:
            func_call = f"ak.stock_sector_fund_flow_summary(symbol='{symbol}', indicator='今日')"
            data = uf.try_to_call(func_call)
            if not data.empty:
                all_data.append(data)
        
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            combined_data = uf.convert_columns(combined_data, f'ak_dg_{func_name}', pg_conn, redis_hook.get_conn())

            if 'td' in combined_data.columns:
                combined_data['td'] = pd.to_datetime(combined_data['td'], errors='coerce').dt.strftime('%Y-%m-%d')

            temp_csv_path = uf.save_data_to_csv(combined_data, func_name)
            if temp_csv_path is None:
                raise AirflowException(f"No CSV file created for {func_name}, skipping database insertion.")
            
            uf.insert_data_from_csv(pg_conn, temp_csv_path, f'ak_dg_{func_name}')
            
            store_tracing_data('ak_dg_tracing_by_date', [(func_name, datetime.now(), datetime.now(), datetime.now(), os.getenv('HOSTNAME', socket.gethostname()))])
    except Exception as e:
        logger.error(f"Failed to fetch data for {func_name}: {str(e)}")
        raise AirflowException(e)

def get_stock_sector_fund_flow_hist():
    logger.info("Fetching stock sector fund flow hist data")
    func_name = "stock_sector_fund_flow_hist"
    try:
        symbols = uf.get_b_names_from_table(pg_conn, "ak_dg_stock_board_industry_name_em_store")
        if LOGGER_DEBUG:
            logger.debug(f"symbols: {symbols}")
        all_data = []
        for symbol in symbols:
            func_call = f"ak.stock_sector_fund_flow_hist(symbol='{symbol}')"
            data = uf.try_to_call(func_call)
            if not data.empty:
                all_data.append(data)
        
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            combined_data = uf.convert_columns(combined_data, f'ak_dg_{func_name}', pg_conn, redis_hook.get_conn())

            if 'td' in combined_data.columns:
                combined_data['td'] = pd.to_datetime(combined_data['td'], errors='coerce').dt.strftime('%Y-%m-%d')

            temp_csv_path = uf.save_data_to_csv(combined_data, func_name)
            if temp_csv_path is None:
                raise AirflowException(f"No CSV file created for {func_name}, skipping database insertion.")
            
            uf.insert_data_from_csv(pg_conn, temp_csv_path, f'ak_dg_{func_name}')
            
            tracing_data = [(func_name, 'symbol', symbol, combined_data['td'].max().date(), datetime.now(), datetime.now(), os.getenv('HOSTNAME', socket.gethostname())) for symbol in symbols]
            store_tracing_data('ak_dg_tracing_by_date_1_param', tracing_data)
    except Exception as e:
        logger.error(f"Failed to fetch data for {func_name}: {str(e)}")
        raise AirflowException(e)

def get_stock_concept_fund_flow_hist():
    logger.info("Fetching stock concept fund flow hist data")
    func_name = "stock_concept_fund_flow_hist"
    try:
        symbols = uf.get_b_names_from_table(pg_conn, "ak_dg_stock_board_concept_name_em_store")
        if LOGGER_DEBUG:
            logger.debug(f"symbols: {symbols}")
        all_data = []
        for symbol in symbols:
            func_call = f"ak.stock_concept_fund_flow_hist(symbol='{symbol}')"
            data = uf.try_to_call(func_call)
            if not data.empty:
                all_data.append(data)
        
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            combined_data = uf.convert_columns(combined_data, f'ak_dg_{func_name}', pg_conn, redis_hook.get_conn())

            if 'td' in combined_data.columns:
                combined_data['td'] = pd.to_datetime(combined_data['td'], errors='coerce').dt.strftime('%Y-%m-%d')

            temp_csv_path = uf.save_data_to_csv(combined_data, func_name)
            if temp_csv_path is None:
                raise AirflowException(f"No CSV file created for {func_name}, skipping database insertion.")
            
            uf.insert_data_from_csv(pg_conn, temp_csv_path, f'ak_dg_{func_name}')
            
            tracing_data = [(func_name, 'symbol', symbol, combined_data['td'].max().date(), datetime.now(), datetime.now(), os.getenv('HOSTNAME', socket.gethostname())) for symbol in symbols]
            store_tracing_data('ak_dg_tracing_by_date_1_param', tracing_data)
    except Exception as e:
        logger.error(f"Failed to fetch data for {func_name}: {str(e)}")
        raise AirflowException(e)

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
    description = ak_func_name_mapping.get(ak_func_name, ak_func_name)[0]
    source = ak_func_name_mapping.get(ak_func_name, ak_func_name)[1]
    return f'资金流向-{description}-{source}'

def generate_dag(ak_func_name: str):
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
        schedule=uf.generate_random_minute_schedule(hour=9), # 北京时间: 9+8=17
        catchup=False,
        tags=['akshare', 'store_daily', '资金流向'],
        max_active_runs=1,
    )

    tasks = {
        'check_trading_day': BranchPythonOperator(
            task_id='check_trading_day',
            python_callable=is_trading_day,
            provide_context=True,
            dag=dag,
        ),
        'continue_task': DummyOperator(task_id='continue_task', dag=dag),
        'skip_task': DummyOperator(task_id='skip_task', dag=dag),
        'get_tracing_data': PythonOperator(
            task_id=f'get_tracing_data_{ak_func_name}',
            python_callable=get_tracing_data,
            op_kwargs={'ak_func_name': ak_func_name},
            dag=dag,
        ),
        'get_data': PythonOperator(
            task_id=f'get_data_{ak_func_name}',
            python_callable=get_ff_data,
            op_kwargs={'ak_func_name': ak_func_name},
            dag=dag,
        ),
        'store_data': PythonOperator(
            task_id=f'store_data_{ak_func_name}',
            python_callable=store_ff_data,
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
    tasks['check_trading_day'] >> tasks['continue_task'] >> tasks['get_tracing_data'] >> tasks['get_data'] >> tasks['store_data'] >> tasks['tracing_data']
    tasks['check_trading_day'] >> tasks['skip_task']
    return dag

ak_func_name_list = [
    'stock_individual_fund_flow', 'stock_individual_fund_flow_rank', 'stock_market_fund_flow',
    'stock_sector_fund_flow_rank', 'stock_main_fund_flow', 'stock_sector_fund_flow_summary',
    'stock_sector_fund_flow_hist', 'stock_concept_fund_flow_hist'
]

for func_name in ak_func_name_list:
    dag_name = f'ak_dg_ff_{func_name}'
    globals()[dag_name] = generate_dag(func_name)
    logger.info(f"DAG for {dag_name} successfully created and registered.")
