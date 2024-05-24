from __future__ import annotations

import os
import sys
import socket
import pandas as pd
import akshare as ak
import psycopg2
from pathlib import Path
from datetime import timedelta, datetime, date
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException

from dg_ak.utils.util_funcs import UtilFuncs as uf
from dg_ak.utils.logger import logger
import dg_ak.utils.config as con

# 配置 today 变量
if con.LOGGER_DEBUG:
    today = '2024-05-24'
else:
    today = datetime.now().strftime('%Y-%m-%d')

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

TRACING_TABLE_NAME_BY_DATE = 'ak_dg_tracing_by_date'
TRACING_TABLE_NAME_BY_DATE_PARAM = 'ak_dg_tracing_by_date_1_param'
TRACING_TABLE_NAME_BY_SCODE_DATE = 'ak_dg_tracing_by_scode_date'


def store_tracing_data(tracing_table, data):
    if tracing_table == 'ak_dg_tracing_by_date':
        insert_sql = f"""
            INSERT INTO {tracing_table} (ak_func_name, last_td, create_time, update_time, host_name)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ak_func_name) DO UPDATE 
            SET last_td = EXCLUDED.last_td, update_time = EXCLUDED.update_time;
        """
    else:
        insert_sql = f"""
            INSERT INTO {tracing_table} (ak_func_name, param_name, param_value, last_td, create_time, update_time, host_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ak_func_name, param_name, param_value) DO UPDATE 
            SET last_td = EXCLUDED.last_td, update_time = EXCLUDED.update_time;
        """
    
    cursor = pg_conn.cursor()
    cursor.executemany(insert_sql, data)
    pg_conn.commit()
    cursor.close()


def is_trading_day(**kwargs) -> str:
    today_date = datetime.strptime(today, '%Y-%m-%d').date()
    trade_dates = uf.get_trade_dates(pg_conn)
    trade_dates.sort(reverse=True)
    if con.LOGGER_DEBUG:
        logger.debug(f'today: {today_date}')
        logger.debug(f'first 5 trade_dates: {trade_dates[:5]}')
    if today_date in trade_dates:
        return 'continue_task'
    else:
        return 'skip_task'


def process_data_columns(data, func_name, s_code=None, b_name=None):
    """
    Remove unnecessary columns, rename columns, and add stock or board name if provided.
    """
    if con.LOGGER_DEBUG:
        logger.debug(f"Removing unnecessary columns for {func_name}")

    cols_config = ak_cols_config_dict[func_name]
    remove_list = cols_config.get("remove_list", [])
    for col in remove_list:
        if col in data.columns:
            if con.LOGGER_DEBUG:
                logger.debug(f"Removing column: {col}")
            data.drop(columns=[col], inplace=True)
        else:
            if con.LOGGER_DEBUG:
                logger.debug(f"Column {col} not found in data, skipping removal.")

    data.rename(columns=uf.get_col_dict(cols_config), inplace=True)
    if s_code:
        data['s_code'] = s_code
    if b_name:
        data['b_name'] = b_name
    return data


def insert_data_with_conflict_handling(df, table_name, conflict_columns, update_columns):
    columns = df.columns.tolist()
    values = [tuple(x) for x in df.to_numpy()]
    
    # SQL template for inserting data with conflict handling
    insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES %s
        ON CONFLICT ({', '.join(conflict_columns)}) DO UPDATE SET
        {', '.join([f'{col} = EXCLUDED.{col}' for col in update_columns])};
    """
    
    # Ensure no duplicate conflict keys within the same batch
    unique_values_dict = {}
    for row in values:
        conflict_key = tuple(row[columns.index(col)] for col in conflict_columns)
        unique_values_dict[conflict_key] = row
    unique_values = list(unique_values_dict.values())
    
    cursor = pg_conn.cursor()
    psycopg2.extras.execute_values(
        cursor, insert_sql, unique_values, template=None, page_size=100
    )
    pg_conn.commit()
    cursor.close()


def fetch_and_process_data(func_name, ak_func, table_name, params=None, param_key=None, b_name=None):
    logger.info(f"Processing data for {func_name}")
    try:
        data = uf.try_to_call(ak_func, params)
        if data is None or data.empty:
            logger.warning(f"No data found for {func_name}")
            return

        if con.LOGGER_DEBUG:
            logger.debug(f"Data fetched for {func_name}: {data.head()}")

        data = process_data_columns(data, func_name, b_name=b_name)

        # Ensure 'td' column contains correct date data
        if 'td' not in data.columns:
            data['td'] = today
        else:
            data['td'] = data['td'].apply(uf.format_td10)

        # Remove rows with null values and ensure date validity
        data.dropna(inplace=True)

        # Convert column order and data types
        data = uf.convert_columns(data, table_name, pg_conn, redis_hook.get_conn())

        # Determine conflict columns and update columns
        # if table_name in ['ak_dg_stock_market_fund_flow', 'ak_dg_stock_sector_fund_flow_rank', 'ak_dg_stock_sector_fund_flow_hist', 'ak_dg_stock_concept_fund_flow_hist']:
        if table_name in ['ak_dg_stock_sector_fund_flow_rank_store', 'stock_sector_fund_flow_summary_store', 'ak_dg_stock_sector_fund_flow_hist_store', 'ak_dg_stock_concept_fund_flow_hist_store']:
            conflict_columns = ['td', 'b_name']
        elif table_name in ['ak_dg_stock_market_fund_flow_store']:
            conflict_columns = ['td']
        else:
            conflict_columns = ['td', 's_code']

        update_columns = [col for col in data.columns if col not in conflict_columns]

        # Insert data and update on conflict
        insert_data_with_conflict_handling(data, table_name, conflict_columns, update_columns)

        if param_key:
            tracing_data = [(func_name, param_key, param_value, data['td'].max(), datetime.now(), datetime.now(), os.getenv('HOSTNAME', socket.gethostname())) for param_value in data[param_key].unique()]
            store_tracing_data(TRACING_TABLE_NAME_BY_DATE_PARAM, tracing_data)
        else:
            store_tracing_data(TRACING_TABLE_NAME_BY_DATE, [(func_name, data['td'].max(), datetime.now(), datetime.now(), os.getenv('HOSTNAME', socket.gethostname()))])
    except Exception as e:
        logger.error(f"Failed to process data for {func_name}: {str(e)}")
        raise AirflowException(e)


def get_b_names_from_table(pg_conn, table_name: str, td=today) -> list:
    query = f"SELECT DISTINCT b_name FROM {table_name} WHERE td = %s"
    cursor = pg_conn.cursor()
    cursor.execute(query, (td,))
    results = cursor.fetchall()
    cursor.close()
    return [row[0] for row in results]

def get_data_by_s_code(func_name, cols_config):
    logger.info("Fetching stock individual fund flow data")
    s_code_list = uf.get_s_code_list(redis_hook.get_conn())
    all_data = []
    _len_s_code_list = len(s_code_list)
    for _index, _s_code in enumerate(s_code_list, start=1):
        try:
            # if con.LOGGER_DEBUG and _index > 5:
            #     break
            logger.info(f'({_index}/{_len_s_code_list}) downloading data with s_code={_s_code}')
            logger.info(f"Fetching data for s_code: {_s_code} using function {func_name}")
            ak_func = getattr(ak, func_name)
            if ak_func is None:
                raise AirflowException(f"Function {func_name} not found in akshare.")

            market = 'sh' if _s_code.startswith('6') else \
                     'sz' if _s_code.startswith(('0', '3')) else \
                     'bj' if _s_code.startswith(('4', '8')) else None

            data = uf.try_to_call(ak_func, {'stock': _s_code, 'market': market})
            if data is None or data.empty:
                logger.warning(f"No data found for s_code {_s_code} using function {func_name}. It may indicate that the stock is newly listed.")
                continue

            data = uf.remove_cols(data, cols_config)
            data.rename(columns=uf.get_col_dict(cols_config), inplace=True)
            data['s_code'] = _s_code

            # Ensure data has expected columns
            # expected_columns = set(uf.get_col_dict(cols_config).values())
            # if con.LOGGER_DEBUG:
            #     logger.debug(f'expected_columns:{expected_columns}')
            #     logger.debug(f'data.columns:{data.columns}')
            # missing_columns = expected_columns - set(data.columns)
            # if missing_columns:
            #     logger.error(f"Missing columns for s_code {_s_code} using function {func_name}: {missing_columns}")
            #     continue

            # data = process_data_columns(data, func_name, s_code=_s_code)

            # 删除包含空值或缺失值的行，并确保日期有效
            data.dropna(inplace=True)
            if 'td' in data.columns:
                data['td'] = data['td'].apply(uf.format_td10)

            # 转换列顺序和数据类型
            data = uf.convert_columns(data, f'ak_dg_{func_name}_store', pg_conn, redis_hook.get_conn())

            all_data.append(data)

            if con.LOGGER_DEBUG:
                logger.debug(f"Fetched data for s_code {_s_code}: {data.head()}")
        except Exception as e:
            logger.error(f"Failed to fetch data for s_code {_s_code} using function {func_name}: {str(e)}")
            continue  # Skip to the next stock code

    if not all_data:
        raise AirflowException("No valid data was fetched for any stock codes.")

    combined_data = pd.concat(all_data, ignore_index=True)
    return combined_data

def get_stock_individual_fund_flow():
    func_name = "stock_individual_fund_flow"
    ak_func = lambda: get_data_by_s_code(func_name, ak_cols_config_dict[func_name])
    fetch_and_process_data(func_name, ak_func, f'ak_dg_{func_name}_store')

def get_stock_individual_fund_flow_rank():
    func_name = "stock_individual_fund_flow_rank"
    ak_func = lambda: ak.stock_individual_fund_flow_rank(indicator="今日")
    fetch_and_process_data(func_name, ak_func, f'ak_dg_{func_name}_store')

def get_stock_market_fund_flow():
    func_name = "stock_market_fund_flow"
    ak_func = lambda: uf.get_data_today(func_name, ak_cols_config_dict)
    fetch_and_process_data(func_name, ak_func, f'ak_dg_{func_name}_store')

def get_stock_sector_fund_flow_rank():
    func_name = "stock_sector_fund_flow_rank"
    sectors = ["行业资金流", "概念资金流", "地域资金流"]
    all_data = []

    
    len_sectors = len(sectors)
    for _index, _sector in enumerate(sectors, start=1):
        logger.info(f'({_index}/{len_sectors}) downloading data with sector={_sector}')
        ak_func = lambda: ak.stock_sector_fund_flow_rank(indicator='今日', sector_type=_sector)
        data = uf.try_to_call(ak_func)
        if data is not None:
            data = process_data_columns(data, func_name)
            data['sector_type'] = _sector
            all_data.append(data)
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        if con.LOGGER_DEBUG:
            logger.debug(f'combined_data in {func_name}:')
            logger.debug(combined_data.head(5))
        fetch_and_process_data(func_name, lambda: combined_data, f'ak_dg_{func_name}_store')

def get_stock_main_fund_flow():
    func_name = "stock_main_fund_flow"
    ak_func = lambda: ak.stock_main_fund_flow(symbol='全部股票')
    fetch_and_process_data(func_name, ak_func, f'ak_dg_{func_name}_store')

def get_stock_sector_fund_flow_summary():
    func_name = "stock_sector_fund_flow_summary"
    b_names = get_b_names_from_table(pg_conn, "ak_dg_stock_board_industry_name_em_store")
    all_data = []

    len_b_name = len(b_names)
    for _index, _b_name in enumerate(b_names, start=1):
        logger.info(f'({_index}/{len_b_name}) downloading data with b_name={_b_name}')
        ak_func = lambda: ak.stock_sector_fund_flow_summary(symbol=_b_name, indicator='今日')
        data = uf.try_to_call(ak_func)
        if data is not None:
            data = process_data_columns(data, func_name, b_name=_b_name)
            all_data.append(data)
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        fetch_and_process_data(func_name, lambda: combined_data, f'ak_dg_{func_name}_store')

def get_stock_sector_fund_flow_hist():
    func_name = "stock_sector_fund_flow_hist"
    b_names = get_b_names_from_table(pg_conn, "ak_dg_stock_board_industry_name_em_store")
    all_data = []
    len_b_name = len(b_names)
    for _index, _b_name in enumerate(b_names, start=1):
        logger.info(f'({_index}/{len_b_name}) downloading data with b_name={_b_name}')
        ak_func = lambda: ak.stock_sector_fund_flow_hist(symbol=_b_name)
        data = uf.try_to_call(ak_func)
        if data is not None:
            data = process_data_columns(data, func_name, b_name=_b_name)
            all_data.append(data)
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        fetch_and_process_data(func_name, lambda: combined_data, f'ak_dg_{func_name}_store')

def get_stock_concept_fund_flow_hist():
    func_name = "stock_concept_fund_flow_hist"
    b_names = get_b_names_from_table(pg_conn, "ak_dg_stock_board_concept_name_em_store")
    all_data = []
    len_b_name = len(b_names)
    for _index, _b_name in enumerate(b_names, start=1):
        logger.info(f'({_index}/{len_b_name}) downloading data with b_name={_b_name}')
        ak_func = lambda: ak.stock_concept_fund_flow_hist(symbol=_b_name)
        data = uf.try_to_call(ak_func)
        if data is not None:
            data = process_data_columns(data, func_name, b_name=_b_name)
            all_data.append(data)
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        fetch_and_process_data(func_name, lambda: combined_data, f'ak_dg_{func_name}_store')

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
        dag_name = f'ak_dg_ff_{func_name}'
        globals()[dag_name] = generate_dag(func_name, task_func)
        logger.info(f"DAG for {dag_name} successfully created and registered.")
    except Exception as e:
        logger.error(f"Failed to create DAG for {func_name}: {str(e)}")
